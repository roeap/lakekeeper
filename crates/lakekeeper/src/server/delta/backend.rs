//! [`LakekeeperDeltaBackend`] — Lakekeeper's implementation of the
//! [`DeltaBackend`] port for the read path.
//!
//! The adapter is a `Clone` wrapper over the [`ApiContext`] (built once at mount
//! time from the cloned host state); each `&self` port method clones the context
//! and delegates to Lakekeeper's existing static server logic (generic-table
//! storage, `StorageProfile` credential vending, and generic-table authz). All
//! Delta *semantics* stay in the crate.
//!
//! # Scope
//!
//! Read path only: `getConfig`, `loadTable`, `tableExists`, and credential vending.
//! The write/commit and managed/staging methods return
//! [`DeltaBackendError::NotImplemented`] and are tracked as follow-ups.
//!
//! # Redundant catalog loads
//!
//! The crate calls `authorize` and each data method (`resolve_table`,
//! `vend_table_credential`) as independent, stateless hooks — `authorize` returns
//! only `()`, and nothing but the table coordinate / id passes between the calls.
//! So a `loadTable` loads the table once to authorize and again to resolve it, and
//! a credential vend loads it in `resolve_table`, in `authorize`, and in the vend.
//! Collapsing those would need a per-request resolved-table cache threaded across
//! the hooks (or an upstream seam to pass resolved state), which is a follow-up:
//! per-process caches here carry the cross-replica-invalidation hazards this repo
//! avoids on authoritative reads.
//!
//! # Coordinate mapping
//!
//! The warehouse is carried on [`DeltaRequestContext::warehouse_id`] (parsed from
//! the URL `{prefix}`). The Delta `catalog` + `schema` map to a two-level
//! [`NamespaceIdent`] within that warehouse; `table` is the table name. A Delta
//! table's schema is stored in the generic table's free-form `schema` JSON blob as
//! the Delta wire `StructType`; [`resolve_table`](LakekeeperDeltaBackend::resolve_table)
//! converts it to the crate's UC [`Column`]s via [`contract::delta_columns_to_uc`].

use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use iceberg::NamespaceIdent;
use iceberg_ext::{
    catalog::rest::ErrorModel,
    configs::table::{gcs, s3},
};
use unitycatalog_delta_api::{
    DeltaBackend, DeltaCapabilities,
    authz::DeltaAction,
    backend::{
        CreateTableSpec, CredentialAccess, ResolvedTable, SchemaRef, StagingReservation,
        UpdateTableSpec, VendedCredential, VendedCredentialKind,
    },
    column::Column,
    contract,
    coordinator::{CommitCoordinator, InMemoryCommitCoordinator},
    error::DeltaBackendError,
    models::{DeltaDataSourceFormat, DeltaStructType, DeltaTableType},
};

use super::context::DeltaRequestContext;
use crate::{
    api::{
        ApiContext,
        iceberg::v1::{DataAccess, DataAccessMode},
    },
    request_metadata::RequestMetadata,
    server::{generic_tables, maybe_get_secret},
    service::{
        CatalogGenericTableOps, CatalogStore, CatalogWarehouseOps, GenericTableId,
        GenericTableInfo, Location, ResolvedWarehouse, SecretStore, State, Transaction,
        WarehouseId,
        authz::{Authorizer, AuthzWarehouseOps, CatalogGenericTableAction, CatalogWarehouseAction},
        events::AuthorizationFailureSource,
        storage::{StoragePermissions, TableConfig},
    },
};

type BackendResult<T> = Result<T, DeltaBackendError>;

/// Lakekeeper's [`DeltaBackend`] adapter.
///
/// Holds the mount-time [`ApiContext`] (which is `Clone`) plus a
/// [`CommitCoordinator`] handle. On the read path the coordinator is never
/// exercised — it is an [`InMemoryCommitCoordinator`] placeholder until the
/// write-path milestone supplies a persistent one.
pub(crate) struct LakekeeperDeltaBackend<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> {
    ctx: ApiContext<State<A, C, S>>,
    coordinator: Arc<InMemoryCommitCoordinator>,
}

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> Clone
    for LakekeeperDeltaBackend<C, A, S>
{
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            coordinator: self.coordinator.clone(),
        }
    }
}

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> LakekeeperDeltaBackend<C, A, S> {
    pub(crate) fn new(ctx: ApiContext<State<A, C, S>>) -> Self {
        Self {
            ctx,
            coordinator: Arc::new(InMemoryCommitCoordinator::default()),
        }
    }

    /// Load the active warehouse for `warehouse_id`, mapping absence to a 404.
    async fn require_warehouse(
        &self,
        warehouse_id: WarehouseId,
    ) -> BackendResult<Arc<ResolvedWarehouse>> {
        let warehouse =
            C::get_active_warehouse_by_id(warehouse_id, self.ctx.v1_state.catalog.clone())
                .await
                .map_err(to_backend_err)?;
        warehouse.ok_or_else(|| {
            DeltaBackendError::NotFoundGeneric(format!("warehouse '{warehouse_id}' not found"))
        })
    }

    /// Load a generic table by name within a warehouse (read transaction), mapping
    /// a missing table to a 404.
    async fn load_generic_table(
        &self,
        warehouse_id: WarehouseId,
        namespace: &NamespaceIdent,
        table_name: &str,
    ) -> BackendResult<GenericTableInfo> {
        let namespace_id = self.resolve_namespace_id(warehouse_id, namespace).await?;
        let mut t = C::Transaction::begin_read(self.ctx.v1_state.catalog.clone())
            .await
            .map_err(to_backend_err)?;
        let info = C::load_generic_table(warehouse_id, namespace_id, table_name, t.transaction())
            .await
            .map_err(to_backend_err)?;
        t.commit().await.map_err(to_backend_err)?;
        Ok(info)
    }

    /// Load a generic table by its stable id within a warehouse (read transaction).
    async fn load_generic_table_by_id(
        &self,
        warehouse_id: WarehouseId,
        table_id: &str,
    ) -> BackendResult<GenericTableInfo> {
        let id = GenericTableId::from_str_or_bad_request(table_id)
            .map_err(|e| DeltaBackendError::InvalidArgument(format!("invalid table id: {e}")))?;
        let mut t = C::Transaction::begin_read(self.ctx.v1_state.catalog.clone())
            .await
            .map_err(to_backend_err)?;
        let info = C::load_generic_table_by_id(warehouse_id, id, t.transaction())
            .await
            .map_err(to_backend_err)?;
        t.commit().await.map_err(to_backend_err)?;
        Ok(info)
    }

    /// Resolve a namespace name to its id within a warehouse.
    async fn resolve_namespace_id(
        &self,
        warehouse_id: WarehouseId,
        namespace: &NamespaceIdent,
    ) -> BackendResult<crate::service::NamespaceId> {
        use crate::service::CatalogNamespaceOps;
        let hierarchy = C::get_namespace(
            warehouse_id,
            namespace.clone(),
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .map_err(to_backend_err)?
        .ok_or_else(|| DeltaBackendError::NotFound(format!("namespace {namespace:?} not found")))?;
        Ok(hierarchy.namespace_id())
    }

    /// Authorize a data action against a generic table identified by id.
    ///
    /// Loads the table by id, then authorizes by ident (the resolved ident matches
    /// the id). Reuses the name-based `require_generic_table_action` helper after
    /// resolving id → ident.
    async fn authorize_table_id(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        table_id: &str,
        action: CatalogGenericTableAction,
    ) -> BackendResult<()> {
        let info = self
            .load_generic_table_by_id(warehouse_id, table_id)
            .await?;
        let namespace = info.tabular_ident.namespace.clone();
        let table_name = info.name.clone();
        generic_tables::load_and_authorize_generic_table_operation::<C, A>(
            &self.ctx.v1_state.authz,
            metadata,
            warehouse_id,
            namespace,
            &table_name,
            action,
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .map(|_| ())
        .map_err(|e| to_backend_err(e.into_error_model()))
    }

    /// Vend storage credentials for a location under the warehouse's storage
    /// profile. `tabular_info` scopes the credential request (its location +
    /// tabular id); `generate_table_config` reads it via [`BasicTabularInfo`].
    async fn generate_table_config(
        &self,
        cx: &DeltaRequestContext,
        location: &Location,
        permissions: StoragePermissions,
        tabular_info: &impl crate::service::BasicTabularInfo,
    ) -> BackendResult<TableConfig> {
        let warehouse = self.require_warehouse(cx.warehouse_id).await?;
        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &self.ctx.v1_state.secrets)
                .await
                .map_err(to_backend_err)?;
        warehouse
            .storage_profile
            .generate_table_config(
                vended_data_access(),
                storage_secret.as_deref(),
                location,
                permissions,
                &cx.metadata,
                tabular_info,
            )
            .await
            // `TableConfigError` reaches `ErrorModel` via `IcebergErrorResponse`.
            .map_err(|e| to_backend_err(iceberg_ext::catalog::rest::IcebergErrorResponse::from(e)))
    }
}

// ===================================================================
// Error mapping
// ===================================================================

/// Map a Lakekeeper error response onto the crate's [`DeltaBackendError`].
///
/// Dispatches on the machine-readable error type/code so e.g. an "already exists"
/// does not surface as a generic 404. Anything unrecognized becomes
/// [`DeltaBackendError::Internal`].
fn to_backend_err(e: impl Into<ErrorModel>) -> DeltaBackendError {
    let e = e.into();
    match e.code {
        404 => DeltaBackendError::NotFound(e.message),
        401 => DeltaBackendError::Unauthenticated(e.message),
        403 => DeltaBackendError::PermissionDenied(e.message),
        400 => DeltaBackendError::InvalidArgument(e.message),
        409 => DeltaBackendError::AlreadyExists(e.message),
        429 => DeltaBackendError::ResourceExhausted(e.message),
        _ => DeltaBackendError::Internal(e.message),
    }
}

// ===================================================================
// Type mapping
// ===================================================================

/// The two-level namespace `[catalog, schema]` a Delta coordinate maps to.
///
/// `NamespaceIdent::from_vec` rejects only an empty *vector*, not empty-string
/// parts, so empty `catalog`/`schema` segments are rejected here as a 400 rather
/// than forming a malformed namespace whose lookup fails later as a confusing 404.
fn namespace(catalog: &str, schema: &str) -> BackendResult<NamespaceIdent> {
    if catalog.is_empty() || schema.is_empty() {
        return Err(DeltaBackendError::InvalidArgument(
            "Delta catalog and schema must be non-empty".to_string(),
        ));
    }
    NamespaceIdent::from_vec(vec![catalog.to_string(), schema.to_string()]).map_err(|e| {
        DeltaBackendError::InvalidArgument(format!("invalid catalog/schema namespace: {e}"))
    })
}

/// Map a stored generic table into the crate's portable [`ResolvedTable`].
///
/// The Delta table type / format are inferred from the generic-table `format`
/// string (`"delta"` → managed Delta). Columns come from the stored `schema` blob,
/// which by this integration's convention holds the Delta wire `StructType` JSON.
/// A stored-but-unparseable schema is an error (see [`columns_from_schema`]); an
/// absent schema yields empty columns so a table without one still loads.
fn table_to_resolved(info: &GenericTableInfo) -> BackendResult<ResolvedTable> {
    let is_delta = info.format.as_str() == "delta";
    let columns = columns_from_schema(info.schema.as_ref())?;

    Ok(ResolvedTable {
        table_id: Some(info.generic_table_id.to_string()),
        location: info.location.to_string(),
        // Generic tables carry no managed/external distinction; a Delta generic
        // table is served as EXTERNAL for the read path (managed/staging is a
        // follow-up). Non-`delta` formats map to `None`, which the handler rejects
        // with the spec's "not a Delta table" 400.
        table_type: is_delta.then_some(DeltaTableType::External),
        data_source_format: is_delta.then_some(DeltaDataSourceFormat::Delta),
        columns,
        properties: info.properties.clone().into_iter().collect(),
        created_at_ms: None,
        updated_at_ms: None,
        // Generic tables have an optimistic `version` counter, but it is not yet
        // surfaced through `GenericTableInfo`; the read path has no CAS, so 0 (the
        // crate's "untracked" sentinel) is correct here. Surfacing the real version
        // is part of the write-path milestone.
        version: 0,
    })
}

/// Convert a stored generic-table `schema` blob into the crate's UC [`Column`]s.
///
/// The blob holds the Delta wire `StructType` JSON (this integration's convention;
/// the field is otherwise free-form and unvalidated). An absent blob yields no
/// columns, so a table without a stored Delta schema still loads. A blob that is
/// present but not a valid Delta `StructType` is a corrupt/foreign schema and is
/// surfaced as an error rather than served as a column-less table, which would
/// leave the Delta client reading nothing.
fn columns_from_schema(schema: Option<&serde_json::Value>) -> BackendResult<Vec<Column>> {
    let Some(schema) = schema else {
        return Ok(Vec::new());
    };
    let struct_type = serde_json::from_value::<DeltaStructType>(schema.clone()).map_err(|e| {
        DeltaBackendError::Internal(format!(
            "stored table schema is not a valid Delta schema: {e}"
        ))
    })?;
    contract::delta_columns_to_uc(&struct_type, None).map_err(|e| {
        DeltaBackendError::Internal(format!("stored Delta schema could not be converted: {e}"))
    })
}

/// Map the crate's [`CredentialAccess`] onto Lakekeeper's [`StoragePermissions`].
fn to_storage_permissions(access: CredentialAccess) -> StoragePermissions {
    match access {
        CredentialAccess::Read => StoragePermissions::Read,
        CredentialAccess::ReadWrite => StoragePermissions::ReadWriteDelete,
    }
}

/// The [`DataAccessMode`] for `generate_table_config`: server-delegated *vended*
/// credentials (the Delta credential endpoints hand the client short-lived tokens,
/// never remote signing).
fn vended_data_access() -> DataAccessMode {
    DataAccessMode::ServerDelegated(DataAccess {
        vended_credentials: true,
        remote_signing: false,
    })
}

/// Build a [`VendedCredential`] from a vended [`TableConfig`] for a location.
///
/// The vended credential kind is determined by which credential keys the storage
/// profile populated in `creds` (S3 access keys, a GCS OAuth token, or a dynamic
/// `adls.sas-token.<account>` entry). A set with none of these recognized keys
/// means the vend did not yield a credential this path can hand to a client (e.g.
/// a remote-signing-only profile, which this read path disables) and is surfaced
/// as an error rather than a `200` carrying an unusable [`VendedCredentialKind::None`].
///
/// `expiration_time_ms` comes straight from the storage profile. A profile that
/// leaves it unset (`credentials_expiration_ms == None`) is stating the credential
/// does not expire; that only holds for genuinely long-lived static creds, so a
/// profile that vends short-lived creds must populate the field.
fn to_vended_credential(
    url: String,
    table_config: &TableConfig,
) -> BackendResult<VendedCredential> {
    let creds = &table_config.creds;
    let expiration_time_ms = table_config.credentials_expiration_ms.unwrap_or(i64::MAX);

    let kind = if let (Some(access_key_id), Some(secret_access_key)) = (
        creds.get_prop_opt::<s3::AccessKeyId>(),
        creds.get_prop_opt::<s3::SecretAccessKey>(),
    ) {
        VendedCredentialKind::S3 {
            access_key_id,
            secret_access_key,
            session_token: creds.get_prop_opt::<s3::SessionToken>(),
        }
    } else if let Some(oauth_token) = creds.get_prop_opt::<gcs::Token>() {
        VendedCredentialKind::GcsOauth { oauth_token }
    } else if let Some(sas_token) = adls_sas_token(creds) {
        VendedCredentialKind::AzureSas { sas_token }
    } else {
        return Err(DeltaBackendError::Internal(
            "storage profile vended no credential this Delta endpoint can serve (no S3, GCS, or ADLS keys present)".to_string(),
        ));
    };

    Ok(VendedCredential {
        url,
        expiration_time_ms,
        kind,
    })
}

/// Extract an ADLS/OneLake SAS token from vended creds. The key is
/// account-scoped (`adls.sas-token.<account>.<suffix>`), so it is matched by
/// prefix — excluding the `adls.sas-token-expires-at-ms.*` companion.
fn adls_sas_token(creds: &iceberg_ext::configs::table::TableProperties) -> Option<String> {
    creds.inner().iter().find_map(|(k, v)| {
        (k.starts_with("adls.sas-token.") && !k.starts_with("adls.sas-token-expires-at-ms."))
            .then(|| v.clone())
    })
}

#[async_trait]
impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> DeltaBackend<DeltaRequestContext>
    for LakekeeperDeltaBackend<C, A, S>
{
    fn capabilities(&self) -> DeltaCapabilities {
        // Rename is part of the write path; not served in the read-path milestone,
        // so `getConfig` must not advertise the rename endpoint.
        DeltaCapabilities { rename: false }
    }

    async fn authorize(
        &self,
        action: DeltaAction<'_>,
        cx: &DeltaRequestContext,
    ) -> BackendResult<()> {
        // All authorization is centralized here (the crate contract: data methods
        // do not re-authorize). The read path authorizes reads and table-credential
        // vends; write/staging/path actions are not served in this milestone.
        match action {
            DeltaAction::ReadTable { table } => {
                let namespace = namespace(&table.catalog, &table.schema)?;
                generic_tables::load_and_authorize_generic_table_operation::<C, A>(
                    &self.ctx.v1_state.authz,
                    &cx.metadata,
                    cx.warehouse_id,
                    namespace,
                    &table.table,
                    CatalogGenericTableAction::GetMetadata,
                    self.ctx.v1_state.catalog.clone(),
                )
                .await
                .map(|_| ())
                .map_err(|e| to_backend_err(e.into_error_model()))
            }
            DeltaAction::VendTableCredential { table_id, access } => {
                let action = match access {
                    CredentialAccess::Read => CatalogGenericTableAction::ReadData,
                    CredentialAccess::ReadWrite => CatalogGenericTableAction::WriteData,
                };
                self.authorize_table_id(&cx.metadata, cx.warehouse_id, table_id, action)
                    .await
            }
            // `getTemporaryPathCredentials` is deferred (see `vend_path_credential`).
            DeltaAction::VendPathCredential { .. } => Err(DeltaBackendError::NotImplemented(
                "Delta temporary-path credentials are not yet supported by Lakekeeper",
            )),
            // Write / staging actions are not served in the read-path milestone.
            DeltaAction::CreateTable { .. }
            | DeltaAction::WriteTable { .. }
            | DeltaAction::DeleteTable { .. }
            | DeltaAction::RenameTable { .. }
            | DeltaAction::CreateStaging { .. }
            | DeltaAction::AdoptStaging { .. } => Err(DeltaBackendError::NotImplemented(
                "Delta write and staging operations are not yet supported by Lakekeeper",
            )),
            // Fail closed on any future variant.
            _ => Err(DeltaBackendError::PermissionDenied(
                "unrecognized Delta action".to_string(),
            )),
        }
    }

    async fn catalog_exists(&self, _catalog: &str, cx: &DeltaRequestContext) -> BackendResult<()> {
        // The warehouse (from the URL prefix) is the authoritative scope; the Delta
        // `catalog` arg is advisory (the top-level namespace the client will use).
        //
        // The crate has no `getConfig` authz action (it authorizes via
        // `catalog_exists`), so the warehouse-level `GetConfig` check that the
        // iceberg `GET /config` enforces (`server::config`) lives here. Without it
        // any authenticated caller could probe warehouse existence via `getConfig`.
        // `require_warehouse_action` masks a warehouse the caller cannot see as a
        // 404 (`WarehouseIdNotFound`) and returns a native 403 only for a forbidden
        // action on a warehouse the caller *can* see, so existence never leaks.
        let warehouse =
            C::get_active_warehouse_by_id(cx.warehouse_id, self.ctx.v1_state.catalog.clone()).await;
        self.ctx
            .v1_state
            .authz
            .require_warehouse_action(
                &cx.metadata,
                cx.warehouse_id,
                warehouse,
                CatalogWarehouseAction::GetConfig,
            )
            .await
            .map(|_| ())
            .map_err(|e| to_backend_err(e.into_error_model()))
    }

    async fn resolve_table(
        &self,
        table: &unitycatalog_delta_api::backend::TableRef,
        cx: &DeltaRequestContext,
    ) -> BackendResult<ResolvedTable> {
        let namespace = namespace(&table.catalog, &table.schema)?;
        let info = self
            .load_generic_table(cx.warehouse_id, &namespace, &table.table)
            .await?;
        table_to_resolved(&info)
    }

    async fn validate_external_location(
        &self,
        location: &str,
        cx: &DeltaRequestContext,
    ) -> BackendResult<()> {
        let warehouse = self.require_warehouse(cx.warehouse_id).await?;
        let parsed = Location::from_str(location)
            .map_err(|e| DeltaBackendError::InvalidArgument(e.to_string()))?;
        warehouse
            .storage_profile
            .require_allowed_location(&parsed)
            .map_err(to_backend_err)
    }

    async fn create_table_row(
        &self,
        _spec: CreateTableSpec,
        _cx: &DeltaRequestContext,
    ) -> BackendResult<ResolvedTable> {
        Err(DeltaBackendError::NotImplemented(
            "Delta createTable is not yet supported by Lakekeeper",
        ))
    }

    async fn update_table_row(
        &self,
        _spec: UpdateTableSpec,
        _cx: &DeltaRequestContext,
    ) -> BackendResult<ResolvedTable> {
        Err(DeltaBackendError::NotImplemented(
            "Delta updateTable is not yet supported by Lakekeeper",
        ))
    }

    async fn delete_table(
        &self,
        _table: &unitycatalog_delta_api::backend::TableRef,
        _cx: &DeltaRequestContext,
    ) -> BackendResult<()> {
        Err(DeltaBackendError::NotImplemented(
            "Delta deleteTable is not yet supported by Lakekeeper",
        ))
    }

    async fn rename_table(
        &self,
        _from: &unitycatalog_delta_api::backend::TableRef,
        _to_name: &str,
        _cx: &DeltaRequestContext,
    ) -> BackendResult<()> {
        Err(DeltaBackendError::NotImplemented(
            "Delta renameTable is not yet supported by Lakekeeper",
        ))
    }

    async fn allocate_staging(
        &self,
        _at: &SchemaRef,
        _name: &str,
        _cx: &DeltaRequestContext,
    ) -> BackendResult<StagingReservation> {
        Err(DeltaBackendError::NotImplemented(
            "Delta staging tables are not yet supported by Lakekeeper",
        ))
    }

    async fn resolve_staging_by_location(
        &self,
        _location: &str,
        _cx: &DeltaRequestContext,
    ) -> BackendResult<StagingReservation> {
        Err(DeltaBackendError::NotImplemented(
            "Delta staging tables are not yet supported by Lakekeeper",
        ))
    }

    async fn resolve_staging_by_id(
        &self,
        _table_id: &str,
        _cx: &DeltaRequestContext,
    ) -> BackendResult<StagingReservation> {
        Err(DeltaBackendError::NotImplemented(
            "Delta staging tables are not yet supported by Lakekeeper",
        ))
    }

    async fn vend_table_credential(
        &self,
        table_id: &str,
        access: CredentialAccess,
        cx: &DeltaRequestContext,
    ) -> BackendResult<VendedCredential> {
        let info = self
            .load_generic_table_by_id(cx.warehouse_id, table_id)
            .await?;
        let location = info.location.clone();
        let table_config = self
            .generate_table_config(cx, &location, to_storage_permissions(access), &info)
            .await?;
        to_vended_credential(location.to_string(), &table_config)
    }

    async fn vend_path_credential(
        &self,
        _location: &str,
        _access: CredentialAccess,
        _cx: &DeltaRequestContext,
    ) -> BackendResult<VendedCredential> {
        // `generate_table_config` vends credentials scoped to a *tabular* (it
        // requires a `BasicTabularInfo`). An arbitrary path has no tabular, and
        // Lakekeeper exposes no path-scoped vending seam today, so
        // `getTemporaryPathCredentials` is deferred with the write/staging path.
        // The four table-scoped read endpoints remain fully functional.
        Err(DeltaBackendError::NotImplemented(
            "Delta temporary-path credentials are not yet supported by Lakekeeper",
        ))
    }

    fn commit_coordinator(&self) -> &dyn CommitCoordinator {
        self.coordinator.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use iceberg_ext::configs::table::TableProperties;
    use serde_json::json;

    use super::*;

    fn table_config(creds: TableProperties, expiration_ms: Option<i64>) -> TableConfig {
        TableConfig {
            creds,
            config: TableProperties::default(),
            credentials_expiration_ms: expiration_ms,
            remote_signing: None,
        }
    }

    #[test]
    fn namespace_rejects_empty_parts() {
        assert!(matches!(
            namespace("", "schema"),
            Err(DeltaBackendError::InvalidArgument(_))
        ));
        assert!(matches!(
            namespace("catalog", ""),
            Err(DeltaBackendError::InvalidArgument(_))
        ));
        let ns = namespace("catalog", "schema").expect("non-empty parts form a namespace");
        assert_eq!(ns.inner(), &["catalog".to_string(), "schema".to_string()]);
    }

    #[test]
    fn columns_from_absent_schema_is_empty() {
        assert!(
            columns_from_schema(None)
                .expect("absent schema is fine")
                .is_empty()
        );
    }

    #[test]
    fn columns_from_malformed_schema_errors() {
        // A present-but-not-a-Delta-StructType blob is corruption, surfaced as an
        // error rather than served as a column-less table.
        let blob = json!({"not": "a delta struct type"});
        assert!(matches!(
            columns_from_schema(Some(&blob)),
            Err(DeltaBackendError::Internal(_))
        ));
    }

    #[test]
    fn columns_from_valid_delta_schema_parses() {
        let blob = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": false, "metadata": {}}
            ]
        });
        let columns = columns_from_schema(Some(&blob)).expect("valid delta schema parses");
        assert_eq!(columns.len(), 1);
    }

    #[test]
    fn vend_s3_credential_uses_profile_expiration() {
        let mut creds = TableProperties::default();
        creds.insert(&s3::AccessKeyId("AKIA".to_string()));
        creds.insert(&s3::SecretAccessKey("secret".to_string()));
        let cfg = table_config(creds, Some(1_700_000_000_000));

        let vended = to_vended_credential("s3://bucket/t".to_string(), &cfg)
            .expect("recognized S3 creds vend");
        assert_eq!(vended.expiration_time_ms, 1_700_000_000_000);
        assert!(matches!(vended.kind, VendedCredentialKind::S3 { .. }));
    }

    #[test]
    fn vend_credential_without_recognized_keys_errors() {
        // No S3/GCS/ADLS keys present: a vend this endpoint cannot serve, surfaced
        // as an error rather than a 200 carrying an unusable `None` kind.
        let cfg = table_config(TableProperties::default(), Some(1));
        assert!(matches!(
            to_vended_credential("s3://bucket/t".to_string(), &cfg),
            Err(DeltaBackendError::Internal(_))
        ));
    }
}
