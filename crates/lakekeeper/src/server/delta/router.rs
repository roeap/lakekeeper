//! Lakekeeper-declared axum routes for the UC Delta v1 API.
//!
//! The `unitycatalog-delta-api` crate owns all Delta *semantics* via the
//! [`DeltaApiHandler`] port (a blanket impl over [`LakekeeperDeltaBackend`]), but
//! its own `get_router` makes the handler the axum `State` and returns a
//! fully-stated `Router`, which cannot be composed into Lakekeeper's
//! `ApiContext`-stated router *inside* the request-metadata/auth layers. So
//! Lakekeeper declares the routes here over its own [`ApiContext`] state: each
//! handler builds the adapter + [`DeltaRequestContext`] and calls the crate's
//! `DeltaApiHandler` method. The crate still owns every Delta behavior; this module
//! is only glue. (Tracked upstream for a composable router: mangrove#135.)
//!
//! Mounted at `/catalog/v1/{prefix}/delta/v1` (the warehouse `{prefix}` precedes
//! the spec-fixed `/delta/v1` base).

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::Deserialize;
use unitycatalog_delta_api::{
    DeltaApiHandler,
    backend::{SchemaRef, TableRef},
    error::{DeltaApiError, DeltaApiResult},
    handler::GetConfigQuery,
    models::{
        DeltaCatalogConfig, DeltaCreateStagingTableRequest, DeltaCreateTableRequest,
        DeltaCredentialOperation, DeltaCredentialsResponse, DeltaLoadTableResponse,
        DeltaRenameTableRequest, DeltaReportMetricsRequest, DeltaStagingTableResponse,
        DeltaUpdateTableRequest,
    },
};

use super::{backend::LakekeeperDeltaBackend, context::DeltaRequestContext};
use crate::{
    api::{ApiContext, iceberg::types::Prefix},
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{CatalogStore, SecretStore, State as ServiceState, authz::Authorizer},
};

type Ctx<A, C, S> = ApiContext<ServiceState<A, C, S>>;

/// Build the Delta v1 sub-router. Mounted by the host under
/// `/catalog/v1/{prefix}/delta/v1`, so route paths here are relative to that base.
pub(crate) fn router<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>()
-> Router<Ctx<A, C, S>> {
    Router::new()
        .route("/config", get(get_config::<C, A, S>))
        .route(
            "/catalogs/{catalog}/schemas/{schema}/staging-tables",
            post(create_staging_table::<C, A, S>),
        )
        .route(
            "/catalogs/{catalog}/schemas/{schema}/tables",
            post(create_table::<C, A, S>),
        )
        .route(
            "/catalogs/{catalog}/schemas/{schema}/tables/{table}",
            get(load_table::<C, A, S>)
                .post(update_table::<C, A, S>)
                .delete(delete_table::<C, A, S>)
                .head(table_exists::<C, A, S>),
        )
        .route(
            "/catalogs/{catalog}/schemas/{schema}/tables/{table}/rename",
            post(rename_table::<C, A, S>),
        )
        .route(
            "/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials",
            get(get_table_credentials::<C, A, S>),
        )
        .route(
            "/catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics",
            post(report_metrics::<C, A, S>),
        )
        .route(
            "/staging-tables/{table_id}/credentials",
            get(get_staging_table_credentials::<C, A, S>),
        )
        .route(
            "/temporary-path-credentials",
            get(get_temporary_path_credentials::<C, A, S>),
        )
}

// ----- helpers ---------------------------------------------------------------

/// Build the adapter + request context for a call. The warehouse comes from the
/// `{prefix}` path segment (the Delta API has no warehouse coordinate of its own).
fn prepare<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    ctx: Ctx<A, C, S>,
    prefix: &str,
    metadata: RequestMetadata,
) -> Result<(LakekeeperDeltaBackend<C, A, S>, DeltaRequestContext), DeltaApiError> {
    let warehouse_id = require_warehouse_id(Some(&Prefix(prefix.to_string())))
        .map_err(|e| DeltaApiError::invalid_argument(e.message))?;
    let backend = LakekeeperDeltaBackend::new(ctx);
    let request_context = DeltaRequestContext {
        metadata,
        warehouse_id,
    };
    Ok((backend, request_context))
}

// ----- path/query parameter helpers ------------------------------------------

#[derive(Debug, Deserialize)]
struct SchemaPath {
    prefix: String,
    catalog: String,
    schema: String,
}

#[derive(Debug, Deserialize)]
struct TablePath {
    prefix: String,
    catalog: String,
    schema: String,
    table: String,
}

#[derive(Debug, Deserialize)]
struct GetConfigParams {
    catalog: String,
    #[serde(rename = "protocol-versions")]
    protocol_versions: String,
}

#[derive(Debug, Deserialize)]
struct OperationParam {
    operation: DeltaCredentialOperation,
}

#[derive(Debug, Deserialize)]
struct PathCredentialParams {
    location: String,
    operation: DeltaCredentialOperation,
}

// ----- handlers --------------------------------------------------------------

async fn get_config<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(prefix): Path<String>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
    Query(params): Query<GetConfigParams>,
) -> DeltaApiResult<Json<DeltaCatalogConfig>> {
    let (backend, cx) = prepare(ctx, &prefix, metadata)?;
    let query = GetConfigQuery {
        catalog: params.catalog,
        protocol_versions: params.protocol_versions,
    };
    Ok(Json(backend.get_config(query, cx).await?))
}

async fn create_staging_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(path): Path<SchemaPath>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
    Json(request): Json<DeltaCreateStagingTableRequest>,
) -> DeltaApiResult<Json<DeltaStagingTableResponse>> {
    let (backend, cx) = prepare(ctx, &path.prefix, metadata)?;
    let at = SchemaRef {
        catalog: path.catalog,
        schema: path.schema,
    };
    Ok(Json(backend.create_staging_table(at, request, cx).await?))
}

async fn create_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(path): Path<SchemaPath>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
    Json(request): Json<DeltaCreateTableRequest>,
) -> DeltaApiResult<Json<DeltaLoadTableResponse>> {
    let (backend, cx) = prepare(ctx, &path.prefix, metadata)?;
    let at = SchemaRef {
        catalog: path.catalog,
        schema: path.schema,
    };
    Ok(Json(backend.create_table(at, request, cx).await?))
}

async fn load_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(path): Path<TablePath>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
) -> DeltaApiResult<Json<DeltaLoadTableResponse>> {
    let (backend, cx) = prepare(ctx, &path.prefix, metadata)?;
    Ok(Json(backend.load_table(table_ref(path), cx).await?))
}

async fn update_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(path): Path<TablePath>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
    Json(request): Json<DeltaUpdateTableRequest>,
) -> DeltaApiResult<Json<DeltaLoadTableResponse>> {
    let (backend, cx) = prepare(ctx, &path.prefix, metadata)?;
    Ok(Json(
        backend.update_table(table_ref(path), request, cx).await?,
    ))
}

async fn delete_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(path): Path<TablePath>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
) -> DeltaApiResult<StatusCode> {
    let (backend, cx) = prepare(ctx, &path.prefix, metadata)?;
    backend.delete_table(table_ref(path), cx).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn table_exists<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(path): Path<TablePath>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
) -> DeltaApiResult<StatusCode> {
    let (backend, cx) = prepare(ctx, &path.prefix, metadata)?;
    backend.table_exists(table_ref(path), cx).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn rename_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(path): Path<TablePath>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
    Json(request): Json<DeltaRenameTableRequest>,
) -> DeltaApiResult<StatusCode> {
    let (backend, cx) = prepare(ctx, &path.prefix, metadata)?;
    backend.rename_table(table_ref(path), request, cx).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_table_credentials<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(path): Path<TablePath>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
    Query(params): Query<OperationParam>,
) -> DeltaApiResult<Json<DeltaCredentialsResponse>> {
    let (backend, cx) = prepare(ctx, &path.prefix, metadata)?;
    Ok(Json(
        backend
            .get_table_credentials(table_ref(path), params.operation, cx)
            .await?,
    ))
}

async fn report_metrics<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(path): Path<TablePath>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
    Json(request): Json<DeltaReportMetricsRequest>,
) -> DeltaApiResult<StatusCode> {
    let (backend, cx) = prepare(ctx, &path.prefix, metadata)?;
    backend.report_metrics(table_ref(path), request, cx).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_staging_table_credentials<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path((prefix, table_id)): Path<(String, String)>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
) -> DeltaApiResult<Json<DeltaCredentialsResponse>> {
    let (backend, cx) = prepare(ctx, &prefix, metadata)?;
    Ok(Json(
        backend.get_staging_table_credentials(table_id, cx).await?,
    ))
}

async fn get_temporary_path_credentials<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    State(ctx): State<Ctx<A, C, S>>,
    Path(prefix): Path<String>,
    axum::Extension(metadata): axum::Extension<RequestMetadata>,
    Query(params): Query<PathCredentialParams>,
) -> DeltaApiResult<Json<DeltaCredentialsResponse>> {
    let (backend, cx) = prepare(ctx, &prefix, metadata)?;
    Ok(Json(
        backend
            .get_temporary_path_credentials(params.location, params.operation, cx)
            .await?,
    ))
}

fn table_ref(path: TablePath) -> TableRef {
    TableRef {
        catalog: path.catalog,
        schema: path.schema,
        table: path.table,
    }
}
