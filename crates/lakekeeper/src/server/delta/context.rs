//! [`DeltaRequestContext`] — the request context the Delta crate router threads
//! through as its `Cx` type.
//!
//! The crate router extracts `Cx` via [`FromRequestParts`], then hands it to every
//! [`DeltaBackend`](unitycatalog_delta_api::DeltaBackend) method. Lakekeeper's
//! context carries two things the backend needs on every call:
//!
//! - the [`RequestMetadata`] (actor, project, request id), installed as an axum
//!   `Extension` by the request-metadata middleware and enriched by the auth
//!   middleware — both layered *above* the nested Delta router, so the extension
//!   is always present here; and
//! - the [`WarehouseId`], parsed from the outer `{prefix}` path segment (the Delta
//!   API has no warehouse coordinate of its own — see [`super`]).

use std::collections::HashMap;

use axum::{
    extract::{FromRequestParts, Path},
    response::{IntoResponse, Response},
};
use http::request::Parts;
use unitycatalog_delta_api::DeltaApiError;

use crate::{
    api::iceberg::types::Prefix, request_metadata::RequestMetadata, server::require_warehouse_id,
    service::WarehouseId,
};

/// The Delta API request context: the caller metadata plus the warehouse resolved
/// from the URL `{prefix}`.
#[derive(Clone, Debug)]
pub(crate) struct DeltaRequestContext {
    /// Caller identity / request metadata, cloned from the request extension the
    /// middleware stack installs.
    pub(crate) metadata: RequestMetadata,
    /// The warehouse this request is scoped to, parsed from the `{prefix}` segment.
    pub(crate) warehouse_id: WarehouseId,
}

impl<S> FromRequestParts<S> for DeltaRequestContext
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // The request-metadata middleware runs above this nested router and inserts
        // `RequestMetadata` into the extensions; absence means the layer was not
        // applied, which is a server misconfiguration (500), not a client error.
        let metadata = parts
            .extensions
            .get::<RequestMetadata>()
            .cloned()
            .ok_or_else(|| {
                DeltaApiError::from(unitycatalog_delta_api::DeltaBackendError::Internal(
                    "request metadata extension missing; Delta routes must be nested inside the \
                     request-metadata middleware"
                        .to_string(),
                ))
                .into_response()
            })?;

        // Read the OUTER `{prefix}` capture. Use a map extractor rather than a
        // single-field struct: a `Path<Struct>` tries to bind the *entire* captured
        // set and would collide with the crate handler's own `Path<TableRef>` over
        // `{catalog}/{schema}/{table}`. A `HashMap` reads just the key we want.
        let Path(params) = Path::<HashMap<String, String>>::from_request_parts(parts, state)
            .await
            .map_err(|rejection| rejection.into_response())?;

        let prefix = params.get("prefix").map(|p| Prefix(p.clone()));
        let warehouse_id = require_warehouse_id(prefix.as_ref()).map_err(|e| {
            // Reuse the Delta error envelope so a bad prefix reads as a Delta error
            // (400 InvalidParameterValueException) rather than an Iceberg one.
            DeltaApiError::invalid_argument(e.message).into_response()
        })?;

        Ok(Self {
            metadata,
            warehouse_id,
        })
    }
}
