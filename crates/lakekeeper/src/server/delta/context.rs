//! [`DeltaRequestContext`] — the context passed to each
//! [`DeltaBackend`](unitycatalog_delta_api::DeltaBackend) method (the crate's
//! `Cx`).
//!
//! It carries the two things every backend call needs: the caller
//! [`RequestMetadata`] and the [`WarehouseId`] the request is scoped to. Both are
//! assembled inside the async [`ContextExtractor`](unitycatalog_delta_api::ContextExtractor)
//! closure in [`super::router`] — the metadata from the request `Extension` the
//! middleware installs, and the warehouse from the URL `{prefix}` (awaited via
//! `Path`) — so this is a plain value here (no `FromRequestParts` impl).

use serde::Deserialize;

use crate::{request_metadata::RequestMetadata, service::WarehouseId};

/// The Delta API request context: the caller metadata plus the warehouse resolved
/// from the URL `{prefix}`.
///
/// `Clone + Send + Sync + 'static` (both fields are), as the crate's
/// [`ContextExtractor`](unitycatalog_delta_api::ContextExtractor) requires of `Cx`.
#[derive(Clone, Debug)]
pub(crate) struct DeltaRequestContext {
    /// Caller identity / request metadata.
    pub(crate) metadata: RequestMetadata,
    /// The warehouse this request is scoped to.
    pub(crate) warehouse_id: WarehouseId,
}

/// Deserializes just the leading `{prefix}` path segment (the warehouse
/// coordinate) from the mount `/catalog/v1/{prefix}/delta/v1/...`. Used by the
/// async context extractor to read `{prefix}` via `Path` without binding the
/// spec-relative `{catalog}`/`{schema}`/`{table}` segments the crate router owns.
#[derive(Debug, Deserialize)]
pub(crate) struct PrefixOnly {
    pub(crate) prefix: String,
}
