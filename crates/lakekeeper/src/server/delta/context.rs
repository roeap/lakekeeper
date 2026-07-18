//! [`DeltaRequestContext`] — the context passed to each
//! [`DeltaBackend`](unitycatalog_delta_api::DeltaBackend) method (the crate's
//! `Cx`).
//!
//! It carries the two things every backend call needs: the caller
//! [`RequestMetadata`] and the [`WarehouseId`] the request is scoped to. Both are
//! assembled by the route handlers in [`super::router`] — the metadata from the
//! request `Extension` the middleware installs, and the warehouse from the URL
//! `{prefix}` — so the context is a plain value here (no extractor).

use crate::{request_metadata::RequestMetadata, service::WarehouseId};

/// The Delta API request context: the caller metadata plus the warehouse resolved
/// from the URL `{prefix}`.
#[derive(Clone, Debug)]
pub(crate) struct DeltaRequestContext {
    /// Caller identity / request metadata.
    pub(crate) metadata: RequestMetadata,
    /// The warehouse this request is scoped to.
    pub(crate) warehouse_id: WarehouseId,
}
