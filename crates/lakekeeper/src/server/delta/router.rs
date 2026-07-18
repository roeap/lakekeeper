//! Mounts the `unitycatalog-delta-api` crate's composable router into Lakekeeper.
//!
//! The crate router is **state-agnostic and host-composable**
//! ([`router_with_context_at`](unitycatalog_delta_api::router_with_context_at)):
//! it returns an *unstated* [`Router<S>`] over Lakekeeper's own
//! [`ApiContext`]-derived state, so it nests directly inside the
//! request-metadata/auth layers instead of Lakekeeper re-declaring every route.
//! The crate owns the full Delta surface; Lakekeeper supplies only two things:
//!
//! - the **backend handler** — [`LakekeeperDeltaBackend`], built once at mount time
//!   from the cloned [`ApiContext`] (not per request);
//! - an **async context extractor** — the closure below, which builds
//!   [`DeltaRequestContext`] (the crate's `Cx`) from the request head: the caller
//!   [`RequestMetadata`] (installed as an extension by the outer middleware) plus
//!   the [`WarehouseId`] resolved from the URL `{prefix}` (awaited via `Path`).
//!
//! Building `Cx` in the async extractor — rather than a pre-staging middleware — is
//! what the async [`ContextExtractor`] upstream enables; `{prefix}` is a matched
//! path-param value, reachable only through the async `Path` extractor. This
//! mirrors the crate's `path_segment_derived_context_builds_in_extractor`
//! acceptance test. (Composability + async extractor: mangrove#135 / #142, resolved.)
//!
//! Mounted at `/catalog/v1/{prefix}/delta/v1` (the warehouse `{prefix}` precedes
//! the spec-fixed `/delta/v1` base); `base = ""` here so route paths stay
//! spec-relative and the host `.nest` adds the prefix.

// The crate's `ContextExtractor` returns `Result<Cx, Response>`; the `Response`
// error arm is intentional (a short-circuit HTTP response), so the large-err lint
// does not apply.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use axum::{
    Router,
    extract::{FromRequestParts, Path},
    response::IntoResponse,
};
use unitycatalog_delta_api::{ContextExtractor, error::DeltaApiError, router_with_context_at};

use super::{
    backend::LakekeeperDeltaBackend,
    context::{DeltaRequestContext, PrefixOnly},
};
use crate::{
    api::{ApiContext, iceberg::types::Prefix},
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{CatalogStore, SecretStore, State as ServiceState, authz::Authorizer},
};

type Ctx<A, C, S> = ApiContext<ServiceState<A, C, S>>;

/// Build the Delta v1 sub-router over Lakekeeper's [`ApiContext`] state.
///
/// The host mounts this under `/catalog/v1/{prefix}/delta/v1` (via `.nest`), so the
/// crate routes are spec-relative (`base = ""`). `state` is cloned once into the
/// backend handler; the per-request context is produced by the async extractor.
pub(crate) fn router<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    state: Ctx<A, C, S>,
) -> Router<Ctx<A, C, S>> {
    // Built once from the cloned `ApiContext`, type-erased to the crate's handler
    // port — no per-request construction.
    let handler = Arc::new(LakekeeperDeltaBackend::new(state));

    // Async context extractor: reads the caller metadata from the request extension
    // and the warehouse from the URL `{prefix}` (awaited via `Path`). Mirrors the
    // crate's `path_segment_derived_context_builds_in_extractor` test.
    let extract_cx: ContextExtractor<DeltaRequestContext> = Arc::new(|parts| {
        Box::pin(async move {
            let metadata = parts
                .extensions
                .get::<RequestMetadata>()
                .cloned()
                .ok_or_else(|| {
                    DeltaApiError::unauthenticated("missing request context").into_response()
                })?;
            // `Path` is async — only awaitable now that the extractor is async.
            let Path(PrefixOnly { prefix }) = Path::<PrefixOnly>::from_request_parts(parts, &())
                .await
                .map_err(IntoResponse::into_response)?;
            let warehouse_id = require_warehouse_id(Some(&Prefix(prefix)))
                .map_err(|e| DeltaApiError::invalid_argument(e.message).into_response())?;
            Ok(DeltaRequestContext {
                metadata,
                warehouse_id,
            })
        })
    });

    router_with_context_at("", handler, extract_cx)
}

#[cfg(test)]
mod tests {
    //! Router-composition smoke test, mirroring the crate's
    //! `path_segment_derived_context_builds_in_extractor` acceptance test but at
    //! Lakekeeper's real mount shape `/catalog/v1/{prefix}/delta/v1/...`.
    //!
    //! It builds the crate's composable router with an async context extractor that
    //! reads the `{prefix}` path segment (as the real [`router`] does), nests it at
    //! the production path, and drives a `GET .../config` request through it. A `200`
    //! proves routing + the async `{prefix}` extractor + query extraction all fire.
    //! It uses the crate's [`InMemoryDeltaBackend`] (context-agnostic) so the test
    //! exercises the composition without Lakekeeper's full `ApiContext` harness.

    use std::sync::Arc;

    use axum::{
        Router,
        body::{Body, to_bytes},
        extract::{FromRequestParts, Path},
        http::{Request, StatusCode},
        response::IntoResponse,
    };
    use tower::ServiceExt; // oneshot
    use unitycatalog_delta_api::{
        ContextExtractor, error::DeltaApiError, router_with_context_at,
        testing::InMemoryDeltaBackend,
    };

    use super::PrefixOnly;

    /// The in-memory backend ignores its context type, so `Cx = ()` here — the test
    /// still routes through the same async extractor that reads `{prefix}`.
    type Cx = ();

    /// Compose the crate router under the production mount path with an async
    /// extractor that reads (and enforces) the `{prefix}` segment.
    fn app() -> Router {
        let extract_cx: ContextExtractor<Cx> = Arc::new(|parts| {
            Box::pin(async move {
                let Path(PrefixOnly { prefix }) =
                    Path::<PrefixOnly>::from_request_parts(parts, &())
                        .await
                        .map_err(IntoResponse::into_response)?;
                if prefix.is_empty() {
                    return Err(DeltaApiError::invalid_argument("empty prefix").into_response());
                }
                Ok(())
            })
        });

        let delta: Router<()> =
            router_with_context_at("", Arc::new(InMemoryDeltaBackend::new()), extract_cx);

        Router::new()
            .nest("/catalog/v1/{prefix}/delta/v1", delta)
            .with_state(())
    }

    #[tokio::test]
    async fn config_routes_through_prefix_scoped_mount() {
        // `catalog` is the in-memory backend's default catalog, so a well-formed
        // request with a negotiable version succeeds — proving routing, the async
        // `{prefix}` extractor, and query extraction all fired at the real mount.
        let response = app()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(
                        "/catalog/v1/wh1/delta/v1/config?catalog=catalog&protocol-versions=1.1,2.3",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.get("protocol-version").is_some(), "body: {json}");
    }
}
