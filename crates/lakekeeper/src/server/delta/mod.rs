//! Lakekeeper's backend for the portable [`unitycatalog_delta_api`] crate.
//!
//! The crate owns every Delta *semantic* (the wire models, the managed-table
//! contract, the `updateTable` action dispatcher, `loadTable` list construction,
//! the commit arbitration, the error envelope, and the axum router). Lakekeeper
//! implements only the narrow [`DeltaBackend`](unitycatalog_delta_api::DeltaBackend)
//! **port** over its existing generic-table storage, credential vending, and
//! authorization — plus the request context the crate router threads through.
//!
//! This milestone wires the **read path** only: `getConfig`, `loadTable`,
//! `tableExists`, and credential vending. Write/commit and managed/staging tables
//! return [`DeltaBackendError::NotImplemented`](unitycatalog_delta_api::DeltaBackendError::NotImplemented)
//! and are tracked as follow-ups.
//!
//! # Routing & warehouse scoping
//!
//! The UC Delta paths carry no warehouse segment, and Lakekeeper identifies a
//! warehouse only by a URL prefix. Lakekeeper mounts the crate router (see
//! [`router`]) under a warehouse `{prefix}` before the fixed `/delta/v1` base, so
//! the effective surface is `/catalog/v1/{prefix}/delta/v1/...`. The warehouse is
//! resolved from that prefix into `DeltaRequestContext` (the crate's `Cx`) inside
//! the async context extractor; the Delta `catalog` + `schema` coordinates map to
//! a two-level namespace within the warehouse.
//!
//! The crate router is **composable and is reused directly** — it returns an
//! unstated `Router<S>` over Lakekeeper's `ApiContext` state, so it nests inside
//! the middleware layers without Lakekeeper re-declaring any route. Lakekeeper
//! supplies only the backend handler and an async context extractor; the crate
//! owns every Delta behavior via the `DeltaApiHandler`/`DeltaBackend` port. The
//! composable router (and the async extractor that lets `Cx` read the `{prefix}`
//! path segment) landed upstream as `open-lakehouse/mangrove#135` / `#142`
//! (**resolved**).

mod backend;
mod context;
mod router;

pub(crate) use router::router;
