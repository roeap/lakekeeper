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
//! warehouse only by a URL prefix. Lakekeeper declares the Delta routes (see
//! [`router`]) under a warehouse `{prefix}` before the fixed `/delta/v1` base, so
//! the effective surface is `/catalog/v1/{prefix}/delta/v1/...`. Each handler
//! parses the warehouse from that prefix into `DeltaRequestContext` (the crate's
//! `Cx`) and builds the adapter; the Delta `catalog` + `schema` coordinates map to
//! a two-level namespace within the warehouse.
//!
//! The routes are declared here rather than reusing the crate's `get_router`
//! because the crate makes the handler the axum `State` and returns a fully-stated
//! `Router`, which cannot compose into Lakekeeper's `ApiContext`-stated router
//! inside its middleware layers. The crate still owns every Delta behavior via the
//! `DeltaApiHandler`/`DeltaBackend` port. Tracked upstream for a composable
//! router: `open-lakehouse/mangrove#135`.

mod backend;
mod context;
mod router;

pub(crate) use router::router;
