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
//! warehouse only by a URL prefix. So the crate router is nested under a
//! warehouse `{prefix}` *before* its fixed `/delta/v1` base — the effective
//! surface is `/catalog/v1/{prefix}/delta/v1/...`. The warehouse is parsed from
//! that prefix into [`DeltaRequestContext`] (the crate's `Cx`), and the Delta
//! `catalog` + `schema` coordinates map to a two-level namespace within it.

mod backend;
mod context;

pub(crate) use backend::LakekeeperDeltaBackend;
pub(crate) use context::DeltaRequestContext;
