-- Add the UC Delta v1 API operations to the `api_endpoints` enum so endpoint
-- statistics can record them. These mirror the `DeltaV1` group in
-- `crates/lakekeeper/src/api/endpoints.rs`; the values are the kebab-case form of
-- each `EndpointFlat::DeltaV1*` variant (the `sqlx(type_name = "api_endpoints",
-- rename_all = "kebab-case")` mapping). Additive only — no existing rows change.
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-get-config';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-create-staging-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-create-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-load-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-update-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-delete-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-table-exists';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-rename-table';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-get-table-credentials';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-report-metrics';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-get-staging-table-credentials';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'delta-v1-get-temporary-path-credentials';
