CREATE TABLE users (
 id uuid PRIMARY KEY, email text NOT NULL UNIQUE, password_hash text NOT NULL,
 role text NOT NULL DEFAULT 'user' CHECK(role IN ('user','admin')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE catalogs (
 id uuid PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id), name text NOT NULL,
 version bigint NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(user_id,name)
);
CREATE TABLE products (
 catalog_id uuid NOT NULL REFERENCES catalogs(id), sku text NOT NULL, name text NOT NULL,
 price numeric(12,2) NOT NULL CHECK(price>=0), stock integer NOT NULL CHECK(stock>=0),
 fingerprint text NOT NULL, version bigint NOT NULL DEFAULT 1,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(catalog_id,sku)
);
CREATE TABLE import_jobs (
 id uuid PRIMARY KEY, catalog_id uuid NOT NULL REFERENCES catalogs(id), user_id uuid NOT NULL REFERENCES users(id),
 format text NOT NULL CHECK(format IN ('csv','jsonl','json')), column_map jsonb NOT NULL,
 delimiter text NOT NULL DEFAULT ',', mode text NOT NULL CHECK(mode IN ('upsert','validate_only')),
 error_policy text NOT NULL CHECK(error_policy IN ('skip','reject')),
 idempotency_key text NOT NULL, request_hash text NOT NULL, UNIQUE(user_id,idempotency_key),
 status text NOT NULL DEFAULT 'awaiting_upload' CHECK(status IN ('awaiting_upload','uploading','ready','running','paused','succeeded','failed','cancelled')),
 source_path text, source_sha256 text, source_size_bytes bigint,
 lease_token uuid, lease_until timestamptz, pause_requested boolean NOT NULL DEFAULT false,
 cancel_requested boolean NOT NULL DEFAULT false, recovery_count integer NOT NULL DEFAULT 0,
 processed_rows bigint NOT NULL DEFAULT 0, valid_rows bigint NOT NULL DEFAULT 0,
 invalid_rows bigint NOT NULL DEFAULT 0, checkpoint_bytes bigint NOT NULL DEFAULT 0,
 duplicate_rows bigint NOT NULL DEFAULT 0, inserted_rows bigint NOT NULL DEFAULT 0,
 updated_rows bigint NOT NULL DEFAULT 0, unchanged_rows bigint NOT NULL DEFAULT 0,
 peak_rss_bytes bigint NOT NULL DEFAULT 0, error text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), started_at timestamptz, finished_at timestamptz,
 CHECK(processed_rows=valid_rows+invalid_rows)
);
CREATE INDEX import_schedule ON import_jobs(status,lease_until,created_at);
CREATE TABLE staged_products (
 job_id uuid NOT NULL REFERENCES import_jobs(id) ON DELETE CASCADE, sku text NOT NULL,
 row_number bigint NOT NULL, name text NOT NULL, price numeric(12,2) NOT NULL, stock integer NOT NULL,
 fingerprint text NOT NULL, PRIMARY KEY(job_id,sku)
);
CREATE TABLE row_errors (
 job_id uuid NOT NULL REFERENCES import_jobs(id) ON DELETE CASCADE, row_number bigint NOT NULL,
 code text NOT NULL, PRIMARY KEY(job_id,row_number)
);
CREATE TABLE import_events (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, job_id uuid NOT NULL REFERENCES import_jobs(id) ON DELETE CASCADE,
 type text NOT NULL, details jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX import_events_job ON import_events(job_id,id);
CREATE TABLE worker_heartbeats (name text PRIMARY KEY,seen_at timestamptz NOT NULL DEFAULT clock_timestamp());
