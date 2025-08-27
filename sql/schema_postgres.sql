-- PostgreSQL dialect schema (server)
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v BYTEA NOT NULL
);
CREATE INDEX IF NOT EXISTS kv_k_prefix ON kv (k);