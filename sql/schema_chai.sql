-- ChaiSQL dialect (kept close to SQLite)
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS kv_k_prefix ON kv(k);