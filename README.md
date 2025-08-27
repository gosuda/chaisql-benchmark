# chaisql-benchmark

Benchmark suite comparing **ChaiSQL** with **go-sqlite** and **PostgreSQL server**, focusing on performance differences between embedded and server SQL engines.


## Engines
- ChaiSQL (embedded, PostgreSQL-like)
- go-sqlite (embedded)
- PostgreSQL server (separate host)


## Workloads (initial)
- `point` : primary-key single-row SELECT
- `range` : primary-key range scan with LIMIT
- `insert` : batched INSERT (configurable batch size)


## Quick start
```bash
# 1) start Postgres server (localhost as real server; for fair tests use a different host)
docker compose up -d


# 2) build bench runner
make build


# 3) run per engine (examples)
# ChaiSQL
./sqlbench -engine=chai -dsn="file:./data/chai.db" -workload=point -concurrency=16 -warmup=10s -duration=30s
# SQLite (CGO)
./sqlbench -engine=sqlite3 -dsn="file:./data/sqlite.db?_journal_mode=WAL&_synchronous=FULL" -workload=point -concurrency=16
# PostgreSQL (see .env or scripts/make_pg_dsn.sh)
./sqlbench -engine=pgx -dsn="postgres://postgres:pg@127.0.0.1:5432/bench?pool_max_conns=64" -workload=point -concurrency=16