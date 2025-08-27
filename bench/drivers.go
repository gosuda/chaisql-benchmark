package bench

import "database/sql"

func Open(engine, dsn string) (*sql.DB, error) {
	// engine must be one of: chai | sqlite3 | pgx
	return sql.Open(engine, dsn)
}
