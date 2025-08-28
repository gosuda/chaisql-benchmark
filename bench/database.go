package bench

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/chaisql/chai/driver" // "chai"
	_ "github.com/glebarez/go-sqlite"  // "sqlite"
	_ "github.com/jackc/pgx/v5/stdlib" // "pgx"
)

func Open(engine, dsn string) (*sql.DB, error) {
	switch strings.ToLower(engine) {
	case "chai":
		p := strings.TrimPrefix(dsn, "file:")
		if p != "" && p != "." {
			_ = os.MkdirAll(filepath.Dir(filepath.FromSlash(p)), 0o755)
		}
		db, err := sql.Open("chai", p)
		if err != nil {
			return nil, err
		}
		// db.SetMaxOpenConns(10)
		// db.SetMaxIdleConns(10)
		return db, nil

	case "sqlite", "sqlite3":
		driver := "sqlite"
		db, err := sql.Open(driver, dsn)
		if err != nil {
			return nil, err
		}
		return db, nil

	case "pgx":
		return sql.Open("pgx", dsn)
	}
	return nil, fmt.Errorf("unknown engine: %s", engine)
}
