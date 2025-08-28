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
	switch e := strings.ToLower(engine); e {
	case "chai":
		p := strings.TrimPrefix(dsn, "file:")
		if p != "" && p != "." {
			_ = os.MkdirAll(filepath.Dir(filepath.FromSlash(p)), 0755)
		}
		return sql.Open(e, p)
	case "sqlite", "sqlite3":
		p := strings.TrimPrefix(dsn, "file:")
		if p != "" && p != "." {
			_ = os.MkdirAll(filepath.Dir(filepath.FromSlash(p)), 0755)
		}
		return sql.Open(e, dsn)
	case "pgx":
		return sql.Open(e, dsn)
	}
	return nil, fmt.Errorf("unknown engine: %s", engine)
}
