package bench

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"

	embed "github.com/gosuda/chaisql-benchmark/sql"
)

func initSchema(ctx context.Context, db *sql.DB, engine string) error {
	var schema string
	switch engine {
	case "pgx":
		schema = embed.PgSchema
	case "sqlite3":
		schema = embed.SqliteSchema
	case "chai":
		schema = embed.ChaiSchema
	default:
		return fmt.Errorf("unsupported engine: %s", engine)
	}
	_, err := db.ExecContext(ctx, schema)
	return err
}

func loadKV(ctx context.Context, db *sql.DB, engine string, rows, batch int) error {
	// idempotent upsert-like via INSERT OR REPLACE (sqlite/chai) vs PG uses ON CONFLICT
	// to keep it portable, pre-clean table.
	if _, err := db.ExecContext(ctx, `DELETE FROM kv`); err != nil {
		return err
	}

	kStmt := `INSERT INTO kv(k, v) VALUES(?, ?)`
	if engine == "pgx" { // PG uses $1, $2 placeholders
		kStmt = `INSERT INTO kv(k, v) VALUES($1, $2)`
	}

	stmt, err := db.PrepareContext(ctx, kStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 0; i < rows; i += batch {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for j := 0; j < batch && i+j < rows; j++ {
			k := fmt.Sprintf("key-%08d", i+j)
			v := randBytes(64)
			if _, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, k, v); err != nil {
				tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func isPG(db *sql.DB) bool {
	// crude: sql.DB doesn't expose driver name; callers can pass engine string instead.
	// kept here to show placeholder switching pattern; we actually pass engine in caller.
	return false
}
