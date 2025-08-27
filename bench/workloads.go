package bench

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

type WorkloadFunc func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result

func pointWorkload(engine string) WorkloadFunc {
	query := `SELECT v FROM kv WHERE k = ?`
	if engine == "pgx" {
		query = `SELECT v FROM kv WHERE k = $1`
	}
	return func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result {
		stmt, _ := db.PrepareContext(ctx, query)
		defer stmt.Close()
		var wg sync.WaitGroup
		res := newResult("point", conc, dur)
		ctx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					key := fmt.Sprintf("key-%08d", id)
					start := time.Now()
					row := stmt.QueryRowContext(ctx, key)
					var v []byte
					if err := row.Scan(&v); err != nil {
						res.addError(err)
						continue
					}
					res.addLatency(time.Since(start))
				}
			}(w)
		}
		wg.Wait()
		return res.finalize()
	}
}

func rangeWorkload(engine string, limit int) WorkloadFunc {
	query := `SELECT k,v FROM kv WHERE k BETWEEN ? AND ? LIMIT ?`
	if engine == "pgx" {
		query = `SELECT k,v FROM kv WHERE k BETWEEN $1 AND $2 LIMIT $3`
	}
	return func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result {
		stmt, _ := db.PrepareContext(ctx, query)
		defer stmt.Close()
		var wg sync.WaitGroup
		res := newResult("range", conc, dur)
		ctx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					lo := fmt.Sprintf("key-%08d", id*1000)
					hi := fmt.Sprintf("key-%08d", id*1000+999)
					start := time.Now()
					rows, err := stmt.QueryContext(ctx, lo, hi, limit)
					if err != nil {
						res.addError(err)
						continue
					}
					for rows.Next() {
						var k string
						var v []byte
						_ = rows.Scan(&k, &v)
					}
					rows.Close()
					res.addLatency(time.Since(start))
				}
			}(w)
		}
		wg.Wait()
		return res.finalize()
	}
}

func insertWorkload(engine string, batch int) WorkloadFunc {
	q := `INSERT INTO kv(k, v) VALUES(?, ?)`
	if engine == "pgx" {
		q = `INSERT INTO kv(k, v) VALUES($1, $2)`
	}
	return func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result {
		res := newResult("insert", conc, dur)
		ctx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()
		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						res.addError(err)
						continue
					}
					stmt, _ := tx.PrepareContext(ctx, q)
					for i := 0; i < batch; i++ {
						k := fmt.Sprintf("w%02d-%d", worker, time.Now().UnixNano())
						v := []byte("payload")
						start := time.Now()
						if _, err := stmt.ExecContext(ctx, k, v); err != nil {
							res.addError(err)
						}
						res.addLatency(time.Since(start))
					}
					stmt.Close()
					_ = tx.Commit()
				}
			}(w)
		}
		wg.Wait()
		return res.finalize()
	}
}
