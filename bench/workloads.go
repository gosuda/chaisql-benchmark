package bench

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type WorkloadFunc func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result

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
		for w := range conc {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				gen, err := NewRandflake(worker)
				if err != nil {
					res.addErrorCnt(err)
					return
				}

				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						res.addErrorCnt(err)
						continue
					}
					stmt, err := tx.PrepareContext(ctx, q)
					if err != nil {
						log.Error().Err(err).Msg("failed to prepare statement")
						_ = tx.Rollback()
						continue
					}
					for i := 0; i < batch; i++ {
						k, err := gen.GenerateString()
						if err != nil {
							res.addErrorCnt(err)
							continue
						}

						v := []byte("payload")
						start := time.Now()
						if _, err := stmt.ExecContext(ctx, k, v); err != nil {
							res.addErrorCnt(err)
							continue
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

func selectWorkload(engine string) WorkloadFunc {
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
						res.addErrorCnt(err)
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
						res.addErrorCnt(err)
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

func updateWorkload(engine string) WorkloadFunc {
	q := `UPDATE kv SET v = ? WHERE k = ?`
	qPick := `SELECT k FROM kv ORDER BY k DESC LIMIT 1`
	if engine == "pgx" {
		q = `UPDATE kv SET v = $1 WHERE k = $2`
		qPick = `SELECT k FROM kv ORDER BY k DESC LIMIT 1`
	}
	return func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result {
		res := newResult("update", conc, dur)
		ctx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()

		stmtUpd, err := db.PrepareContext(ctx, q)
		if err != nil {
			res.addErrorCnt(err)
			return res.finalize()
		}
		defer stmtUpd.Close()

		stmtPick, err := db.PrepareContext(ctx, qPick)
		if err != nil {
			res.addErrorCnt(err)
			return res.finalize()
		}
		defer stmtPick.Close()

		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					var k string
					if err := stmtPick.QueryRowContext(ctx).Scan(&k); err != nil {
						res.addErrorCnt(err)
						continue
					}
					start := time.Now()
					if _, err := stmtUpd.ExecContext(ctx, []byte("updated"), k); err != nil {
						res.addErrorCnt(err)
						continue
					}
					res.addLatency(time.Since(start))
				}
			}()
		}
		wg.Wait()
		return res.finalize()
	}
}

func deleteWorkload(engine string) WorkloadFunc {
	q := `DELETE FROM kv WHERE k = ?`
	qPick := `SELECT k FROM kv ORDER BY k ASC LIMIT 1`
	if engine == "pgx" {
		q = `DELETE FROM kv WHERE k = $1`
		qPick = `SELECT k FROM kv ORDER BY k ASC LIMIT 1`
	}
	return func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result {
		res := newResult("delete", conc, dur)
		ctx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()

		stmtDel, err := db.PrepareContext(ctx, q)
		if err != nil {
			res.addErrorCnt(err)
			return res.finalize()
		}
		defer stmtDel.Close()

		stmtPick, err := db.PrepareContext(ctx, qPick)
		if err != nil {
			res.addErrorCnt(err)
			return res.finalize()
		}
		defer stmtPick.Close()

		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					var k string
					if err := stmtPick.QueryRowContext(ctx).Scan(&k); err != nil {
						res.addErrorCnt(err)
						continue
					}
					start := time.Now()
					if _, err := stmtDel.ExecContext(ctx, k); err != nil {
						res.addErrorCnt(err)
						continue
					}
					res.addLatency(time.Since(start))
				}
			}()
		}
		wg.Wait()
		return res.finalize()
	}
}
