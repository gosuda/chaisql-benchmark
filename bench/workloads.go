package bench

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
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
					for range batch {
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

func selectWorkload(engine string, keys []string) WorkloadFunc {
	query := `SELECT v FROM kv WHERE k = ?`
	if engine == "pgx" {
		query = `SELECT v FROM kv WHERE k = $1`
	}
	return func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result {
		res := newResult("select", conc, dur)
		if len(keys) == 0 {
			return res.finalize()
		}

		ctx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()

		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			res.addErrorCnt(err)
			return res.finalize()
		}
		defer stmt.Close()

		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					key := keys[rnd.Intn(len(keys))]
					start := time.Now()
					var v []byte
					if err := stmt.QueryRowContext(ctx, key).Scan(&v); err != nil {
						res.addErrorCnt(err) // 원한다면 ErrNoRows는 별도 계수
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

func rangeWorkload(engine string, keys []string, limit int) WorkloadFunc {
	query := `SELECT k,v FROM kv WHERE k BETWEEN ? AND ? LIMIT ?`
	if engine == "pgx" {
		query = `SELECT k,v FROM kv WHERE k BETWEEN $1 AND $2 LIMIT $3`
	}
	return func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result {
		res := newResult("range", conc, dur)
		if len(keys) < 2 {
			return res.finalize()
		}

		ctx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()

		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			res.addErrorCnt(err)
			return res.finalize()
		}
		defer stmt.Close()

		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					a := keys[rnd.Intn(len(keys))]
					b := keys[rnd.Intn(len(keys))]
					lo, hi := a, b
					if lo > hi {
						lo, hi = hi, lo
					}

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
					_ = rows.Close()
					res.addLatency(time.Since(start))
				}
			}(w)
		}
		wg.Wait()
		return res.finalize()
	}
}

func updateWorkload(engine string, keys []string) WorkloadFunc {
	q := `UPDATE kv SET v = ? WHERE k = ?`
	if engine == "pgx" {
		q = `UPDATE kv SET v = $1 WHERE k = $2`
	}
	return func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result {
		res := newResult("update", conc, dur)
		if len(keys) == 0 {
			return res.finalize()
		}

		ctx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()

		stmtUpd, err := db.PrepareContext(ctx, q)
		if err != nil {
			res.addErrorCnt(err)
			return res.finalize()
		}
		defer stmtUpd.Close()

		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					k := keys[rnd.Intn(len(keys))]
					start := time.Now()
					if _, err := stmtUpd.ExecContext(ctx, []byte("updated"), k); err != nil {
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

func deleteWorkload(engine string, keys []string) WorkloadFunc {
	q := `DELETE FROM kv WHERE k = ?`
	if engine == "pgx" {
		q = `DELETE FROM kv WHERE k = $1`
	}
	return func(ctx context.Context, db *sql.DB, conc int, dur time.Duration) Result {
		res := newResult("delete", conc, dur)
		if len(keys) == 0 {
			return res.finalize()
		}

		ctx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()

		stmtDel, err := db.PrepareContext(ctx, q)
		if err != nil {
			res.addErrorCnt(err)
			return res.finalize()
		}
		defer stmtDel.Close()

		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					k := keys[rnd.Intn(len(keys))]
					start := time.Now()
					if _, err := stmtDel.ExecContext(ctx, k); err != nil {
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
func FetchKeySnapshot(ctx context.Context, db *sql.DB, engine string, n int) ([]string, error) {
	q := fmt.Sprintf(`SELECT k FROM kv ORDER BY k DESC LIMIT %d`, n)

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make([]string, 0, n)
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("snapshot is empty: no keys fetched")
	}
	return keys, nil
}
