package bench

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	embed "github.com/gosuda/chaisql-benchmark/sql"
	"github.com/rs/zerolog/log"
)

type Config struct {
	Engine      string
	DSN         string
	Concurrency int
	Warmup      time.Duration
	Duration    time.Duration
	TxBatch     int
}

func runPhase(ctx context.Context, db *sql.DB, conc int, warmup, dur time.Duration, wf WorkloadFunc) Result {
	if warmup > 0 {
		_ = wf(ctx, db, conc, warmup)
	}
	return wf(ctx, db, conc, dur)
}

func Run(ctx context.Context, cfg Config) ([]Result, error) {
	db, err := Open(cfg.Engine, cfg.DSN)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if err := initSchema(ctx, db, cfg.Engine); err != nil {
		return nil, err
	}

	insertW := insertWorkload(cfg.Engine, max(1, cfg.TxBatch))
	selectW := selectWorkload(cfg.Engine)
	rangeW := rangeWorkload(cfg.Engine, 100)
	updateW := updateWorkload(cfg.Engine)
	deleteW := deleteWorkload(cfg.Engine)

	results := make([]Result, 0, 5)

	// insert phase
	log.Info().Msg("1. insert workload start")
	results = append(results, runPhase(ctx, db, cfg.Concurrency, cfg.Warmup, cfg.Duration, insertW))

	// select phase
	log.Info().Msg("2. select workload start")
	results = append(results, runPhase(ctx, db, cfg.Concurrency, cfg.Warmup, cfg.Duration, selectW))

	// range phase
	log.Info().Msg("3. range workload start")
	results = append(results, runPhase(ctx, db, cfg.Concurrency, cfg.Warmup, cfg.Duration, rangeW))

	// update phase
	log.Info().Msg("4. update workload start")
	results = append(results, runPhase(ctx, db, cfg.Concurrency, cfg.Warmup, cfg.Duration, updateW))

	// delete phase
	log.Info().Msg("5. delete workload start")
	results = append(results, runPhase(ctx, db, cfg.Concurrency, cfg.Warmup, cfg.Duration, deleteW))

	log.Info().Msg("all workloads completed")
	return results, nil
}

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

func Open(engine, dsn string) (*sql.DB, error) {
	// engine must be one of: chai | sqlite3 | pgx
	return sql.Open(engine, dsn)
}
