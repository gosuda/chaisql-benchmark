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
	prefetch, err := FetchKeySnapshot(ctx, db, cfg.Engine, 2048)
	if err != nil {
		return nil, err
	}
	selectW := selectWorkload(cfg.Engine, prefetch)
	rangeW := rangeWorkload(cfg.Engine, prefetch, 100)
	updateW := updateWorkload(cfg.Engine, prefetch)
	deleteW := deleteWorkload(cfg.Engine, prefetch)

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
	case "sqlite":
		schema = embed.SqliteSchema
	case "chai":
		schema = embed.ChaiSchema
	default:
		return fmt.Errorf("unsupported engine: %s", engine)
	}
	_, err := db.ExecContext(ctx, schema)
	return err
}
