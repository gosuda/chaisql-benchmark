package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gosuda/chaisql-benchmark/bench"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
)

var k = koanf.New(".")

func main() {
	mustSetDefault("engine", "chai") // chai|sqlite|pgx
	mustSetDefault("dsn", "")        // auto-fill by engine if empty
	mustSetDefault("concurrency", 1)
	mustSetDefault("warmup", "5s")    // duration string
	mustSetDefault("duration", "20s") // duration string
	mustSetDefault("tx_batch", 1)
	mustSetDefault("rows", 10000)
	mustSetDefault("config", "config.yaml") // config file path

	cfgPath := k.String("config")
	if _, err := os.Stat(cfgPath); err == nil {
		if err := k.Load(file.Provider(cfgPath), yaml.Parser()); err != nil {
			log.Fatal().Err(err).Str("path", cfgPath).Msg("failed to load config file")
		}
	}

	if err := k.Load(env.Provider("CHB_", ".", func(s string) string {
		return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, "CHB_")), "_", ".")
	}), nil); err != nil {
		log.Fatal().Err(err).Msg("failed to load env")
	}

	fs := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	fs.String("config", k.String("config"), "config file path (yaml)")
	fs.String("engine", k.String("engine"), "chai|sqlite|pgx")
	fs.String("dsn", k.String("dsn"), "database DSN")
	fs.Int("concurrency", k.Int("concurrency"), "number of workers")
	fs.String("warmup", k.String("warmup"), "warmup duration (e.g. 10s)")
	fs.String("duration", k.String("duration"), "measurement duration (e.g. 30s)")
	fs.Int("tx-batch", k.Int("tx_batch"), "rows per transaction for write workload")
	fs.Int("rows", k.Int("rows"), "dataset size for kv table")

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if err := k.Load(posflag.Provider(fs, ".", k), nil); err != nil {
		log.Fatal().Err(err).Msg("failed to load flags")
	}

	engine := k.String("engine")
	dsn := k.String("dsn")
	if dsn == "" {
		switch engine {
		case "chai":
			dsn = "./data/chai/chai.db"
		case "sqlite":
			dsn = "file:./data/sqlite/sqlite.db?cache=shared&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
		case "pgx":
			dsn = "postgres://postgres:pg@127.0.0.1:5432/bench?sslmode=disable"
		default:
			log.Fatal().Str("engine", engine).Msg("no default DSN for engine")
		}
	}

	warmup, err := time.ParseDuration(k.String("warmup"))
	if err != nil {
		log.Fatal().Err(err).Str("warmup", k.String("warmup")).Msg("invalid warmup duration")
	}
	dur, err := time.ParseDuration(k.String("duration"))
	if err != nil {
		log.Fatal().Err(err).Str("duration", k.String("duration")).Msg("invalid duration")
	}

	cfg := bench.Config{
		Engine:      engine,
		DSN:         dsn,
		Concurrency: k.Int("concurrency"),
		Warmup:      warmup,
		Duration:    dur,
		TxBatch:     k.Int("tx_batch"),
	}

	ctx := context.Background()
	res, runErr := bench.Run(ctx, cfg)
	if runErr != nil {
		log.Fatal().Err(runErr).Msg("bench run failed")
	}
	for _, r := range res {
		fmt.Println(r.Pretty())
	}
}

func mustSetDefault(key string, v any) {
	if !k.Exists(key) {
		_ = k.Set(key, v)
	}
}
