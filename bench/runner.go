package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	Engine      string
	DSN         string
	Workload    string
	Concurrency int
	Warmup      time.Duration
	Duration    time.Duration
	TxBatch     int
	Rows        int
}

type Result struct {
	Name        string        `json:"name"`
	Concurrency int           `json:"concurrency"`
	Duration    time.Duration `json:"duration"`
	Ops         int64         `json:"ops"`
	Errors      int64         `json:"errors"`
	P50         time.Duration `json:"p50"`
	P95         time.Duration `json:"p95"`
	P99         time.Duration `json:"p99"`
}

func (r Result) Pretty() string {
	b, _ := json.MarshalIndent(r, "", " ")
	return string(b)
}

type histogram struct{ samples []time.Duration }

func (h *histogram) add(d time.Duration) { h.samples = append(h.samples, d) }
func (h *histogram) quantile(q float64) time.Duration {
	if len(h.samples) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), h.samples...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(float64(len(s)-1) * q)
	return s[idx]
}

func newResult(name string, conc int, dur time.Duration) Result {
	return Result{Name: name, Concurrency: conc, Duration: dur}
}

func (r *Result) addLatency(d time.Duration) { atomic.AddInt64(&r.Ops, 1) /* count per op only */ }
func (r *Result) addError(err error)         { atomic.AddInt64(&r.Errors, 1) }

func (r Result) finalize() Result { return r }

// Run executes init -> warmup -> selected workload run.
func Run(ctx context.Context, cfg Config) (Result, error) {
	db, err := Open(cfg.Engine, cfg.DSN)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()

	// schema & data
	if err := initSchema(ctx, db, cfg.Engine); err != nil {
		return Result{}, err
	}
	if err := loadKV(ctx, db, cfg.Engine, cfg.Rows, max(1, cfg.TxBatch)); err != nil {
		return Result{}, err
	}

	// select workload
	var wl WorkloadFunc
	switch strings.ToLower(cfg.Workload) {
	case "point":
		wl = pointWorkload(cfg.Engine)
	case "range":
		wl = rangeWorkload(cfg.Engine, 100)
	case "insert":
		wl = insertWorkload(cfg.Engine, max(1, cfg.TxBatch))
	default:
		return Result{}, fmt.Errorf("unknown workload: %s", cfg.Workload)
	}

	// warmup
	_ = wl(ctx, db, cfg.Concurrency, cfg.Warmup)
	// timed
	res := wl(ctx, db, cfg.Concurrency, cfg.Duration)
	return res, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
