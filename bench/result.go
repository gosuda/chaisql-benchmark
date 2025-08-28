package bench

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type Result struct {
	Workload    string        `json:"workload"`
	Concurrency int           `json:"concurrency"`
	Duration    time.Duration `json:"duration"`
	Ops         int64         `json:"ops"`
	Errors      int64         `json:"errors"`
	P50         time.Duration `json:"p50"`
	P95         time.Duration `json:"p95"`
	P99         time.Duration `json:"p99"`

	// internal
	hist          histogram          `json:"-"`
	latCh         chan time.Duration `json:"-"`
	collectorDone chan struct{}      `json:"-"`
}

// --------- histogram + quantile ---------

type histogram struct{ samples []time.Duration }

func (h *histogram) add(d time.Duration) { h.samples = append(h.samples, d) }

func (h *histogram) quantile(q float64) time.Duration {
	if len(h.samples) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), h.samples...)
	slices.Sort(s)
	idx := int(float64(len(s)-1) * q)
	return s[idx]
}

func (h *histogram) export() []time.Duration {
	if len(h.samples) == 0 {
		return nil
	}
	out := make([]time.Duration, len(h.samples))
	copy(out, h.samples)
	return out
}

// --------- constructors & updates ---------

func newResult(name string, conc int, dur time.Duration) *Result {
	r := &Result{
		Workload:      name,
		Concurrency:   conc,
		Duration:      dur,
		latCh:         make(chan time.Duration, 1<<16),
		collectorDone: make(chan struct{}),
	}
	go r.collector()
	return r
}

func (r *Result) collector() {
	for d := range r.latCh {
		r.hist.add(d)
		atomic.AddInt64(&r.Ops, 1)
	}
	close(r.collectorDone)
}

func (r *Result) addLatency(d time.Duration) {
	r.latCh <- d
}

func (r *Result) addErrorCnt(_ error) { atomic.AddInt64(&r.Errors, 1) }
func (r *Result) finalize() Result {
	close(r.latCh)
	<-r.collectorDone

	r.P50 = r.hist.quantile(0.50)
	r.P95 = r.hist.quantile(0.95)
	r.P99 = r.hist.quantile(0.99)
	return *r
}

// --------- pretty printers ---------
func (r Result) Pretty() string {
	opsPerSec := 0.0
	if r.Duration > 0 {
		opsPerSec = float64(r.Ops) / r.Duration.Seconds()
	}
	errRate := 0.0
	if r.Ops > 0 {
		errRate = float64(r.Errors) * 100 / float64(r.Ops)
	}

	minDur, maxDur, spark := sparkline(r.hist.samples, 14)

	var b strings.Builder
	fmt.Fprintf(&b, "Workload\t: %s\n", r.Workload)
	fmt.Fprintf(&b, "Concurrency\t: %d\n", r.Concurrency)
	fmt.Fprintf(&b, "Duration\t: %s\n", r.Duration)
	fmt.Fprintf(&b, "Ops\t\t\t: %s (%.1f ops/s)\n", commaI(r.Ops), opsPerSec)
	fmt.Fprintf(&b, "Errors\t\t: %s (%.2f%%)\n", commaI(r.Errors), errRate)
	fmt.Fprintf(&b, "Latency\t\t: P50=%s  P95=%s  P99=%s\n", fDur(r.P50), fDur(r.P95), fDur(r.P99))
	if spark != "" {
		fmt.Fprintf(&b, "Histogram\t: %s  (min %s, max %s)\n", spark, fDur(minDur), fDur(maxDur))
	}
	return b.String()
}

func (r Result) JSON() string {
	j, _ := json.MarshalIndent(r, "", "  ")
	return string(j)
}

func sparkline(samples []time.Duration, bins int) (time.Duration, time.Duration, string) {
	if len(samples) == 0 || bins <= 0 {
		return 0, 0, ""
	}

	s := append([]time.Duration(nil), samples...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })

	for i, v := range s {
		if v <= 0 {
			s[i] = time.Nanosecond
		}
	}

	lo := quantileDur(s, 0.01)
	hi := quantileDur(s, 0.99)
	if hi <= lo {
		return s[0], s[len(s)-1], strings.Repeat("█", bins)
	}

	lf := func(d time.Duration) float64 { return math.Log(float64(d)) }
	lmin, lmax := lf(lo), lf(hi)

	counts := make([]int, bins)
	for _, d := range s {
		x := lf(d)
		if x < lmin {
			x = lmin
		}
		if x > lmax {
			x = lmax
		}
		ratio := (x - lmin) / (lmax - lmin)
		idx := int(ratio * float64(bins-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= bins {
			idx = bins - 1
		}
		counts[idx]++
	}

	maxCnt := 0
	for _, c := range counts {
		if c > maxCnt {
			maxCnt = c
		}
	}
	if maxCnt == 0 {
		return s[0], s[len(s)-1], ""
	}
	chars := []rune("▁▂▃▄▅▆▇█")
	var sb strings.Builder
	for _, c := range counts {
		level := int(math.Round((float64(c) / float64(maxCnt)) * float64(len(chars)-1)))
		if level < 0 {
			level = 0
		}
		if level >= len(chars) {
			level = len(chars) - 1
		}
		sb.WriteRune(chars[level])
	}

	return lo, hi, sb.String()
}

func commaI(v int64) string {
	s := fmt.Sprintf("%d", v)
	n := len(s)
	if v < 0 {
		n-- // minus sign
	}
	for i := n - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func quantileDur(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(float64(len(sorted)-1) * q)
	return sorted[idx]
}

func fDur(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d)
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fµs", float64(d)/float64(time.Microsecond))
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.2fs", float64(d)/float64(time.Second))
}
