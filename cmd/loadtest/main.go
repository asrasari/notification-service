// Command loadtest is the `make load` driver (D-020 §2). It runs inside the
// isolated load-test compose project, next to the API, the worker and the
// fake provider. It sends 3,000 notifications through the batch API (1,000
// per channel; per channel the low ones first and the high ones last),
// waits until every one is sent, and prints:
//   - per channel: the throughput and the peak received in any 1-second
//     window, both measured by the fake provider (the receiving side);
//   - per priority: p50/p95 delivery latency (created_at → sent).
//
// It exits non-zero if a notification is not sent in time or a channel's
// peak exceeds the rate limit.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

var (
	channels   = []string{"sms", "email", "push"}
	priorities = []string{"low", "normal", "high"} // creation order: low first, high last
)

type config struct {
	apiURL, statsURL string
	perChannel       int
	rateLimit        int
	timeout          time.Duration
}

func main() {
	cfg := config{
		apiURL:     envOr("API_URL", "http://api:8080"),
		statsURL:   envOr("PROVIDER_STATS_URL", "http://fakeprovider:9090/stats"),
		perChannel: envInt("PER_CHANNEL", 1000),
		rateLimit:  envInt("RATE_LIMIT_PER_CHANNEL", 100),
		timeout:    3 * time.Minute,
	}
	if err := run(context.Background(), cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "load test failed:", err)
		os.Exit(1)
	}
}

type batch struct {
	id       string
	channel  string
	priority string
	ids      []string
}

func run(ctx context.Context, cfg config, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	c := &client{base: cfg.apiURL, http: &http.Client{Timeout: 30 * time.Second}}

	if err := c.waitHealthy(ctx); err != nil {
		return err
	}

	// 1,000 per channel, split over the priorities; low batches first.
	split := splitEven(cfg.perChannel, len(priorities))
	start := time.Now()
	var batches []batch
	for pi, p := range priorities {
		for _, ch := range channels {
			b, err := c.createBatch(ctx, ch, p, split[pi])
			if err != nil {
				return err
			}
			batches = append(batches, b)
		}
	}
	created := time.Since(start)
	total := cfg.perChannel * len(channels)
	fmt.Fprintf(out, "Load test: %d notifications (%d per channel) through POST /notifications/batch.\n", total, cfg.perChannel)
	fmt.Fprintf(out, "Per channel the low ones were created first and the high ones last (%v); all created in %dms.\n\n",
		splitLabel(split), created.Milliseconds())

	if err := c.waitAllSent(ctx, batches); err != nil {
		return err
	}
	fmt.Fprintf(out, "All %d sent %.1fs after the first batch.\n\n", total, time.Since(start).Seconds())

	stats, err := c.providerStats(ctx, cfg.statsURL)
	if err != nil {
		return err
	}
	latencies, err := c.latencies(ctx, batches)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintf(tw, "channel\treceived\tthroughput/s\tpeak in any 1s window\tlimit\t\n")
	var failures []string
	for _, ch := range channels {
		s := stats[ch]
		fmt.Fprintf(tw, "%s\t%d\t%.1f\t%d\t%d\t\n", ch, s.Received, s.throughput(), s.PeakPerSecond, cfg.rateLimit)
		if s.PeakPerSecond > cfg.rateLimit {
			failures = append(failures, fmt.Sprintf("%s peaked at %d/s, above the limit of %d", ch, s.PeakPerSecond, cfg.rateLimit))
		}
	}
	tw.Flush()
	fmt.Fprintf(out, "(measured by the fake provider as requests arrive; the worker paces at 99%% of the limit)\n\n")

	tw = tabwriter.NewWriter(out, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintf(tw, "priority\tcount\tp50\tp95\t\n")
	for _, p := range slices.Backward(priorities) {
		l := latencies[p]
		fmt.Fprintf(tw, "%s\t%d\t%.1fs\t%.1fs\t\n", p, len(l), percentile(l, 50).Seconds(), percentile(l, 95).Seconds())
	}
	tw.Flush()
	fmt.Fprintf(out, "(delivery latency = sent - created_at, all channels; weights 70/20/10)\n")

	if len(failures) > 0 {
		fmt.Fprintf(out, "\nFAIL: %s\n", strings.Join(failures, "; "))
		return errors.New(strings.Join(failures, "; "))
	}
	fmt.Fprintf(out, "\nPASS: every channel's peak is <= %d per second.\n", cfg.rateLimit)
	return nil
}

type client struct {
	base string
	http *http.Client
}

func (c *client) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, res.StatusCode, data)
	}
	return json.Unmarshal(data, out)
}

// waitHealthy waits until the API reports "ok": database up and a live
// worker with closed breakers.
func (c *client) waitHealthy(ctx context.Context) error {
	for {
		var h struct{ Status string }
		err := c.do(ctx, "GET", "/health", nil, &h)
		if err == nil && h.Status == "ok" {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("the stack did not become healthy: last status %q, error %v", h.Status, err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (c *client) createBatch(ctx context.Context, channel, priority string, n int) (batch, error) {
	type item struct {
		Recipient string `json:"recipient"`
		Channel   string `json:"channel"`
		Content   string `json:"content"`
		Priority  string `json:"priority"`
	}
	items := make([]item, n)
	for i := range items {
		items[i] = item{Recipient: recipient(channel, i), Channel: channel, Content: "Load test message " + strconv.Itoa(i), Priority: priority}
	}
	var res struct {
		BatchID string `json:"batch_id"`
		Items   []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := c.do(ctx, "POST", "/notifications/batch", map[string]any{"items": items}, &res); err != nil {
		return batch{}, err
	}
	b := batch{id: res.BatchID, channel: channel, priority: priority}
	for _, it := range res.Items {
		b.ids = append(b.ids, it.ID)
	}
	return b, nil
}

func recipient(channel string, i int) string {
	switch channel {
	case "sms":
		return fmt.Sprintf("+90555%07d", i)
	case "email":
		return fmt.Sprintf("load%d@example.com", i)
	}
	return fmt.Sprintf("device-token-%d", i)
}

// waitAllSent polls GET /batches/{id} until every notification is sent.
func (c *client) waitAllSent(ctx context.Context, batches []batch) error {
	for {
		done, failed := 0, 0
		for _, b := range batches {
			var s struct {
				Total  int            `json:"total"`
				Counts map[string]int `json:"counts"`
			}
			if err := c.do(ctx, "GET", "/batches/"+b.id, nil, &s); err != nil {
				return err
			}
			done += s.Counts["sent"]
			failed += s.Counts["failed"] + s.Counts["cancelled"]
		}
		total := 0
		for _, b := range batches {
			total += len(b.ids)
		}
		if failed > 0 {
			return fmt.Errorf("%d notifications ended failed or cancelled", failed)
		}
		if done == total {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("only %d of %d sent before the timeout", done, total)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

type channelStats struct {
	Received      int       `json:"received"`
	PeakPerSecond int       `json:"peak_per_second"`
	FirstAt       time.Time `json:"first_at"`
	LastAt        time.Time `json:"last_at"`
}

// throughput is the received rate between the first and the last request.
func (s channelStats) throughput() float64 {
	d := s.LastAt.Sub(s.FirstAt).Seconds()
	if s.Received < 2 || d <= 0 {
		return 0
	}
	return float64(s.Received-1) / d
}

func (c *client) providerStats(ctx context.Context, url string) (map[string]channelStats, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fake provider stats: %w", err)
	}
	defer res.Body.Close()
	var out struct{ Channels map[string]channelStats }
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("fake provider stats: %w", err)
	}
	return out.Channels, nil
}

// latencies reads every notification and returns sent - created_at per
// priority. Both times come from the database clock.
func (c *client) latencies(ctx context.Context, batches []batch) (map[string][]time.Duration, error) {
	type job struct {
		id, priority string
	}
	jobs := make(chan job)
	var (
		mu       sync.Mutex
		out      = map[string][]time.Duration{}
		firstErr error
		wg       sync.WaitGroup
	)
	for range 16 {
		wg.Go(func() {
			for j := range jobs {
				var n struct {
					Status    string    `json:"status"`
					CreatedAt time.Time `json:"created_at"`
					UpdatedAt time.Time `json:"updated_at"` // the sent write
				}
				err := c.do(ctx, "GET", "/notifications/"+j.id, nil, &n)
				mu.Lock()
				switch {
				case err != nil && firstErr == nil:
					firstErr = err
				case err == nil && n.Status == "sent":
					out[j.priority] = append(out[j.priority], n.UpdatedAt.Sub(n.CreatedAt))
				}
				mu.Unlock()
			}
		})
	}
	for _, b := range batches {
		for _, id := range b.ids {
			jobs <- job{id, b.priority}
		}
	}
	close(jobs)
	wg.Wait()
	return out, firstErr
}

// percentile returns the p-th percentile (nearest rank).
func percentile(ds []time.Duration, p int) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	rank := (p*len(s) + 99) / 100 // ceil(p/100 × n)
	return s[max(rank, 1)-1]
}

// splitEven splits n into k parts that differ by at most one, the larger
// ones first.
func splitEven(n, k int) []int {
	out := make([]int, k)
	for i := range out {
		out[i] = n / k
		if i < n%k {
			out[i]++
		}
	}
	return out
}

func splitLabel(split []int) string {
	parts := make([]string, len(split))
	for i, n := range split {
		parts[i] = fmt.Sprintf("%d %s", n, priorities[i])
	}
	return strings.Join(parts, ", ")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}
