package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestPercentile(t *testing.T) {
	var ds []time.Duration
	for i := 1; i <= 100; i++ {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	for p, want := range map[int]time.Duration{50: 50 * time.Millisecond, 95: 95 * time.Millisecond, 100: 100 * time.Millisecond, 1: time.Millisecond} {
		if got := percentile(ds, p); got != want {
			t.Errorf("p%d = %v, want %v", p, got, want)
		}
	}
	if got := percentile([]time.Duration{3, 1, 2}, 50); got != 2 {
		t.Errorf("unsorted p50 = %v", got)
	}
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("empty = %v", got)
	}
}

func TestSplitEven(t *testing.T) {
	if diff := cmp.Diff([]int{334, 333, 333}, splitEven(1000, 3)); diff != "" {
		t.Error(diff)
	}
	if diff := cmp.Diff([]int{2, 2, 2}, splitEven(6, 3)); diff != "" {
		t.Error(diff)
	}
}

func TestThroughput(t *testing.T) {
	t0 := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	s := channelStats{Received: 101, FirstAt: t0, LastAt: t0.Add(time.Second)}
	if got := s.throughput(); got != 100 {
		t.Errorf("throughput = %v, want 100", got)
	}
	if got := (channelStats{Received: 1}).throughput(); got != 0 {
		t.Errorf("single = %v", got)
	}
}
