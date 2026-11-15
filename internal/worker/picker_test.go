package worker

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/asrasari/notification-service/internal/notification"
)

func letters(ps []notification.Priority) string {
	var b strings.Builder
	for _, p := range ps {
		b.WriteByte(map[notification.Priority]byte{notification.PriorityHigh: 'H', notification.PriorityNormal: 'N', notification.PriorityLow: 'L'}[p])
	}
	return b.String()
}

func picks(p *Picker, n int) []notification.Priority {
	out := make([]notification.Priority, n)
	for i := range out {
		out[i] = p.Next()
	}
	return out
}

func TestPickerSmoothSequence(t *testing.T) {
	// Interleaved, never bunched. (D-006 shows "HHNHHLHNHH" as the style;
	// nginx's tie rule, first maximum wins, gives this exact order.)
	if got := letters(picks(NewPicker(70, 20, 10), 10)); got != "HHNHHLHHNH" {
		t.Errorf("sequence = %s, want HHNHHLHHNH", got)
	}
	// Small weights give the same pattern as their multiples, and it repeats.
	if got := letters(picks(NewPicker(7, 2, 1), 20)); got != "HHNHHLHHNHHHNHHLHHNH" {
		t.Errorf("sequence = %s", got)
	}
}

func TestPickerExactCounts(t *testing.T) {
	tests := []struct {
		weights [3]int
		n       int
		want    map[notification.Priority]int
	}{
		{[3]int{70, 20, 10}, 10, map[notification.Priority]int{1: 7, 2: 2, 3: 1}},
		{[3]int{70, 20, 10}, 100, map[notification.Priority]int{1: 70, 2: 20, 3: 10}},
		{[3]int{1, 1, 1}, 9, map[notification.Priority]int{1: 3, 2: 3, 3: 3}},
		{[3]int{5, 3, 1}, 9, map[notification.Priority]int{1: 5, 2: 3, 3: 1}},
	}
	for _, tt := range tests {
		p := NewPicker(tt.weights[0], tt.weights[1], tt.weights[2])
		// Every window of sum-of-weights picks, not only the first, matches.
		for round := range 3 {
			got := map[notification.Priority]int{}
			for _, pr := range picks(p, tt.n) {
				got[pr]++
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("weights %v, round %d (-want +got):\n%s", tt.weights, round, diff)
			}
		}
	}
}

func TestPickerOrderFallsBackStrictly(t *testing.T) {
	p := NewPicker(70, 20, 10)
	want := []string{"HNL", "HNL", "NHL", "HNL", "HNL", "LHN"}
	for i, w := range want {
		if got := letters(p.Order()); got != w {
			t.Errorf("order %d = %s, want %s", i, got, w)
		}
	}
}
