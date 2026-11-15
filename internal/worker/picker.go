package worker

import "github.com/asrasari/notification-service/internal/notification"

// strictOrder is the fallback order when the picked priority has nothing due
// (D-006: work-conserving, high → normal → low).
var strictOrder = [3]notification.Priority{notification.PriorityHigh, notification.PriorityNormal, notification.PriorityLow}

// Picker chooses the priority of each claim with smooth weighted round-robin
// (the nginx algorithm): every pick adds each weight to its running score,
// takes the highest score and subtracts the total from it. Over any
// sum-of-weights picks the counts match the weights exactly, and the picks
// are spread out instead of bunched (D-006).
//
// It is pure (no time, no DB) and not safe for concurrent use; each channel's
// dispatcher owns one.
type Picker struct {
	weights [3]int
	current [3]int
	total   int
}

// NewPicker returns a picker for the high, normal and low weights, which
// config validation guarantees are positive.
func NewPicker(high, normal, low int) *Picker {
	return &Picker{weights: [3]int{high, normal, low}, total: high + normal + low}
}

// Next returns the next priority to try first.
func (p *Picker) Next() notification.Priority {
	best := 0
	for i := range p.weights {
		p.current[i] += p.weights[i]
		if p.current[i] > p.current[best] {
			best = i
		}
	}
	p.current[best] -= p.total
	return strictOrder[best]
}

// Order returns the probe order for one claim: the next pick first, then the
// others in strict order.
func (p *Picker) Order() []notification.Priority {
	first := p.Next()
	order := make([]notification.Priority, 0, len(strictOrder))
	order = append(order, first)
	for _, pr := range strictOrder {
		if pr != first {
			order = append(order, pr)
		}
	}
	return order
}
