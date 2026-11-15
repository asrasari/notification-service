// Package worker runs one dispatcher and a pool of senders per channel
// (D-004, D-007). The dispatcher enforces every rule in one place: a free
// sender, no 429 pause, the circuit breaker, a rate-limit token, a priority
// pick, then a claim of one row. Senders call the provider and write the
// outcome.
//
// It declares the interfaces it needs (Store, Sender, Breaker, Limiter) and
// depends on no implementation of them (D-019).
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
	"github.com/asrasari/notification-service/internal/provider"
	"github.com/asrasari/notification-service/internal/retry"
)

// Store is the queue: postgres.Queue implements it.
type Store interface {
	Claim(ctx context.Context, channel notification.Channel, priorities []notification.Priority) (postgres.ClaimResult, error)
	Complete(ctx context.Context, c postgres.Completion) error
}

// Sender sends one message: provider.Client implements it.
type Sender interface {
	Send(ctx context.Context, m provider.Message) provider.Response
}

// Breaker is one channel's circuit breaker: breaker.Breaker implements it.
// Allow returns an error when the request is refused; otherwise done must be
// called exactly once with the result.
type Breaker interface {
	Allow() (done func(error), err error)
	State() string
}

// Limiter hands out send tokens: *rate.Limiter implements it.
type Limiter interface {
	Wait(ctx context.Context) error
}

// completeTimeout bounds the write of a result. The write runs even after
// the send context is cancelled: a 2xx must be recorded if at all possible
// (D-009 rule 4), or the message is sent again after the lease expires.
const completeTimeout = 5 * time.Second

// Config is the worker's tuning, from config.Queue/Retry/Breaker.
type Config struct {
	PollInterval       time.Duration // sleep when nothing is due or the breaker refuses
	Concurrency        int           // senders per channel
	Weights            [3]int        // high, normal, low (D-006)
	Backoff            retry.Backoff
	RetryAfterCap      time.Duration // RETRY_AFTER_CAP
	BreakerOpenTimeout time.Duration // requeue delay after a config error (R-M1 Q-004)
	// OnAttempt, if set, is called after every send's result is written (or
	// discarded): the hook for the attempt, result, send-duration and
	// delivery-latency metrics. The worker itself imports no metrics library.
	OnAttempt func(AttemptEvent)
	// OnClaim, if set, is called after every claim transaction: the hook for
	// the claim-duration metric and for attempts recorded inside the claim.
	OnClaim func(ClaimEvent)
}

// AttemptEvent describes one finished send.
type AttemptEvent struct {
	Channel      notification.Channel
	Priority     notification.Priority
	Outcome      notification.Outcome
	Status       notification.Status // the status written: sent, pending or failed
	SendDuration time.Duration
	// DueAt is coalesce(scheduled_at, created_at): delivery latency is
	// measured from here, so scheduled sends don't look slow (D-015).
	DueAt time.Time
	Stale bool // the lease was lost, so nothing was written
}

// ClaimEvent describes one claim transaction.
type ClaimEvent struct {
	Channel  notification.Channel
	Duration time.Duration
	Err      bool // the claim failed
	// LostAttempts are the priorities of expired leases found by the claim:
	// each recorded an unknown attempt (the reclaimed row and every
	// exhausted one).
	LostAttempts []notification.Priority
	// Exhausted are the priorities of rows failed inside the claim because
	// their last attempt's lease expired (R-M3).
	Exhausted []notification.Priority
}

// ChannelDeps are the per-channel parts: each channel has its own breaker
// and rate limiter.
type ChannelDeps struct {
	Channel notification.Channel
	Breaker Breaker
	Limiter Limiter
}

// Worker runs the dispatchers of all channels.
type Worker struct {
	channels []*channelLoop
}

// New returns a Worker for the given channels.
func New(cfg Config, store Store, sender Sender, logger *slog.Logger, deps []ChannelDeps) *Worker {
	w := &Worker{}
	for _, d := range deps {
		w.channels = append(w.channels, &channelLoop{
			channel: d.Channel,
			breaker: d.Breaker,
			limiter: d.Limiter,
			store:   store,
			sender:  sender,
			logger:  logger.With("channel", string(d.Channel)),
			cfg:     cfg,
			picker:  NewPicker(cfg.Weights[0], cfg.Weights[1], cfg.Weights[2]),
			slots:   make(chan struct{}, cfg.Concurrency),
		})
	}
	return w
}

// Run runs every channel until ctx is cancelled and all in-flight sends have
// finished. ctx stops claiming; sendCtx is given to in-flight sends and
// should only be cancelled when the shutdown timeout expires (D-007).
func (w *Worker) Run(ctx, sendCtx context.Context) {
	var wg sync.WaitGroup
	for _, c := range w.channels {
		wg.Go(func() { c.run(ctx, sendCtx) })
	}
	wg.Wait()
}

// ChannelStatus is one channel's state for /health and the heartbeat.
type ChannelStatus struct {
	Breaker string `json:"breaker"`
	Paused  bool   `json:"paused"`
}

// Status returns the state of every channel, keyed by channel name.
// Busy returns the number of in-flight sends per channel, keyed by channel
// name (the senders_busy gauge).
func (w *Worker) Busy() map[string]int {
	out := make(map[string]int, len(w.channels))
	for _, c := range w.channels {
		out[string(c.channel)] = int(c.busy.Load())
	}
	return out
}

func (w *Worker) Status() map[string]ChannelStatus {
	out := make(map[string]ChannelStatus, len(w.channels))
	for _, c := range w.channels {
		out[string(c.channel)] = ChannelStatus{Breaker: c.breaker.State(), Paused: c.pausedFor() > 0}
	}
	return out
}

type channelLoop struct {
	channel notification.Channel
	breaker Breaker
	limiter Limiter
	store   Store
	sender  Sender
	logger  *slog.Logger
	cfg     Config
	picker  *Picker // used by the dispatcher goroutine only

	claimErrors int // consecutive failed claims; dispatcher goroutine only

	slots   chan struct{} // one entry per busy sender (or the dispatcher holding a slot)
	busy    atomic.Int64  // sends in flight
	senders sync.WaitGroup

	mu          sync.Mutex
	pausedUntil time.Time // set by a 429 (D-010)
}

// run is the dispatcher, in the D-007 order. It returns when ctx is
// cancelled and every sender it started has finished.
func (c *channelLoop) run(ctx, sendCtx context.Context) {
	defer c.senders.Wait()
	for {
		// 1. A free sender slot.
		select {
		case c.slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		if !c.dispatchOne(ctx, sendCtx) {
			<-c.slots
			if ctx.Err() != nil {
				return
			}
		}
	}
}

// dispatchOne runs steps 2–7 holding a slot. It reports whether a sender was
// started (which then owns the slot).
func (c *channelLoop) dispatchOne(ctx, sendCtx context.Context) bool {
	// 2. Not paused by a 429.
	if d := c.pausedFor(); d > 0 {
		sleep(ctx, d)
		return false
	}

	// 3. The breaker lets a request through.
	done, err := c.breaker.Allow()
	if err != nil {
		sleep(ctx, c.cfg.PollInterval)
		return false
	}

	// 4. A rate-limit token, before the claim (D-009 rule 1).
	if err := c.limiter.Wait(ctx); err != nil {
		done(ErrNothingClaimed)
		return false
	}

	// 5–6. Pick a priority and claim one row.
	claimStart := time.Now()
	res, err := c.store.Claim(ctx, c.channel, c.picker.Order())
	c.reportClaim(time.Since(claimStart), res, err)
	for _, n := range res.Exhausted {
		c.logger.WarnContext(withCorrelation(ctx, n), "notification failed: its last attempt's lease expired",
			"notification_id", n.ID, "attempt", n.Attempts, "outcome", notification.OutcomeUnknown)
	}
	if err != nil {
		done(ErrNothingClaimed)
		if ctx.Err() == nil {
			c.claimErrors++
			wait := c.claimErrorBackoff()
			c.logger.ErrorContext(ctx, "claim failed", "error", err,
				"consecutive_errors", c.claimErrors, "retry_in", wait.String())
			sleep(ctx, wait)
		}
		return false
	}
	c.claimErrors = 0 // any successful claim, with or without a row
	if res.Claimed == nil {
		done(ErrNothingClaimed)
		sleep(ctx, c.cfg.PollInterval)
		return false
	}

	// 7. Hand the row to a sender. It runs on sendCtx, so shutdown doesn't
	// abort a send already under way.
	n := *res.Claimed
	if res.Reclaimed {
		c.logger.WarnContext(withCorrelation(ctx, n), "reclaimed an expired lease; previous attempt recorded as unknown",
			"notification_id", n.ID, "attempt", n.Attempts)
	}
	c.busy.Add(1)
	c.senders.Go(func() {
		defer func() { c.busy.Add(-1); <-c.slots }()
		c.send(withCorrelation(sendCtx, n), n, done)
	})
	return true
}

// send calls the provider, writes the result and reports to the breaker.
func (c *channelLoop) send(ctx context.Context, n notification.Notification, done func(error)) {
	started := time.Now()
	resp := c.sender.Send(ctx, provider.Message{
		IdempotencyKey: n.ID.String(),
		To:             n.Recipient,
		Channel:        string(n.Channel),
		Content:        n.Content,
	})
	class := retry.Classify(resp.Result)
	if class == retry.ClassRateLimited {
		// Pause first, so the dispatcher stops as soon as possible.
		c.pause(retry.RateLimitPause(resp.Result, c.cfg.RetryAfterCap))
	}

	comp := c.completion(n, class, resp, started)
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), completeTimeout)
	err := c.store.Complete(writeCtx, comp)
	cancel()

	// The breaker hears about the provider, not about our database.
	if class == retry.ClassSuccess {
		done(nil)
	} else {
		done(&SendError{Class: class, Err: resp.Err})
	}

	attrs := []any{
		"notification_id", n.ID,
		"priority", n.Priority.String(),
		"attempt", n.Attempts,
		"outcome", comp.Attempt.Outcome,
		"status", comp.Status,
		"duration_ms", resp.Duration.Milliseconds(),
	}
	if comp.Attempt.HTTPStatus != nil {
		attrs = append(attrs, "http_status", *comp.Attempt.HTTPStatus)
	}
	if comp.Status == notification.StatusPending {
		attrs = append(attrs, "retry_in", comp.Delay.String())
	}
	if resp.Err != nil {
		attrs = append(attrs, "error", resp.Err.Error())
	}
	stale := errors.Is(err, postgres.ErrStaleLease)
	switch {
	case stale:
		c.logger.WarnContext(ctx, "delivery attempt result discarded: the lease was lost", attrs...)
	case err != nil:
		c.logger.ErrorContext(ctx, "delivery attempt result could not be written", append(attrs, "write_error", err.Error())...)
	default:
		c.logger.InfoContext(ctx, "delivery attempt", attrs...)
	}
	if resp.MessageIDErr != nil {
		c.logger.WarnContext(ctx, "provider accepted the message but its messageId was not usable",
			"notification_id", n.ID, "error", resp.MessageIDErr.Error())
	}

	if c.cfg.OnAttempt != nil {
		c.cfg.OnAttempt(AttemptEvent{
			Channel: n.Channel, Priority: n.Priority, Outcome: comp.Attempt.Outcome,
			Status: comp.Status, SendDuration: resp.Duration, DueAt: dueAt(n), Stale: stale,
		})
	}
}

// reportClaim calls OnClaim.
func (c *channelLoop) reportClaim(d time.Duration, res postgres.ClaimResult, err error) {
	if c.cfg.OnClaim == nil {
		return
	}
	ev := ClaimEvent{Channel: c.channel, Duration: d, Err: err != nil}
	for _, n := range res.Exhausted {
		ev.Exhausted = append(ev.Exhausted, n.Priority)
		ev.LostAttempts = append(ev.LostAttempts, n.Priority)
	}
	if res.Claimed != nil && res.Reclaimed {
		ev.LostAttempts = append(ev.LostAttempts, res.Claimed.Priority)
	}
	c.cfg.OnClaim(ev)
}

func dueAt(n notification.Notification) time.Time {
	if n.ScheduledAt != nil {
		return *n.ScheduledAt
	}
	return n.CreatedAt
}

// completion turns a send's result into the row update (D-010).
func (c *channelLoop) completion(n notification.Notification, class retry.Class, resp provider.Response, started time.Time) postgres.Completion {
	comp := postgres.Completion{
		ID:         n.ID,
		LeaseToken: *n.LeaseToken,
		Attempt: notification.Attempt{
			AttemptNo: n.Attempts,
			StartedAt: started,
			Outcome:   notification.Outcome(class.Outcome()),
		},
	}
	ms := int(resp.Duration.Milliseconds())
	comp.Attempt.DurationMS = &ms
	if resp.Result.Transport == retry.TransportResponded {
		code := resp.Result.StatusCode
		comp.Attempt.HTTPStatus = &code
	}
	if resp.Err != nil {
		msg := resp.Err.Error()
		comp.Attempt.Error = &msg
	}

	switch retry.Decide(class, n.Attempts, n.MaxAttempts) {
	case retry.ActionMarkSent:
		comp.Status = notification.StatusSent
		if resp.MessageID != "" {
			id := resp.MessageID
			comp.ProviderMessageID = &id
		}
	case retry.ActionRetry:
		comp.Status = notification.StatusPending
		comp.Delay = c.cfg.Backoff.Wait(n.Attempts)
	case retry.ActionRequeue:
		comp.Status = notification.StatusPending
		comp.RefundAttempt = true
		if class == retry.ClassRateLimited {
			comp.Delay = retry.RateLimitPause(resp.Result, c.cfg.RetryAfterCap)
		} else {
			comp.Delay = c.cfg.BreakerOpenTimeout
		}
	default: // retry.ActionFail
		comp.Status = notification.StatusFailed
	}
	return comp
}

// maxClaimErrorBackoff caps the sleep after consecutive claim errors.
const maxClaimErrorBackoff = 5 * time.Second

// claimErrorBackoff doubles the sleep from POLL_INTERVAL with each
// consecutive claim error, up to 5 s, so a database outage doesn't produce
// an error line every poll (R-M3).
func (c *channelLoop) claimErrorBackoff() time.Duration {
	d := c.cfg.PollInterval
	for i := 1; i < c.claimErrors && d < maxClaimErrorBackoff; i++ {
		d *= 2
	}
	return min(d, maxClaimErrorBackoff)
}

func (c *channelLoop) pause(d time.Duration) {
	until := time.Now().Add(d)
	c.mu.Lock()
	if until.After(c.pausedUntil) {
		c.pausedUntil = until
	}
	c.mu.Unlock()
	c.logger.Warn("channel paused by provider rate limit (429)", "pause", d.String())
}

// pausedFor returns how long the channel stays paused, or 0.
func (c *channelLoop) pausedFor() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return max(time.Until(c.pausedUntil), 0)
}

// withCorrelation puts the row's correlation ID into the context, so every
// log line about it can be found with the API request's ID (D-015).
func withCorrelation(ctx context.Context, n notification.Notification) context.Context {
	if n.CorrelationID != nil {
		return logging.WithCorrelationID(ctx, *n.CorrelationID)
	}
	return ctx
}

// sleep waits for d or until ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
