package worker

import (
	"errors"
	"fmt"

	"github.com/asrasari/notification-service/internal/retry"
)

// ErrNothingClaimed is reported to the breaker when Allow let a request
// through but there was nothing due to send (or the claim itself failed).
// It must never count as a success or a failure (D-007, D-010): done(nil)
// would close a half-open breaker without any real probe.
var ErrNothingClaimed = errors.New("nothing claimed")

// SendError is the breaker's view of a send that did not succeed.
type SendError struct {
	Class retry.Class
	Err   error
}

func (e *SendError) Error() string {
	return fmt.Sprintf("send %s: %v", e.Class, e.Err)
}

func (e *SendError) Unwrap() error { return e.Err }

// IsExcluded is the breaker's IsExcluded rule (D-010, R-M1 Q-003): results
// that say nothing about the provider's health. Permanent errors are about
// one message, a 429 is the provider asking us to slow down (handled by the
// channel pause), and an empty claim sent nothing. Config errors (3xx, 401,
// 403, 404) still count as failures.
func IsExcluded(err error) bool {
	if errors.Is(err, ErrNothingClaimed) {
		return true
	}
	var se *SendError
	if errors.As(err, &se) {
		return se.Class == retry.ClassPermanent || se.Class == retry.ClassRateLimited
	}
	return false
}
