package worker

import "golang.org/x/time/rate"

// PacingFactor is the share of the provider's limit that the token bucket
// actually runs at (R-M5, amends D-007).
//
// The limiter controls when sends start. Each send then spends a few
// milliseconds in the claim transaction and on the network, and that time
// varies, so arrivals at the provider are shifted by a little jitter. At
// exactly 100/s two sends 100 tokens apart can arrive less than 1 s apart,
// and the provider sees 101 in one 1-second window (measured by `make
// load`). Pacing at 99 % spaces tokens ~10.1 ms apart, so 101 sends span at
// least 1,010 ms and up to ~10 ms of jitter is absorbed. A real deployment
// would size this headroom from its measured jitter (or set
// RATE_LIMIT_PER_CHANNEL lower). A 429 that still happens costs no attempt
// (D-010).
const PacingFactor = 0.99

// NewLimiter returns one channel's token bucket for a provider limit of
// providerLimit messages per second (RATE_LIMIT_PER_CHANNEL): paced at
// PacingFactor of it, with burst 1 so sends are evenly spaced (D-007).
func NewLimiter(providerLimit int) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(float64(providerLimit)*PacingFactor), 1)
}
