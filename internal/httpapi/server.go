package httpapi

import (
	"log"
	"net/http"
	"time"
)

// Server timeouts (D-012). They are fixed rather than configurable: they
// protect the process from slow or stuck clients and have no tuning value.
const (
	// ReadHeaderTimeout bounds how long a client may take to send headers
	// (slowloris protection).
	ReadHeaderTimeout = 5 * time.Second
	// ReadTimeout bounds reading the whole request, a 10 MiB body included.
	ReadTimeout = 30 * time.Second
	// WriteTimeout bounds handling plus writing the response.
	WriteTimeout = 30 * time.Second
	// IdleTimeout closes keep-alive connections nobody uses.
	IdleTimeout = 120 * time.Second
)

// NewServer returns an http.Server for h with the timeouts above.
func NewServer(addr string, h http.Handler, errorLog *log.Logger) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: ReadHeaderTimeout,
		ReadTimeout:       ReadTimeout,
		WriteTimeout:      WriteTimeout,
		IdleTimeout:       IdleTimeout,
		ErrorLog:          errorLog,
	}
}
