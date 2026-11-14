package notification

import (
	"crypto/sha256"
	"encoding/binary"
	"time"
)

// HashInput is the set of fields covered by the request hash (D-009). The
// caller fills it from the parsed, validated request with defaults already
// applied, never from the raw body, so formatting differences in the JSON
// (key order, whitespace, time zone offsets) do not change the hash.
type HashInput struct {
	Recipient   string
	Channel     Channel
	Content     string
	Priority    Priority
	ScheduledAt *time.Time // nil when the notification is not scheduled
}

// hashVersion is mixed into every hash so the encoding can change later
// without old and new hashes ever colliding.
const hashVersion = "notification-request-v1"

// RequestHash returns the SHA-256 over the fields in a fixed order. Each field
// is length-prefixed, so values cannot run into each other ("ab"+"c" and
// "a"+"bc" hash differently).
//
// Normalization done here:
//   - priority is hashed as its number;
//   - scheduled_at is converted to UTC and truncated to microseconds (what
//     Postgres timestamptz stores), so the same instant written with different
//     offsets or extra nanosecond digits hashes the same.
//
// Recipient and content are hashed exactly as given; any trimming or case
// folding belongs to validation (D-011) and must happen before this call.
func RequestHash(in HashInput) []byte {
	h := sha256.New()
	writeField := func(b []byte) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(b)))
		h.Write(n[:])
		h.Write(b)
	}

	writeField([]byte(hashVersion))
	writeField([]byte(in.Recipient))
	writeField([]byte(in.Channel))
	writeField([]byte(in.Content))
	writeField(binary.BigEndian.AppendUint16(nil, uint16(in.Priority)))
	if in.ScheduledAt == nil {
		writeField(nil)
	} else {
		writeField([]byte(in.ScheduledAt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)))
	}
	return h.Sum(nil)
}
