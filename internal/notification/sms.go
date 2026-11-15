package notification

import "unicode/utf16"

// SMSEncoding is the character set an SMS is sent in.
type SMSEncoding string

const (
	// EncodingGSM7 packs 7-bit characters: 160 per single SMS, 153 per part.
	EncodingGSM7 SMSEncoding = "gsm7"
	// EncodingUCS2 uses UTF-16 code units: 70 per single SMS, 67 per part.
	EncodingUCS2 SMSEncoding = "ucs2"
)

// SMS size limits (D-011). A multi-part SMS loses room in every part to the
// concatenation header, hence 153 and 67 instead of 160 and 70.
const (
	gsm7Single = 160
	gsm7Part   = 153
	ucs2Single = 70
	ucs2Part   = 67

	// MaxSMSSegments is the largest SMS the API accepts.
	MaxSMSSegments = 10
)

// SMSInfo describes how an SMS body will be sent.
type SMSInfo struct {
	Encoding SMSEncoding `json:"encoding"`
	Segments int         `json:"segments"`
	// Length is the size in the encoding's units: septets for GSM-7 (an
	// extension character counts 2), UTF-16 code units for UCS-2 (an emoji
	// counts 2).
	Length int `json:"-"`
}

// gsm7Basic is the GSM 03.38 default alphabet; each costs one septet.
// ESC (0x1B) is left out: it only introduces extension characters.
var gsm7Basic = runeSet("@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?" +
	"¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà")

// gsm7Extension is the GSM 03.38 extension table; each costs two septets
// (ESC + the character).
var gsm7Extension = runeSet("\f^{}\\[~]|€")

func runeSet(s string) map[rune]struct{} {
	m := make(map[rune]struct{}, len(s))
	for _, r := range s {
		m[r] = struct{}{}
	}
	return m
}

// ComputeSMSInfo picks the encoding for content and counts its segments
// (D-011): GSM-7 when every character is in the basic or extension table,
// UCS-2 otherwise.
//
// The segment count is approximate at part boundaries: real senders never
// split an extension character or a surrogate pair across two parts, which
// can occasionally cost one more part than counted here.
func ComputeSMSInfo(content string) SMSInfo {
	septets, gsm := 0, true
	for _, r := range content {
		if _, ok := gsm7Basic[r]; ok {
			septets++
			continue
		}
		if _, ok := gsm7Extension[r]; ok {
			septets += 2
			continue
		}
		gsm = false
		break
	}
	if gsm {
		return SMSInfo{Encoding: EncodingGSM7, Length: septets, Segments: segments(septets, gsm7Single, gsm7Part)}
	}
	units := len(utf16.Encode([]rune(content)))
	return SMSInfo{Encoding: EncodingUCS2, Length: units, Segments: segments(units, ucs2Single, ucs2Part)}
}

func segments(length, single, part int) int {
	if length <= single {
		return 1
	}
	return (length + part - 1) / part
}
