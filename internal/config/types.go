package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ByteSize is a size in bytes, written as an integer with an optional
// binary unit: 512, 256KiB, 4MiB, 1GiB.
type ByteSize uint64

const (
	KiB ByteSize = 1 << (10 * (iota + 1))
	MiB
	GiB
)

func (b *ByteSize) UnmarshalText(text []byte) error {
	s := string(text)
	unit := ByteSize(1)
	// "B" last: it is also the end of the others
	for _, u := range []struct {
		suffix string
		size   ByteSize
	}{{"KiB", KiB}, {"MiB", MiB}, {"GiB", GiB}, {"B", 1}} {
		if digits, ok := strings.CutSuffix(s, u.suffix); ok {
			s, unit = digits, u.size
			break
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n > uint64(^ByteSize(0)/unit) {
		return fmt.Errorf("size %q: an integer with an optional B, KiB, MiB or GiB", string(text))
	}
	*b = ByteSize(n) * unit
	return nil
}

// TakeoverDelay is "auto" (the default) or a duration (§8.4).
type TakeoverDelay struct {
	fixed *time.Duration
}

// Fixed returns the configured delay, unless it is auto.
func (t TakeoverDelay) Fixed() (time.Duration, bool) {
	if t.fixed == nil {
		return 0, false
	}
	return *t.fixed, true
}

func (t *TakeoverDelay) UnmarshalText(text []byte) error {
	if string(text) == "auto" {
		t.fixed = nil
		return nil
	}
	d, err := time.ParseDuration(string(text))
	if err != nil || d < 0 {
		return fmt.Errorf("takeover_delay %q: auto or a duration", string(text))
	}
	t.fixed = &d
	return nil
}
