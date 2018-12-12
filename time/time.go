package time

import "time"

var d time.Duration

func Reset(t time.Time) {
	d = t.Sub(time.Now())
}

func Now() time.Time {
	return time.Now().Add(d)
}
