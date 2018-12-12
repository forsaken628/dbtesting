package dbtesting

import (
	"fmt"
	"time"
)

func TimeShouldAfter(expect, actual interface{}) (s string, b bool) {
	at, ok := actual.(time.Time)
	if !ok {
		return fmt.Sprintf("expect time.Time, get %T", at), false
	}

	if !at.After(expect.(time.Time)) {
		return "actual time should after expect time", false
	}

	return "", true
}
