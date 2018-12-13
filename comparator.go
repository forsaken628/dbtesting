package dbtesting

import (
	"bytes"
	"database/sql"
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

func RawBytesEqual(expect, actual interface{}) (s string, b bool) {
	if bytes.Compare(expect.(sql.RawBytes), actual.(sql.RawBytes)) != 0 {
		return fmt.Sprintf("expect: %v, actual: %v", expect, actual), false
	}
	return "", true
}

func TimeEqual(expect, actual interface{}) (s string, b bool) {
	if !expect.(time.Time).Equal(actual.(time.Time)) {
		return fmt.Sprintf("expect: %v, actual: %v", expect, actual), false
	}
	return "", true
}
