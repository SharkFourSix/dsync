package assert

import (
	"reflect"
	"testing"
)

func Equal[V comparable](t *testing.T, actual V, expected any, message ...string) {
	t.Helper()
	if actual != expected {
		var msg string
		if len(message) > 0 {
			msg = message[0]
		}
		t.Errorf("expected %v, got %v. %s", expected, actual, msg)
	}
}

func EqualSlice[V comparable](t *testing.T, actual []V, expected any, message ...string) {
	t.Helper()
	if !reflect.DeepEqual(actual, expected) {
		var msg string
		if len(message) > 0 {
			msg = message[0]
		}
		t.Errorf("expected %v, got %v. %s", expected, actual, msg)
	}
}
