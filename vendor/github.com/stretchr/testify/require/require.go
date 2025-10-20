package require

import (
	"fmt"
	"reflect"
)

type TestingT interface {
	Helper()
	Fatalf(format string, args ...interface{})
}

func NoError(t TestingT, err error, msgAndArgs ...interface{}) {
	t.Helper()
	if err != nil {
		fail(t, fmt.Sprintf("expected no error, got %v", err), msgAndArgs...)
	}
}

func True(t TestingT, value bool, msgAndArgs ...interface{}) {
	t.Helper()
	if !value {
		fail(t, "expected true but was false", msgAndArgs...)
	}
}

func False(t TestingT, value bool, msgAndArgs ...interface{}) {
	t.Helper()
	if value {
		fail(t, "expected false but was true", msgAndArgs...)
	}
}

func Equal(t TestingT, expected, actual interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		fail(t, fmt.Sprintf("expected %v, got %v", expected, actual), msgAndArgs...)
	}
}

func Nil(t TestingT, object interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if !isNil(object) {
		fail(t, "expected nil but was not", msgAndArgs...)
	}
}

func fail(t TestingT, base string, msgAndArgs ...interface{}) {
	t.Helper()
	if len(msgAndArgs) == 0 {
		t.Fatalf(base)
		return
	}
	t.Fatalf(base + ": " + fmt.Sprint(msgAndArgs...))
}

func isNil(obj interface{}) bool {
	if obj == nil {
		return true
	}
	v := reflect.ValueOf(obj)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
