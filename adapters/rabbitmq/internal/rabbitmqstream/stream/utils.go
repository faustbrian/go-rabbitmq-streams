package stream

import "strings"

type responseError struct {
	Err       error
	isTimeout bool
}

func newResponseError(err error, timeout bool) responseError {
	return responseError{
		Err:       err,
		isTimeout: timeout,
	}
}

func uShortExtractResponseCode(code uint16) uint16 {
	return code & 0b0111_1111_1111_1111
}

// func UIntExtractResponseCode(code int32) int32 {
//	return code & 0b0111_1111_1111_1111
//}

func uShortEncodeResponseCode(code uint16) uint16 {
	return code | 0b1000_0000_0000_0000
}

func containsOnlySpaces(input string) bool {
	return len(input) > 0 && len(strings.TrimSpace(input)) == 0
}
