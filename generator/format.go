package generator

import "runtime"

var (
	CRLF string
)

func init() {
	if runtime.GOOS == `windows` {
		CRLF = "\r\n"
	} else {
		CRLF = "\n"
	}
}

type Json struct {
}

type Csv struct {
}
