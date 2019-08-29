package parse

import (
	"github.com/8090Lambert/go-redis-parser/constants"
)

type Parser interface {
	Analyze() error
	LayoutCheck() bool
}

type Factory func(file string) Parser

func NewParserFactory(mod int) Factory {
	if mod == constants.RDBMOD {
		return RdbNew
	} else if mod == constants.AOFMOD {
		return AofNew
	} else {
		return nil
	}
}
