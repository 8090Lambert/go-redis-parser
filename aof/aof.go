package aof

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"os"
)

type ParserAOF struct {
	handler *os.File
}

func NewAof(file string) protocol.Parser {
	handler, err := os.Open(file)
	if err != nil {
		panic(err.Error())
	}

	return &ParserAOF{handler: handler}
}

func (a *ParserAOF) Parse() {
	fmt.Println("aof")
}
