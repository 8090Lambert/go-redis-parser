package aof

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"os"
)

type AOFParser struct {
	handler *os.File
}

func NewAof(file string) protocol.Parser {
	handler, err := os.Open(file)
	if err != nil {
		panic(err.Error())
	}

	return &AOFParser{handler: handler}
}

func (a *AOFParser) Parse() error {
	fmt.Println("aof")
	return nil
}
