package aof

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/parse"
	"os"
)

type AOFParser struct {
	handler *os.File
}

func NewAof(file string) parse.Parser {
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
