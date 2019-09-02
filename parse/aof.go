package parse

import (
	"fmt"
	"os"
)

type AOFParser struct {
	handler *os.File
}

func AofNew(file string) Parser {
	handler, err := os.Open(file)
	if err != nil {
		panic(err.Error())
	}

	return &AOFParser{handler: handler}
}

func (a *AOFParser) Analyze() error {
	fmt.Println("aof")
	return nil
}

func (a *AOFParser) LayoutCheck() (bool, error) {
	return false, nil
}
