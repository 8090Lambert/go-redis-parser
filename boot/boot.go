package boot

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/command"
	"github.com/8090Lambert/go-redis-parser/constants"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"github.com/8090Lambert/go-redis-parser/rdb"
	"github.com/fatih/color"
	"os"
)

func Boot() {
	mod, file := command.Watch()
	if mod == constants.UNKNOWN || file == "" {
		return
	}
	if _, err := os.Stat(file); err != nil && os.IsNotExist(err) {
		panic(file + " not exist !")
	}

	factory := NewParserFactory(mod)
	if factory == nil {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			if _, ok := err.(string); ok {
				fmt.Println(color.RedString(fmt.Sprintf("Parse failed: %s", err)))
			}
		}
	}()

	factory(file).Parse()
}

func NewParserFactory(mod int) protocol.Factory {
	if mod == constants.RDBMOD {
		return rdb.NewRDB
	} else {
		return nil
	}
}
