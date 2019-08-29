package boot

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/command"
	"github.com/8090Lambert/go-redis-parser/constants"
	"github.com/8090Lambert/go-redis-parser/parse"
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

	factory := parse.NewParserFactory(mod)
	if factory == nil {
		return
	}

	parser := factory(file)
	err := parser.Analyze()
	if err != nil {
		fmt.Println("Analyze failed: " + err.Error())
		return
	}
}
