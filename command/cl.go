package command

import (
	"flag"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/constants"
	"github.com/fatih/color"
	"os"
)

var (
	RdbFile     string
	AofFile     string
	Output      string
	GenFileType string
)

func Start() {
	flag.StringVar(&Output, "o", "", "set the output directory for gen-file. (default: current directory)\n")
	flag.StringVar(&GenFileType, "type", "", "set the gen-file's type, support type: json、csv. (default: csv)\n")
	flag.StringVar(&RdbFile, "rdb", "", "<rdb-file-name>. For example: ./dump.rdb\n")
	flag.StringVar(&AofFile, "aof", "", "file.aof. For example: ./appendonly.aof\n")

	flag.Parse()
	flag.Usage = defaultUsage
	flag.Usage()
}

func Watch() (mod int, file string) {
	Start()
	if RdbFile != "" {
		return constants.RDBMOD, RdbFile
	} else if AofFile != "" {
		return constants.AOFMOD, AofFile
	} else {
		return constants.UNKNOWN, ""
	}
}

func defaultUsage() {
	fmt.Fprintf(
		os.Stderr, fmt.Sprintf(usageformat, logo, color.GreenString(app), color.YellowString(version), releaseTime),
	)
	flag.PrintDefaults()
}
