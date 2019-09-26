package command

import (
	"flag"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/constants"
	"github.com/fatih/color"
	"os"
)

var (
	Output      string
	GenFileType string
)

var (
	rdbFile string
	aofFile string
)

func Start() {
	flag.StringVar(&Output, "o", "", "set the output directory for gen-file. (default: current directory. parser.(json|csv) will be created)\n")
	flag.StringVar(&GenFileType, "type", "csv", "set the gen-file's type, support type: json、csv. (default: csv)\n")
	flag.StringVar(&rdbFile, "rdb", "", "<rdb-file-name>. For example: ./dump.rdb\n")
	//flag.StringVar(&aofFile, "aof", "", "file.aof. For example: ./appendonly.aof\n")

	flag.Parse()
	flag.Usage = defaultUsage
}

func Watch() (mod int, file string) {
	Start()
	if GenFileType != "csv" && GenFileType != "json" {
		return
	}
	if rdbFile != "" {
		return constants.RDBMOD, rdbFile
	} else if aofFile != "" {
		return constants.AOFMOD, aofFile
	} else {
		flag.Usage()
		return constants.UNKNOWN, ""
	}
}

func defaultUsage() {
	fmt.Fprintf(
		os.Stderr, fmt.Sprintf(usageformat, logo, color.GreenString(app), color.YellowString(version), releaseTime),
	)
	flag.PrintDefaults()
}
