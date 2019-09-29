package rdb

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"io"
	"strings"
	"sync"
)

type WriterRDB struct {
	Writer      io.Writer
	KeysCount   uint64
	KeysSize    uint64
	Gather      map[string][]uint64
	Biggest     map[string][]string
	flag        uint32
	jsonHandler *bufio.Writer
	csvHandler  *csv.Writer
	mu          sync.Mutex
}

const (
	beginning = 1
)

var (
	turns = []string{protocol.String, protocol.Hash, protocol.List, protocol.SortedSet, protocol.Set, protocol.Stream}
	units = map[string]string{protocol.String: "bytes", protocol.Hash: "fields", protocol.List: "items", protocol.SortedSet: "members", protocol.Set: "members", protocol.Stream: "entries"}
)

func NewRDBWriter(writer io.Writer, gather map[string][]uint64, biggest map[string][]string) *WriterRDB {
	w := &WriterRDB{
		Writer:    writer,
		KeysCount: 0,
		KeysSize:  0,
		Gather:    gather,
		Biggest:   biggest,
	}
	if suffix == ".json" {
		w.jsonHandler = bufio.NewWriter(writer)
	} else {
		w.csvHandler = csv.NewWriter(writer)
	}

	return w
}

func (w *WriterRDB) FlushGather() {
	println("# Scanning the rdb file to find biggest keys\n")
	println("-------- summary -------\n")
	println(fmt.Sprintf("Sampled %d keys in the keyspace!", w.KeysCount))
	println(fmt.Sprintf("Total key length in bytes is %d\n", w.KeysSize))

	// Biggest.
	for _, val := range turns {
		if len(w.Biggest[val]) > 0 {
			println(fmt.Sprintf("Biggest %6s found '%s' has %s %s", strings.ToLower(val), w.Biggest[val][0], w.Biggest[val][1], units[val]))
		}
	}
	println()

	// Gather
	for _, val := range turns {
		if len(w.Gather[val]) > 0 {
			println(fmt.Sprintf("%d %s with %d %s", w.Gather[val][0], strings.ToLower(val), w.Gather[val][1], units[val]))
		}
	}
}

func (w *WriterRDB) AdditionKV(entity protocol.TypeObject) {
	if suffix == ".json" {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.flag&beginning == 0 {
			w.jsonHandler.WriteString("{" + entity.String())
			w.flag ^= beginning
		} else {
			w.jsonHandler.WriteString("," + entity.String())
		}
	} else {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.flag&beginning == 0 {
			w.csvHandler.Write([]string{"DataType", "Key", "Value", "Size(bytes)"})
			w.flag ^= beginning
		}
		w.csvHandler.Write([]string{entity.Type(), entity.Key(), entity.Value(), ToString(entity.ConcreteSize())})
	}
}

func (w *WriterRDB) FlushFile() {
	if w.jsonHandler != nil {
		// Json End
		w.jsonHandler.WriteString("}")
		w.jsonHandler.Flush()
	}
	if w.csvHandler != nil {
		w.csvHandler.Flush()
	}
}
