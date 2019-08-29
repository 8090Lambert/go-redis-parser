package parse

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

type RDBParser struct {
	handler *bufio.Reader
}

const (
	RDB_TYPE_STRING = iota
	RDB_TYPE_LIST
	RDB_TYPE_SET
	RDB_TYPE_ZSET
	RDB_TYPE_HASH
	RDB_TYPE_ZSET_2 /* ZSET version 2 with doubles stored in binary. */
	RDB_TYPE_MODULE
	RDB_TYPE_MODULE_2
	_
	RDB_TYPE_HASH_ZIPMAP
	RDB_TYPE_LIST_ZIPLIST
	RDB_TYPE_SET_INTSET
	RDB_TYPE_ZSET_ZIPLIST
	RDB_TYPE_HASH_ZIPLIST
	RDB_TYPE_LIST_QUICKLIST
	RDB_TYPE_STREAM_LISTPACKS

	// Aux field.
	RDB_OPCODE_MODULE_AUX    = 247 /* Module auxiliary data. */
	RDB_OPCODE_IDLE          = 248 /* LRU idle time. */
	RDB_OPCODE_FREQ          = 249 /* LFU frequency. */
	RDB_OPCODE_AUX           = 250 /* RDB aux field. */
	RDB_OPCODE_RESIZEDB      = 251 /* Hash table resize hint. */
	RDB_OPCODE_EXPIRETIME_MS = 252 /* Expire time in milliseconds. */
	RDB_OPCODE_EXPIRETIME    = 253 /* Old expire time in seconds. */
	RDB_OPCODE_SELECTDB      = 254 /* DB number of the following keys. */
	RDB_OPCODE_EOF           = 255
)

const (
	REDIS           = "REDIS"
	RDB_VERSION_MIN = 1
	RDB_VERSION_MAX = 9
)

func RdbNew(file string) Parser {
	handler, err := os.Open(file)
	if err != nil {
		panic(err.Error())
	}

	return &RDBParser{handler: bufio.NewReader(handler)}
}

func (r *RDBParser) Analyze() error {
	err := r.skipHeader()
	if err != nil {
		return err
	}

	// Begin analyze
	auxfiled, err := r.handler.ReadByte()
	switch auxfiled {
	case RDB_OPCODE_MODULE_AUX:
		fmt.Println(1)
	case RDB_OPCODE_IDLE:
		fmt.Println(2)
	case RDB_OPCODE_FREQ:

	case RDB_OPCODE_AUX:
	case RDB_OPCODE_RESIZEDB:
	case RDB_OPCODE_EXPIRETIME_MS:
	case RDB_OPCODE_EXPIRETIME:
	case RDB_OPCODE_SELECTDB:
	case RDB_OPCODE_EOF:
	}
	if err != nil {
		return err
	}

	return nil
}

// 5 bytes "REDIS" and 4 bytes version.
func (r *RDBParser) skipHeader() error {
	header := make([]byte, 9)
	_, err := r.handler.Read(header)
	if err != nil {
		if err == io.EOF {
			return errors.New("RDB file is empty")
		}
		return errors.New("Read RDB file failed, error: " + err.Error())
	}

	// Check "REDIS" string and version.
	if version, err := strconv.Atoi(string(header[5:])); !bytes.Equal(header[0:5], []byte(REDIS)) || err != nil || (version < RDB_VERSION_MIN || version > RDB_VERSION_MAX) {
		return errors.New("RDB file version is wrong")
	}

	return nil
}

func (r *RDBParser) LayoutCheck() bool {
	return false
}
