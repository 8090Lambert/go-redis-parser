package parse

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/generator"
	"io"
	"os"
	"strconv"
)

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

	RDB_OPCODE_IDLE          = 248 /* LRU idle time. */
	RDB_OPCODE_AUX           = 250 /* RDB aux field. */
	RDB_OPCODE_RESIZEDB      = 251 /* Hash table resize hint. */
	RDB_OPCODE_EXPIRETIME_MS = 252 /* Expire time in milliseconds. */
	RDB_OPCODE_EXPIRETIME    = 253 /* Old expire time in seconds. */
	RDB_OPCODE_SELECTDB      = 254 /* DB number of the following keys. */
	RDB_OPCODE_EOF           = 255

	RDB_6BIT   = 0
	RDB_14BIT  = 1
	RDB_32BIT  = 0x80
	RDB_64BIT  = 0x81
	RDB_ENCVAL = 3
)

const (
	REDIS           = "REDIS"
	RDB_VERSION_MIN = 1
	RDB_VERSION_MAX = 9
)

var (
	buff = make([]byte, 8)
)

type RDBParser struct {
	handler *bufio.Reader
	output  generator.Putter
}

func RdbNew(file string) Parser {
	handler, err := os.Open(file)
	if err != nil {
		panic(err.Error())
	}

	return &RDBParser{handler: bufio.NewReader(handler), output: generator.Putter{}}
}

func (r *RDBParser) Analyze() error {
	err := r.skipHeader()
	if err != nil {
		return err
	}

	var lru_idle, lfu_freq, expire uint64 = -1, -1, -1
	for {
		// Begin analyze
		t, err := r.handler.ReadByte()
		if t == RDB_OPCODE_IDLE {
			qword, _, err := r.loadLen()
			if err != nil {
				return err
			}
			lru_idle = qword
			continue
		} else if t == RDB_OPCODE_AUX {

		} else if t == RDB_OPCODE_RESIZEDB {

		} else if t == RDB_OPCODE_EXPIRETIME_MS {

		} else if t == RDB_OPCODE_EXPIRETIME {

		} else if t == RDB_OPCODE_SELECTDB {

		} else if t == RDB_OPCODE_EOF {

		} else {

		}
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

func (r *RDBParser) loadLen() (length uint64, isEncode bool, err error) {
	buf := make([]byte, 2)
	_, err = r.handler.Read(buf)
	if err != nil {
		return
	}
	typeLen := (buf[0] & 0xC0) >> 6
	if typeLen == RDB_ENCVAL || typeLen == RDB_6BIT {
		if typeLen == RDB_ENCVAL {
			isEncode = true
		}
		length = uint64(buf[0]) & 0x3f
	} else if typeLen == RDB_14BIT {
		length = (uint64(buf[0])&0x3f)<<8 | uint64(buf[1])
	} else if buf[0] == RDB_32BIT {
		_, err = io.ReadFull(r.handler, buff[0:4])
		if err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint32(buff))
	} else if buf[0] == RDB_64BIT {
		_, err = io.ReadFull(r.handler, buff)
		if err != nil {
			return
		}
		length = binary.BigEndian.Uint64(buff)
	} else {
		err = errors.New(fmt.Sprintf("unknown length encoding %d in loadLen()", typeLen))
	}

	return
}

func (r *RDBParser) loadString() ([]byte, error) {
	length, needEncode, err := r.loadLen()
	if err != nil {
		return nil, err
	}
}
