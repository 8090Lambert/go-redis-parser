package parse

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/generator"
	"io"
	"math"
	"os"
	"strconv"
)

const (
	// Redis Object type
	RO_TYPE_STRING = iota
	RO_TYPE_LIST
	RO_TYPE_SET
	RO_TYPE_ZSET
	RO_TYPE_HASH
	RO_TYPE_ZSET_2 /* ZSET version 2 with doubles stored in binary. */
	RO_TYPE_MODULE
	RO_TYPE_MODULE_2
	_
	RO_TYPE_HASH_ZIPMAP
	RO_TYPE_LIST_ZIPLIST
	RO_TYPE_SET_INTSET
	RO_TYPE_ZSET_ZIPLIST
	RO_TYPE_HASH_ZIPLIST
	RO_TYPE_LIST_QUICKLIST
	RO_TYPE_STREAM_LISTPACKS

	RDB_OPCODE_IDLE          = 248 /* LRU idle time. */
	RDB_OPCODE_AUX           = 250 /* RDB aux field. */
	RDB_OPCODE_RESIZEDB      = 251 /* Hash table resize hint. */
	RDB_OPCODE_EXPIRETIME_MS = 252 /* Expire time in milliseconds. */
	RDB_OPCODE_EXPIRETIME    = 253 /* Old expire time in seconds. */
	RDB_OPCODE_SELECTDB      = 254 /* DB number of the following keys. */
	RDB_OPCODE_EOF           = 255

	RDB_ENCODE_INT8 = iota
	RDB_ENCODE_INT16
	RDB_ENCODE_INT32
	RDB_ENCODE_LZF

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
	buff   = make([]byte, 8)
	PosInf = math.Inf(1)
	NegInf = math.Inf(-1)
	Nan    = math.NaN()
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
	_, err := r.LayoutCheck()
	if err != nil {
		return err
	}

	var lru_idle, lfu_freq, expire uint64 = -1, -1, -1
	var hasSelectDb bool
	for {
		// Begin analyze
		t, err := r.handler.ReadByte()
		if err != nil {
			break
		}
		if t == RDB_OPCODE_IDLE {
			qword, _, err := r.loadLen()
			if err != nil {
				return err
			}
			lru_idle = qword
			continue
		} else if t == RDB_OPCODE_AUX {
			key, err := r.loadString()
			if err != nil {
				return errors.New("Parse Aux key failed: " + err.Error())
			}
			val, err := r.loadString()
			if err != nil {
				return errors.New("Parse Aux value failed: " + err.Error())
			}
			r.output.AuxField(key, val)
			continue
		} else if t == RDB_OPCODE_RESIZEDB {
			dbSize, _, err := r.loadLen()
			if err != nil {
				return errors.New("Parse ResizeDB size failed: " + err.Error())
			}
			expiresSize, _, err := r.loadLen()
			if err != nil {
				return errors.New("Parse ResizeDB size failed: " + err.Error())
			}
			r.output.ResizeDB(dbSize, expiresSize)
			continue
		} else if t == RDB_OPCODE_EXPIRETIME_MS {
			_, err := io.ReadFull(r.handler, buff)
			if err != nil {
				return errors.New("Parse ExpireTime_ms failed: " + err.Error())
			}
			expire = uint64(binary.LittleEndian.Uint64(buff))
			continue
		} else if t == RDB_OPCODE_EXPIRETIME {
			_, err := io.ReadFull(r.handler, buff)
			if err != nil {
				return errors.New("Parse ExpireTime failed: " + err.Error())
			}
			expire = uint64(binary.LittleEndian.Uint64(buff)) * 1000
			continue
		} else if t == RDB_OPCODE_SELECTDB {
			if hasSelectDb == true {
				continue
			}
			dbid, _, err := r.loadLen()
			if err != nil {
				return err
			}
			r.output.SelectDb(int(dbid))
			continue
		} else if t == RDB_OPCODE_EOF {
			err = nil
			break
		} else {
			key, err := r.loadString()
			if err != nil {
				return err
			}
			// load redisObject
			if err := r.loadObject(key, t, expire); err != nil {
				return err
			}
		}
		expire = -1
	}

	if err != nil {
		return err
	}

	return nil
}

func (r *RDBParser) LayoutCheck() (bool, error) {
	if err := r.header(); err != nil {
		return false, err
	}
	return true, nil
}

// 5 bytes "REDIS" and 4 bytes version in rdb.file
func (r *RDBParser) header() error {
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

	if needEncode {
		switch length {
		case RDB_ENCODE_INT8:
			b, err := r.handler.ReadByte()
			return []byte(strconv.Itoa(int(b))), err
		case RDB_ENCODE_INT16:
			b, err := r.loadUint16()
			return []byte(strconv.Itoa(int(b))), err
		case RDB_ENCODE_INT32:
			b, err := r.loadUint32()
			return []byte(strconv.Itoa(int(b))), err
		case RDB_ENCODE_LZF:
			res, err := r.loadLZF()
			return res, err
		default:
			return []byte{}, errors.New("Unknown string encode type ")
		}
	}

	res := make([]byte, length)
	_, err = io.ReadFull(r.handler, res)
	return res, err
}

func (r *RDBParser) loadUint16() (res uint16, err error) {
	_, err = io.ReadFull(r.handler, buff[:2])
	if err != nil {
		return
	}

	res = binary.LittleEndian.Uint16(buff[:2])
	return
}

func (r *RDBParser) loadUint32() (res uint32, err error) {
	_, err = io.ReadFull(r.handler, buff[:4])
	if err != nil {
		return
	}
	res = binary.LittleEndian.Uint32(buff[:4])
	return
}

func (r *RDBParser) loadFloat() (float64, error) {
	b, err := r.handler.ReadByte()
	if err != nil {
		return 0, err
	}
	if b == 0xff {
		return NegInf, nil
	} else if b == 0xfe {
		return PosInf, nil
	} else if b == 0xfd {
		return Nan, nil
	}

	floatBytes := make([]byte, b)
	_, err = io.ReadFull(r.handler, floatBytes)
	if err != nil {
		return 0, err
	}
	float, err := strconv.ParseFloat(string(floatBytes)+`\0`, 64)
	return float, err
}

// 8 bytes float64, follow IEEE754 float64 stddef (standard definitions)
func (r *RDBParser) loadBinaryFloat() (float64, error) {
	if _, err := io.ReadFull(r.handler, buff); err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(buff)
	return math.Float64frombits(bits), nil
}

func (r *RDBParser) loadZipMap() error {
	zipmap, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newBuffer(zipmap)
	length, err := buf.ReadByte()
	if err != nil {
		return err
	}
	if length > 
}

func (r *RDBParser) loadObject(key []byte, t byte, expire uint64) error {
	convertExpire := int(expire)
	if t == RO_TYPE_STRING {
		val, err := r.loadString()
		if err != nil {
			return err
		}
		r.output.Set(key, val, convertExpire)
	} else if t == RO_TYPE_LIST {
		length, _, err := r.loadLen()
		if err != nil {
			return err
		}
		listCollect := make([][]byte, length)
		for i := uint64(0); i < length; i++ {
			val, err := r.loadString()
			if err != nil {
				return err
			}
			listCollect = append(listCollect, val)
		}
		r.output.List(key, convertExpire, listCollect...)
	} else if t == RO_TYPE_SET {
		length, _, err := r.loadLen()
		if err != nil {
			return err
		}
		setCollect := make([][]byte, length)
		for i := uint64(0); i < length; i++ {
			member, err := r.loadString()
			if err != nil {
				return err
			}
			setCollect = append(setCollect, member)
		}
		r.output.SAdd(key, convertExpire, setCollect...)
	} else if t == RO_TYPE_ZSET || t == RO_TYPE_ZSET_2 {
		length, _, err := r.loadLen()
		if err != nil {
			return err
		}
		for i := uint64(0); i < length; i++ {
			member, err := r.loadString()
			if err != nil {
				return err
			}
			if t == RO_TYPE_ZSET_2 {
				score, err := r.loadBinaryFloat()
				if err != nil {
					return err
				}
				r.output.ZSet(key, member, score)
			} else {
				score, err := r.loadFloat()
				if err != nil {
					return err
				}
				r.output.ZSet(key, member, score)
			}
		}
	} else if t == RO_TYPE_HASH {
		length, _, err := r.loadLen()
		if err != nil {
			return err
		}
		for i := uint64(0); i < length; i++ {
			field, err := r.loadString()
			if err != nil {
				return err
			}
			value, err := r.loadString()
			if err != nil {
				return err
			}
			r.output.HSet(key, field, value)
		}
	} else if t == RO_TYPE_LIST_QUICKLIST { // quicklist + ziplist to realize linked list
		length, _, err := r.loadLen()
		if err != nil {
			return err
		}
		ele := make([][]byte, length)
		for i := uint64(0); i < length; i++ {
			val, err := r.loadString()
			if err != nil {
				return err
			}
			ele = append(ele, val)
		}
		r.output.List(key, convertExpire, ele...)
	} else if t == RO_TYPE_HASH_ZIPMAP {

	} else if t == RO_TYPE_LIST_ZIPLIST {
	} else if t == RO_TYPE_SET_INTSET {
	} else if t == RO_TYPE_ZSET_ZIPLIST {
	} else if t == RO_TYPE_HASH_ZIPLIST {
	}

	return nil
}

func (r *RDBParser) loadLZF() (res []byte, err error) {
	ilength, _, err := r.loadLen()
	if err != nil {
		return
	}
	ulength, _, err := r.loadLen()
	if err != nil {
		return
	}
	val := make([]byte, ilength)
	_, err = io.ReadFull(r.handler, val)
	if err != nil {
		return
	}
	res = lzfDecompress(val, int(ilength), int(ulength))
	return
}

func lzfDecompress(in []byte, inLen, outLen int) []byte {
	out := make([]byte, outLen)
	for i, o := 0, 0; i < inLen; {
		ctrl := int(in[i])
		i++
		if ctrl < 1<<5 {
			for x := 0; x <= ctrl; x++ {
				out[o] = in[i]
				i++
				o++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length += int(in[i])
				i++
			}
			ref := o - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				out[o] = out[ref]
				ref++
				o++
			}
		}
	}

	return out
}
