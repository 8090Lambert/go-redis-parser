package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/command"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	// Redis Object type
	TypeString = iota
	TypeList
	TypeSet
	TypeZset
	TypeHash
	TypeZset2 /* ZSET version 2 with doubles stored in binary. */
	TypeModule
	TypeModule2 // Module should import module entity, not support at present.
	_
	TypeHashZipMap
	TypeListZipList
	TypeSetIntSet
	TypeZsetZipList
	TypeHashZipList
	TypeListQuickList
	TypeStreamListPacks

	// Redis RDB protocol
	FlagOpcodeIdle         = 248 /* LRU idle time. */
	FlagOpcodeFreq         = 249 /* LFU frequency. */
	FlagOpcodeAux          = 250 /* RDB aux field. */
	FlagOpcodeResizeDB     = 251 /* Hash table resize hint. */
	FlagOpcodeExpireTimeMs = 252 /* Expire time in milliseconds. */
	FlagOpcodeExpireTime   = 253 /* Old expire time in seconds. */
	FlagOpcodeSelectDB     = 254 /* DB number of the following keys. */
	FlagOpcodeEOF          = 255

	// Redis length type
	Type6Bit   = 0
	Type14Bit  = 1
	Type32Bit  = 0x80
	Type64Bit  = 0x81
	TypeEncVal = 3

	// Redis ziplist types
	ZipStr06B = 0
	ZipStr14B = 1
	ZipStr32B = 2

	// Redis ziplist entry
	ZipInt04B = 15
	ZipInt08B = 0xfe        // 11111110
	ZipInt16B = 0xc0 | 0<<4 // 11000000
	ZipInt24B = 0xc0 | 3<<4 // 11110000
	ZipInt32B = 0xc0 | 1<<4 // 11010000
	ZipInt64B = 0xc0 | 2<<4 //11100000

	ZipBigPrevLen = 0xfe

	// Redis listpack
	StreamItemFlagNone       = 0      /* No special flags. */
	StreamItemFlagDeleted    = 1 << 0 /* Entry was deleted. Skip it. */
	StreamItemFlagSameFields = 1 << 1 /* Same fields as master entry. */
)

const (
	EncodeInt8 = iota
	EncodeInt16
	EncodeInt32
	EncodeLZF

	REDIS      = "REDIS"
	VersionMin = 1
	VersionMax = 9
)

var (
	buff   = make([]byte, 8)
	PosInf = math.Inf(1)
	NegInf = math.Inf(-1)
	Nan    = math.NaN()
	prefix = "parser" // output file prefix
	suffix = ".csv"
)

type ParseRdb struct {
	handler *bufio.Reader
	wg      sync.WaitGroup
	d1      []interface{}
	d2      chan protocol.TypeObject
	quit    chan struct{}
	writer  *WriterRDB
}

func NewRDB(file string) protocol.Parser {
	handler, err := os.Open(file)
	if err != nil {
		panic(err.Error())
	}

	if command.GenFileType == "json" {
		suffix = ".json"
	}
	writer, err := os.OpenFile(generateFileName(prefix, suffix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err.Error())
	}
	gather := map[string][]uint64{protocol.String: make([]uint64, 2), protocol.Hash: make([]uint64, 2), protocol.List: make([]uint64, 2), protocol.SortedSet: make([]uint64, 2), protocol.Set: make([]uint64, 2), protocol.Stream: make([]uint64, 2)}
	biggest := map[string][]string{protocol.String: make([]string, 0, 3), protocol.Hash: make([]string, 0, 3), protocol.List: make([]string, 0, 3), protocol.SortedSet: make([]string, 0, 3), protocol.Set: make([]string, 0, 3), protocol.Stream: make([]string, 0, 3)}

	return &ParseRdb{
		handler: bufio.NewReader(handler),
		d1:      make([]interface{}, 1),
		d2:      make(chan protocol.TypeObject),
		quit:    make(chan struct{}),
		writer:  NewRDBWriter(writer, gather, biggest),
	}
}

func (r *ParseRdb) Parse() {
	r.listening()
	if res, err := r.layoutCheck(); res == false || err != nil {
		panic(err.Error())
	}
	if err := r.start(); err != nil {
		panic(err.Error())
	}
	r.endListening()
	r.flushWriter()
}

func (r *ParseRdb) listening() {
	r.wg.Add(1)
	go func() {
		for {
			select {
			case v := <-r.d2:
				r.collect(v)
			case <-r.quit:
				r.wg.Done()
				return
			}
		}
	}()
}

func (r *ParseRdb) endListening() {
	close(r.quit)
	r.wg.Wait()
}

// 9 bytes length include: 5 bytes "REDIS" and 4 bytes version in rdb.file
func (r *ParseRdb) layoutCheck() (bool, error) {
	header := make([]byte, 9)
	_, err := io.ReadFull(r.handler, header)
	if err != nil {
		if err == io.EOF {
			return false, errors.New("RDB file is empty")
		}
		return false, errors.New("Read RDB file failed, error: " + err.Error())
	}

	// Check "REDIS" string and version.
	rdbVersion, err := strconv.Atoi(string(header[5:]))
	if !bytes.Equal(header[0:5], []byte(REDIS)) || err != nil || (rdbVersion < VersionMin || rdbVersion > VersionMax) {
		return false, errors.New("RDB file version is wrong")
	}

	return true, nil
}

func (r *ParseRdb) start() error {
	//var lruIdle, lfuIdle int64
	var expire int64
	var hasSelectDb bool
	var t byte // Object type
	var err error
	for {
		// Begin analyze
		t, err = r.handler.ReadByte()
		if err != nil {
			break
		}
		if t == FlagOpcodeIdle {
			b, _, err := r.loadLen()
			if err != nil {
				break
			}
			_ = int64(b) // lruIdle
			continue
		} else if t == FlagOpcodeFreq {
			b, err := r.handler.ReadByte()
			if err != nil {
				break
			}
			_ = int64(b) // lfuIdle
			continue
		} else if t == FlagOpcodeAux {
			// RDB 7 版本之后引入
			// redis-ver：版本号
			// redis-bits：OS Arch
			// ctime：RDB文件创建时间
			// used-mem：使用内存大小
			// repl-stream-db：在server.master客户端中选择的数据库
			// repl-id：当前实例 replication ID
			// repl-offset：当前实例复制的偏移量
			// lua：lua脚本
			key, err := r.loadString()
			if err != nil {
				err = errors.New("Parse Aux key failed: " + err.Error())
				break
			}
			val, err := r.loadString()
			if err != nil {
				err = errors.New("Parse Aux value failed: " + err.Error())
				break
			}
			r.AuxFields(key, val)
			continue
		} else if t == FlagOpcodeResizeDB {
			// RDB 7 版本之后引入，详见 https://github.com/antirez/redis/pull/5039/commits/5cd3c9529df93b7e726256e2de17985a57f00e7b
			// 包含两个编码后的值，用于加速RDB的加载，避免在加载过程中额外的调整hash空间(resize)和rehash操作
			// 1.数据库的哈希表大小
			// 2.失效哈希表的大小
			dbSize, _, err := r.loadLen()
			if err != nil {
				err = errors.New("Parse ResizeDB size failed: " + err.Error())
				break
			}
			expiresSize, _, err := r.loadLen()
			if err != nil {
				err = errors.New("Parse ResizeDB size failed: " + err.Error())
				break
			}
			r.Resize(dbSize, expiresSize)
			continue
		} else if t == FlagOpcodeExpireTimeMs {
			_, err := io.ReadFull(r.handler, buff)
			if err != nil {
				err = errors.New("Parse ExpireTime_ms failed: " + err.Error())
				break
			}
			expire = int64(binary.LittleEndian.Uint64(buff))
			continue
		} else if t == FlagOpcodeExpireTime {
			_, err := io.ReadFull(r.handler, buff)
			if err != nil {
				err = errors.New("Parse ExpireTime failed: " + err.Error())
				break
			}
			expire = int64(binary.LittleEndian.Uint64(buff)) * 1000
			continue
		} else if t == FlagOpcodeSelectDB {
			if hasSelectDb == true {
				continue
			}
			dbindex, _, err := r.loadLen()
			if err != nil {
				break
			}
			r.Selection(dbindex)
			hasSelectDb = false
			continue
		} else if t == FlagOpcodeEOF {
			// TODO rdb checksum
			err = nil
			break
		}
		// Read key
		key, err := r.loadString()
		if err != nil {
			return err
		}
		// Read value
		if err := r.loadObject(key, t, expire); err != nil {
			return err
		}
		expire = -1
		//lfuIdle, lruIdle = -1, -1
	}

	return err
}

func (r *ParseRdb) loadObject(key []byte, t byte, expire int64) error {
	keyObj := NewKeyObject(key, expire)
	if t == TypeString {
		if err := r.readString(keyObj); err != nil {
			return err
		}
	} else if t == TypeList {
		if err := r.readList(keyObj); err != nil {
			return err
		}
	} else if t == TypeSet {
		if err := r.readSet(keyObj); err != nil {
			return err
		}
	} else if t == TypeZset || t == TypeZset2 {
		if err := r.readZSet(keyObj, t); err != nil {
			return err
		}
	} else if t == TypeHash {
		keyObj := NewKeyObject(key, expire)
		if err := r.readHashMap(keyObj); err != nil {
			return err
		}
	} else if t == TypeListQuickList { // quicklist + ziplist to realize linked list
		if err := r.readListWithQuickList(keyObj); err != nil {
			return err
		}
	} else if t == TypeHashZipMap {
		if err := r.readHashMapWithZipmap(keyObj); err != nil {
			return err
		}
	} else if t == TypeListZipList {
		if err := r.readListWithZipList(keyObj); err != nil {
			return err
		}
	} else if t == TypeSetIntSet {
		if err := r.readIntSet(keyObj); err != nil {
			return err
		}
		//return r.loadIntSet(key, expire)
	} else if t == TypeZsetZipList {
		if err := r.readZipListSortSet(keyObj); err != nil {
			return err
		}
	} else if t == TypeHashZipList {
		if err := r.readHashMapZiplist(keyObj); err != nil {
			return err
		}
	} else if t == TypeStreamListPacks {
		if err := r.loadStreamListPack(keyObj); err != nil {
			return err
		}
	} else if t == TypeModule || t == TypeModule2 {
		return errors.New("Module should import module entity, not support at present! ")
	}

	return nil
}

func (r *ParseRdb) loadLen() (length uint64, isEncode bool, err error) {
	buf, err := r.handler.ReadByte()
	if err != nil {
		return
	}
	typeLen := (buf & 0xc0) >> 6
	if typeLen == TypeEncVal || typeLen == Type6Bit {
		/* Read a 6 bit encoding type or 6 bit len. */
		if typeLen == TypeEncVal {
			isEncode = true
		}
		length = uint64(buf) & 0x3f
	} else if typeLen == Type14Bit {
		/* Read a 14 bit len, need read next byte. */
		nb, err := r.handler.ReadByte()
		if err != nil {
			return 0, false, err
		}
		length = (uint64(buf)&0x3f)<<8 | uint64(nb)
	} else if buf == Type32Bit {
		_, err = io.ReadFull(r.handler, buff[0:4])
		if err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint32(buff))
	} else if buf == Type64Bit {
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

func (r *ParseRdb) loadString() ([]byte, error) {
	length, needEncode, err := r.loadLen()
	if err != nil {
		return nil, err
	}

	if needEncode {
		switch length {
		case EncodeInt8:
			b, err := r.handler.ReadByte()
			return []byte(strconv.Itoa(int(b))), err
		case EncodeInt16:
			b, err := r.loadUint16()
			return []byte(strconv.Itoa(int(b))), err
		case EncodeInt32:
			b, err := r.loadUint32()
			return []byte(strconv.Itoa(int(b))), err
		case EncodeLZF:
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

func (r *ParseRdb) loadUint16() (res uint16, err error) {
	_, err = io.ReadFull(r.handler, buff[:2])
	if err != nil {
		return
	}

	res = binary.LittleEndian.Uint16(buff[:2])
	return
}

func (r *ParseRdb) loadUint32() (res uint32, err error) {
	_, err = io.ReadFull(r.handler, buff[:4])
	if err != nil {
		return
	}
	res = binary.LittleEndian.Uint32(buff[:4])
	return
}

func (r *ParseRdb) loadFloat() (float64, error) {
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
	float, err := strconv.ParseFloat(string(floatBytes), 64)
	return float, err
}

// 8 bytes float64, follow IEEE754 float64 stddef (standard definitions)
func (r *ParseRdb) loadBinaryFloat() (float64, error) {
	if _, err := io.ReadFull(r.handler, buff); err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(buff)
	return math.Float64frombits(bits), nil
}

func (r *ParseRdb) loadLZF() (res []byte, err error) {
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

func (r *ParseRdb) collect(entity protocol.TypeObject) {
	// AllKV
	r.writer.AdditionKV(entity)
	// Gather && Biggest
	if !strings.EqualFold(entity.Type(), protocol.Aux) && !strings.EqualFold(entity.Type(), protocol.SelectDB) && !strings.EqualFold(entity.Type(), protocol.ResizeDB) {
		r.writer.KeysCount += 1
		r.writer.KeysSize += uint64(len([]byte(entity.Key())))
		// Gather all keys
		if _, ok := r.writer.Gather[entity.Type()]; ok {
			r.writer.Gather[entity.Type()][0] += 1
			r.writer.Gather[entity.Type()][1] += entity.ValueLen()
		}
		// Compare biggest key
		if len(r.writer.Biggest[entity.Type()]) == 0 {
			r.writer.Biggest[entity.Type()] = append(r.writer.Biggest[entity.Type()], entity.Key())
			r.writer.Biggest[entity.Type()] = append(r.writer.Biggest[entity.Type()], strconv.FormatUint(entity.ValueLen(), 10))
			r.writer.Biggest[entity.Type()] = append(r.writer.Biggest[entity.Type()], strconv.FormatUint(entity.ConcreteSize(), 10))
		} else if strings.Compare(r.writer.Biggest[entity.Type()][2], strconv.FormatUint(entity.ConcreteSize(), 10)) == -1 {
			r.writer.Biggest[entity.Type()][0] = entity.Key()
			r.writer.Biggest[entity.Type()][1] = strconv.FormatUint(entity.ValueLen(), 10)
			r.writer.Biggest[entity.Type()][2] = strconv.FormatUint(entity.ConcreteSize(), 10)
		}
	}
}

func (r *ParseRdb) flushWriter() {
	r.wg.Add(2)
	go func() {
		r.writer.FlushFile()
		r.writer.FlushGather()
		r.wg.Done()
		r.wg.Done()
	}()
	r.wg.Wait()
}
