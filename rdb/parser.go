package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
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
)

type ParseRdb struct {
	handler *bufio.Reader
	wg      sync.WaitGroup
	d1      []interface{}
}

func NewRDB(file string) protocol.Parser {
	handler, err := os.Open(file)
	if err != nil {
		panic(err.Error())
	}

	return &ParseRdb{handler: bufio.NewReader(handler), d1: make([]interface{}, 0)}
}

func (r *ParseRdb) Parse() {
	if res, err := r.layoutCheck(); res == false || err != nil {
		panic(err.Error())
	}
	if err := r.start(); err != nil {
		panic(err.Error())
	}

	if len(r.d1) > 0 {
		r.wg.Add(2)
		go r.findBiggestKey()
		go r.output()
		r.wg.Wait()
	}
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
			r.d1 = append(r.d1, AuxFields(key, val))
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
			r.d1 = append(r.d1, Resize(dbSize, expiresSize))
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
			r.d1 = append(r.d1, Selection(dbindex))
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

func (r *ParseRdb) output() {
	defer r.wg.Done()
	if command.GenFileType == "json" {
		r.writeJson()
	} else {
		r.writeCsv()
	}
}

func (r *ParseRdb) generateFileName(prefix, suffix string) string {
	dir := command.Output
	if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) {
		os.Mkdir(dir, 0755)
	}
	fileName := strings.TrimRight(dir, "/") + "/" + prefix + suffix
	return fileName
}

func (r *ParseRdb) writeCsv() {
	data := make([][]string, 0, len(r.d1)+1)
	data = append(data, []string{"DataType", "Key", "Value", "Size(bytes)"})
	for _, val := range r.d1 {
		if entity, ok := val.(protocol.TypeObject); ok {
			data = append(data, []string{entity.Type(), ToString(entity.Key()), ToString(entity.Value()), ToString(entity.ConcreteSize())})
		}
	}
	f, err := os.OpenFile(r.generateFileName(prefix, ".csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic("Open file wrong.")
	}
	w := csv.NewWriter(f)
	w.WriteAll(data)
	w.Flush()
}

func (r *ParseRdb) writeJson() {
	b, err := json.Marshal(r.d1)
	if err != nil {
		panic("Convert json wrong.")
	}
	f, err := os.OpenFile(r.generateFileName(prefix, ".json"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic("Open file wrong.")
	}
	w := bufio.NewWriter(f)
	w.Write(b)
	w.Flush()
}

func (r *ParseRdb) findBiggestKey() {
	defer r.wg.Done()
	var count, keySize uint64 // value's count and key's bytes
	turns := []string{protocol.String, protocol.Hash, protocol.List, protocol.SortedSet, protocol.Set, protocol.Stream}
	units := map[string]string{protocol.String: "bytes", protocol.Hash: "fields", protocol.List: "items", protocol.SortedSet: "members", protocol.Set: "members", protocol.Stream: "entries"}
	gather := map[string][]uint64{protocol.String: make([]uint64, 2), protocol.Hash: make([]uint64, 2), protocol.List: make([]uint64, 2), protocol.SortedSet: make([]uint64, 2), protocol.Set: make([]uint64, 2), protocol.Stream: make([]uint64, 2)}
	biggest := map[string][]string{protocol.String: make([]string, 0, 3), protocol.Hash: make([]string, 0, 3), protocol.List: make([]string, 0, 3), protocol.SortedSet: make([]string, 0, 3), protocol.Set: make([]string, 0, 3), protocol.Stream: make([]string, 0, 3)}
	println("# Scanning the rdb file to find biggest keys\n")
	for _, val := range r.d1 {
		if entity, ok := val.(protocol.TypeObject); ok && !strings.EqualFold(entity.Type(), protocol.Aux) && !strings.EqualFold(entity.Type(), protocol.SelectDB) {
			count += 1
			keySize += uint64(len([]byte(entity.Key())))
			// Gather all keys
			gather[entity.Type()][0] += 1
			gather[entity.Type()][1] += entity.ValueLen()

			// Compare biggest key
			if len(biggest[entity.Type()]) == 0 {
				biggest[entity.Type()] = append(biggest[entity.Type()], entity.Key())
				biggest[entity.Type()] = append(biggest[entity.Type()], strconv.FormatUint(entity.ValueLen(), 10))
				biggest[entity.Type()] = append(biggest[entity.Type()], strconv.FormatUint(entity.ConcreteSize(), 10))
			} else if strings.Compare(biggest[entity.Type()][2], strconv.FormatUint(entity.ConcreteSize(), 10)) == -1 {
				biggest[entity.Type()][0] = entity.Key()
				biggest[entity.Type()][1] = strconv.FormatUint(entity.ValueLen(), 10)
				biggest[entity.Type()][2] = strconv.FormatUint(entity.ConcreteSize(), 10)
			}
		}
	}
	println("-------- summary -------\n")
	println(fmt.Sprintf("Sampled %d keys in the keyspace!", count))
	println(fmt.Sprintf("Total key length in bytes is %d\n", keySize))

	// Biggest.
	for _, val := range turns {
		if len(biggest[val]) > 0 {
			println(fmt.Sprintf("Biggest %6s found '%s' has %s %s", strings.ToLower(val), biggest[val][0], biggest[val][1], units[val]))
		}
	}
	println()

	// Gather
	for _, val := range turns {
		if len(gather[val]) > 0 {
			println(fmt.Sprintf("%d %s with %d %s", gather[val][0], strings.ToLower(val), gather[val][1], units[val]))
		}
	}
}
