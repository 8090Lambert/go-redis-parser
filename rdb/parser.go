package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/generator"
	"github.com/8090Lambert/go-redis-parser/parse"
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

	// Redis RDB protocol
	RDB_OPCODE_IDLE          = 248 /* LRU idle time. */
	RDB_OPCODE_FREQ          = 249 /* LFU frequency. */
	RDB_OPCODE_AUX           = 250 /* RDB aux field. */
	RDB_OPCODE_RESIZEDB      = 251 /* Hash table resize hint. */
	RDB_OPCODE_EXPIRETIME_MS = 252 /* Expire time in milliseconds. */
	RDB_OPCODE_EXPIRETIME    = 253 /* Old expire time in seconds. */
	RDB_OPCODE_SELECTDB      = 254 /* DB number of the following keys. */
	RDB_OPCODE_EOF           = 255

	// Redis length type
	RDB_6BIT   = 0
	RDB_14BIT  = 1
	RDB_32BIT  = 0x80
	RDB_64BIT  = 0x81
	RDB_ENCVAL = 3

	// Redis ziplist types
	ZIP_STR_06B = 0
	ZIP_STR_14B = 1
	ZIP_STR_32B = 2

	// Redis ziplist entry
	ZIP_INT_4B  = 15
	ZIP_INT_8B  = 0xfe        // 11111110
	ZIP_INT_16B = 0xc0 | 0<<4 // 11000000
	ZIP_INT_24B = 0xc0 | 3<<4 // 11110000
	ZIP_INT_32B = 0xc0 | 1<<4 // 11010000
	ZIP_INT_64B = 0xc0 | 2<<4 //11100000

	ZIP_BIG_PREVLEN = 0xfe

	// Redis listpack
	STREAM_ITEM_FLAG_NONE       = 0      /* No special flags. */
	STREAM_ITEM_FLAG_DELETED    = 1 << 0 /* Entry was deleted. Skip it. */
	STREAM_ITEM_FLAG_SAMEFIELDS = 1 << 1 /* Same fields as master entry. */
)

const (
	RDB_ENCODE_INT8 = iota
	RDB_ENCODE_INT16
	RDB_ENCODE_INT32
	RDB_ENCODE_LZF

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

type ParseRdb struct {
	handler *bufio.Reader
	output  generator.Putter
	data    chan interface{}
	exit    chan struct{}
}

func NewRDB(file string) parse.Parser {
	handler, err := os.Open(file)
	if err != nil {
		panic(err.Error())
	}

	return &ParseRdb{handler: bufio.NewReader(handler), output: generator.Putter{}, data: make(chan interface{})}
}

func (r *ParseRdb) Parse() error {
	go func() {
		for {
			select {
			case v := <-r.data:
				fmt.Println(v)
			case <-r.exit:
				break
			}
		}
	}()
	_, err := r.layoutCheck()
	if err != nil {
		return err
	}

	var lru_idle, lfu_freq, expire int64
	//var expire uint64
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
			lru_idle = int64(qword)
			r.output.LRU(lru_idle)
			continue
		} else if t == RDB_OPCODE_FREQ {
			b, err := r.handler.ReadByte()
			if err != nil {
				return err
			}
			lfu_freq = int64(b)
			r.output.LFU(lfu_freq)
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
			expire = int64(binary.LittleEndian.Uint64(buff))
			continue
		} else if t == RDB_OPCODE_EXPIRETIME {
			_, err := io.ReadFull(r.handler, buff)
			if err != nil {
				return errors.New("Parse ExpireTime failed: " + err.Error())
			}
			expire = int64(binary.LittleEndian.Uint64(buff)) * 1000
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
			// TODO rdb checksum
			return nil
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
		lfu_freq, lru_idle, expire = -1, -1, 0
	}

	if err != nil {
		return err
	}

	return nil
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
	if !bytes.Equal(header[0:5], []byte(REDIS)) || err != nil || (rdbVersion < RDB_VERSION_MIN || rdbVersion > RDB_VERSION_MAX) {
		return false, errors.New("RDB file version is wrong")
	}

	return true, nil
}

func (r *ParseRdb) loadObject(key []byte, t byte, expire int64) error {
	keyObj := NewKeyObject(key, expire)
	if t == RO_TYPE_STRING {
		if err := r.readString(keyObj); err != nil {
			return err
		}
	} else if t == RO_TYPE_LIST {
		if err := r.readList(keyObj); err != nil {
			return err
		}
	} else if t == RO_TYPE_SET {
		//length, _, err := r.loadLen()
		//if err != nil {
		//	return err
		//}
		//setCollect := make([][]byte, length)
		//for i := uint64(0); i < length; i++ {
		//	member, err := r.loadString()
		//	if err != nil {
		//		return err
		//	}
		//	setCollect = append(setCollect, member)
		//}
		//r.output.SAdd(key, expire, setCollect...)
		if err := r.readSet(keyObj); err != nil {
			return err
		}
	} else if t == RO_TYPE_ZSET || t == RO_TYPE_ZSET_2 {
		//length, _, err := r.loadLen()
		//if err != nil {
		//	return err
		//}
		//for i := uint64(0); i < length; i++ {
		//	member, err := r.loadString()
		//	if err != nil {
		//		return err
		//	}
		//	var score float64
		//	if t == RO_TYPE_ZSET_2 {
		//		score, err = r.loadBinaryFloat()
		//		if err != nil {
		//			return err
		//		}
		//		r.output.ZSet(key, member, score)
		//	} else {
		//		score, err = r.loadFloat()
		//		if err != nil {
		//			return err
		//		}
		//		r.output.ZSet(key, member, score)
		//	}
		//}
		if err := r.readZSet(keyObj, t); err != nil {
			return err
		}
	} else if t == RO_TYPE_HASH {
		//length, _, err := r.loadLen()
		//if err != nil {
		//	return err
		//}
		//for i := uint64(0); i < length; i++ {
		//	field, err := r.loadString()
		//	if err != nil {
		//		return err
		//	}
		//	value, err := r.loadString()
		//	if err != nil {
		//		return err
		//	}
		//	r.output.HSet(key, field, value, expire)
		//}
		keyObj := NewKeyObject(key, expire)
		if err := r.readHashMap(keyObj); err != nil {
			return err
		}
	} else if t == RO_TYPE_LIST_QUICKLIST { // quicklist + ziplist to realize linked list
		//length, _, err := r.loadLen()
		//if err != nil {
		//	return err
		//}
		//
		//for i := uint64(0); i < length; i++ {
		//	listItems, err := r.loadZipList()
		//	if err != nil {
		//		return err
		//	}
		//	r.output.List(key, expire, listItems...)
		//}
		if err := r.readListWithQuickList(keyObj); err != nil {
			return err
		}
	} else if t == RO_TYPE_HASH_ZIPMAP {
		if err := r.readHashMapWithZipmap(keyObj); err != nil {
			return err
		}
		//return r.loadZipMap()
	} else if t == RO_TYPE_LIST_ZIPLIST {
		if err := r.readListWithZipList(keyObj); err != nil {
			return err
		}
		//_, err := r.loadZipList()
		//return err
	} else if t == RO_TYPE_SET_INTSET {
		if err := r.readIntSet(keyObj); err != nil {
			return err
		}
		//return r.loadIntSet(key, expire)
	} else if t == RO_TYPE_ZSET_ZIPLIST {
		if err := r.readZipListSortSet(keyObj); err != nil {
			return err
		}
		//return r.loadZiplistZset(key, expire)
	} else if t == RO_TYPE_HASH_ZIPLIST {
		if err := r.readHashMapZiplist(keyObj); err != nil {
			return err
		}
		//return r.loadZiplistHash(key, expire)
	} else if t == RO_TYPE_STREAM_LISTPACKS {
		_, err := r.loadStreamListPack()
		return err
	}

	return nil
}

func (r *ParseRdb) loadLen() (length uint64, isEncode bool, err error) {
	buf, err := r.handler.ReadByte()
	if err != nil {
		return
	}
	typeLen := (buf & 0xc0) >> 6
	if typeLen == RDB_ENCVAL || typeLen == RDB_6BIT {
		/* Read a 6 bit encoding type or 6 bit len. */
		if typeLen == RDB_ENCVAL {
			isEncode = true
		}
		length = uint64(buf) & 0x3f
	} else if typeLen == RDB_14BIT {
		/* Read a 14 bit len, need read next byte. */
		nb, err := r.handler.ReadByte()
		if err != nil {
			return 0, false, err
		}
		length = (uint64(buf)&0x3f)<<8 | uint64(nb)
	} else if buf == RDB_32BIT {
		_, err = io.ReadFull(r.handler, buff[0:4])
		if err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint32(buff))
	} else if buf == RDB_64BIT {
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

func (r *ParseRdb) loadZipMap() error {
	zipmap, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newStream(zipmap)
	blen, err := buf.ReadByte()
	if err != nil {
		return err
	}

	length := int(blen)
	if blen > 254 {
		length, err = countZipmapItems(buf)
		if err != nil {
			return err
		}
		length /= 2
	}

	for i := 0; i < length; i++ {
		field, err := loadZipmapItem(buf, false)
		if err != nil {
			return err
		}
		value, err := loadZipmapItem(buf, true)
		if err != nil {
			return err
		}
		r.output.HSet([]byte("test"), field, value, 123)
	}
	return nil
}

func (r *ParseRdb) loadStreamListPack() (map[string]interface{}, error) {
	// stream entry
	entries, err := r.loadStreamEntry()
	if err != nil {
		return nil, nil
	}

	length, _, _ := r.loadLen()
	msi64, _, _ := r.loadLen()
	seqi64, _, _ := r.loadLen()
	lastId := concatStreamId([]byte(strconv.Itoa(int(msi64))), []byte(strconv.Itoa(int(seqi64))))
	stream := map[string]interface{}{"last_id": lastId, "length": strconv.Itoa(int(length))}

	//stream group
	groups, err := r.loadStreamGroup()

	entriesBytes, _ := json.Marshal(entries)
	groupBytes, _ := json.Marshal(groups)
	stream["entries"] = string(entriesBytes)
	stream["groups"] = string(groupBytes)

	return stream, nil
}

func (r *ParseRdb) loadStreamEntry() (map[string]interface{}, error) {
	entryLength, _, err := r.loadLen()
	if err != nil {
		return nil, err
	}

	entries := make(map[string]interface{}, entryLength)
	for i := uint64(0); i < entryLength; i++ {
		streamAuxBytes, err := r.loadString()
		if err != nil {
			return nil, err
		}
		header := newStream(streamAuxBytes)
		msBytes, err := header.Slice(8) // ms
		if err != nil {
			return nil, err
		}
		seqBytes, err := header.Slice(8) // seq
		if err != nil {
			return nil, err
		}
		messageId := concatStreamId(msBytes, seqBytes)

		headerBytes, err := r.loadString()
		lp := newStream(headerBytes)
		// Skip the header.
		// 4b total-bytes + 2b num-elements
		lp.Seek(6, 1)
		entry, err := loadStreamItem(lp)
		if err != nil {
			return nil, err
		}
		entries[messageId] = entry
	}

	return entries, nil
}

func (r *ParseRdb) loadStreamGroup() ([]map[string]interface{}, error) {
	/*Redis group, struct is this
	typedef struct streamCG {
	 	streamID last_id
	     rax *pel
			rax *consumers;
	}*/
	groupCount, _, err := r.loadLen()
	if err != nil {
		return nil, err
	}

	groups := make([]map[string]interface{}, 0, groupCount)
	for i := uint64(0); i < groupCount; i++ {
		gName, err := r.loadString()
		if err != nil {
			return nil, err
		}
		msi64, _, _ := r.loadLen()
		seqi64, _, _ := r.loadLen()
		// last_id
		groupLastid := concatStreamId([]byte(strconv.Itoa(int(msi64))), []byte(strconv.Itoa(int(seqi64))))
		groupItem := map[string]interface{}{
			"group_name": string(gName),
			"last_id":    groupLastid,
		}

		// global pel
		pel, _, _ := r.loadLen()
		groupPendingEntries := make(map[string]map[string]string, pel)
		for i := uint64(0); i < pel; i++ {
			io.ReadFull(r.handler, buff)
			msBytes := buff
			io.ReadFull(r.handler, buff)
			seqBytes := buff
			rawId := concatStreamId(msBytes, seqBytes)
			io.ReadFull(r.handler, buff)
			deliveryTime := uint64(binary.LittleEndian.Uint64(buff))
			deliveryCount, _, _ := r.loadLen()
			item := map[string]string{"delivery_time": strconv.Itoa(int(deliveryTime)), "delivery_count": strconv.Itoa(int(deliveryCount))}
			groupPendingEntries[rawId] = item
		}

		// Consumer
		consumerCount, _, _ := r.loadLen()
		consumers := make([]map[string]interface{}, 0, consumerCount)
		for i := uint64(0); i < consumerCount; i++ {
			cName, err := r.loadString()
			if err != nil {
				return nil, err
			}
			io.ReadFull(r.handler, buff)
			seenTime := uint64(binary.LittleEndian.Uint64(buff))
			consumerItem := map[string]interface{}{
				"consumer_name": string(cName),
				"seen_time":     strconv.Itoa(int(seenTime)),
			}

			// Consumer pel
			pel, _, _ := r.loadLen()
			consumersPendingEntries := make(map[string]map[string]string, pel)
			for i := uint64(0); i < pel; i++ {
				io.ReadFull(r.handler, buff)
				msBytes := buff
				io.ReadFull(r.handler, buff)
				seqBytes := buff
				rawId := concatStreamId(msBytes, seqBytes)

				itemBytes, _ := json.Marshal(consumerItem)
				groupPendingEntries[rawId]["consumer"] = string(itemBytes)
				//entryString, _ := json.Marshal(groupPendingEntries[rawId])
				consumersPendingEntries[rawId] = groupPendingEntries[rawId]
			}
			pendingEntryString, _ := json.Marshal(consumersPendingEntries)
			consumerItem["pendings"] = string(pendingEntryString)
			consumers = append(consumers, consumerItem)
		}

		//gpendingString, _ := json.Marshal(groupPendingEntries)
		groupItem["pendings"] = groupPendingEntries
		//consumersStr, _ := json.Marshal(consumers)
		groupItem["consumers"] = consumers
		groups = append(groups, groupItem)
	}

	return groups, nil
}

func (r *ParseRdb) loadEOF(version int) (checkSum []byte, err error) {
	if version >= 5 {
		_, err = io.ReadFull(r.handler, buff)
		if err != nil {
			return checkSum, errors.New("Parse Check_sum failed: " + err.Error())
		}
		//checkSum = uint64(binary.LittleEndian.Uint64(buff))
		checkSum = buff
		return
	}
	return
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
