package parse

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
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

	// Redis string type
	RDB_LOAD_NONE  = 0
	RDB_LOAD_ENC   = 1 << 0
	RDB_LOAD_PLAIN = 1 << 1

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

	var lru_idle, lfu_freq int64
	var expire uint64
	//var expire uint64 = -1
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
			dbIndex, _, err := r.loadLen()
			if err != nil {
				return err
			}
			r.output.SelectDb(int(dbIndex))
			continue
		} else if t == RDB_OPCODE_EOF {
			continue
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

func (r *RDBParser) LayoutCheck() (bool, error) {
	if err := r.header(); err != nil {
		return false, err
	}
	return true, nil
}

// 9 bytes length include: 5 bytes "REDIS" and 4 bytes version in rdb.file
func (r *RDBParser) header() error {
	header := make([]byte, 9)
	_, err := io.ReadFull(r.handler, header)
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
		listItems := make([][]byte, 0, length)
		for i := uint64(0); i < length; i++ {
			val, err := r.loadString()
			if err != nil {
				return err
			}
			listItems = append(listItems, val)
		}
		r.output.List(key, convertExpire, listItems...)
	} else if t == RO_TYPE_SET {
		length, _, err := r.loadLen()
		if err != nil {
			return err
		}
		setCollect := make([][]byte, 0, length)
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
			r.output.HSet(key, field, value, convertExpire)
		}
	} else if t == RO_TYPE_LIST_QUICKLIST { // quicklist + ziplist to realize linked list
		length, _, err := r.loadLen()
		if err != nil {
			return err
		}

		for i := uint64(0); i < length; i++ {
			listItems, err := r.loadZipList()
			if err != nil {
				return err
			}
			r.output.List(key, convertExpire, listItems...)
		}
	} else if t == RO_TYPE_HASH_ZIPMAP {
		return r.loadZipMap(key, convertExpire)
	} else if t == RO_TYPE_LIST_ZIPLIST {
		listItems, err := r.loadZipList()
		if err != nil {
			return err
		}
		r.output.List(key, convertExpire, listItems...)
		return err
	} else if t == RO_TYPE_SET_INTSET {
		intSet, err := r.loadIntSet()
		if err != nil {
			return err
		}
		r.output.SAdd(key, convertExpire, intSet...)
		return err
	} else if t == RO_TYPE_ZSET_ZIPLIST {
		return r.loadZiplistZset(key, convertExpire)
	} else if t == RO_TYPE_HASH_ZIPLIST {
		return r.loadZiplistHash(key, convertExpire)
	} else if t == RO_TYPE_STREAM_LISTPACKS {
		fmt.Println(key)
		_, err := r.loadStreamListPack()
		return err
	}

	return nil
}

func (r *RDBParser) loadLen() (length uint64, isEncode bool, err error) {
	rawByte, err := r.handler.ReadByte()
	if err != nil {
		return
	}
	theType := (rawByte & 0xc0) >> 6
	if theType == RDB_ENCVAL || theType == RDB_6BIT {
		/* Read a 6 bit encoding type or 6 bit len. */
		if theType == RDB_ENCVAL { // This value need go on decode.
			isEncode = true
		}
		length = uint64(rawByte) & 0x3f
	} else if theType == RDB_14BIT {
		/* Read a 14 bit len, need read next byte. */
		nb, err := r.handler.ReadByte()
		if err != nil {
			return 0, false, err
		}
		length = (uint64(rawByte)&0x3f)<<8 | uint64(nb)
	} else if rawByte == RDB_32BIT {
		_, err = io.ReadFull(r.handler, buff[0:4])
		if err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint32(buff))
	} else if rawByte == RDB_64BIT {
		_, err = io.ReadFull(r.handler, buff)
		if err != nil {
			return
		}
		length = binary.BigEndian.Uint64(buff)
	} else {
		err = errors.New(fmt.Sprintf("unknown length encoding %d in loadLen()", theType))
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
			return []byte(strconv.FormatInt(int64(int8(uint8(b))), 10)), err
		case RDB_ENCODE_INT16:
			b, err := r.loadUint16()
			return []byte(strconv.FormatInt(int64(int16(b)), 10)), err
		case RDB_ENCODE_INT32:
			b, err := r.loadUint32()
			return []byte(strconv.FormatInt(int64(int32(b)), 10)), err
		case RDB_ENCODE_LZF:
			res, err := r.loadLZFCompress()
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
	var float float64
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
	float, err = strconv.ParseFloat(string(floatBytes), 64)

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

func (r *RDBParser) loadZipMap(key []byte, expire int) error {
	zipmap, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newBuffer(zipmap)
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
		r.output.HSet(key, field, value, expire)
	}
	return nil
}

func (r *RDBParser) loadZipList() ([][]byte, error) {
	b, err := r.loadString()
	if err != nil {
		return nil, err
	}
	buf := newBuffer(b)
	length, err := loadZiplistLength(buf)
	if err != nil {
		return nil, err
	}

	ziplistItem := make([][]byte, 0, length)
	for i := int64(0); i < length; i++ {
		entry, err := loadZiplistEntry(buf)
		if err != nil {
			return nil, err
		}
		ziplistItem = append(ziplistItem, entry)
	}

	return ziplistItem, nil
}

func (r *RDBParser) loadIntSet() ([][]byte, error) {
	b, err := r.loadString()
	if err != nil {
		return nil, err
	}
	buf := newBuffer(b)
	sizeBytes, err := buf.Slice(4)
	if err != nil {
		return nil, err
	}
	intSize := binary.LittleEndian.Uint32(sizeBytes)
	if intSize != 2 && intSize != 4 && intSize != 8 {
		return nil, errors.New(fmt.Sprintf("unknown intset encoding: %d", intSize))
	}
	lenBytes, err := buf.Slice(4)
	if err != nil {
		return nil, err
	}

	length := binary.LittleEndian.Uint32(lenBytes)
	intSetItem := make([][]byte, 0, length)
	for i := uint32(0); i < length; i++ {
		intBytes, err := buf.Slice(int(intSize))
		if err != nil {
			return nil, err
		}
		var intString string
		switch intSize {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		intSetItem = append(intSetItem, []byte(intString))
	}

	return intSetItem, nil
}

func (r *RDBParser) loadZiplistZset(key []byte, expire int) error {
	ziplist, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newBuffer(ziplist)
	length, err := loadZiplistLength(buf)
	if err != nil {
		return err
	}
	length /= 2
	for i := int64(0); i < length; i++ {
		member, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		scoreBytes, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		score, err := strconv.ParseFloat(string(scoreBytes), 64)
		if err != nil {
			return err
		}
		r.output.ZSet(key, member, score)
	}

	return nil
}

func (r *RDBParser) loadZiplistHash(key []byte, expire int) error {
	ziplist, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newBuffer(ziplist)
	length, err := loadZiplistLength(buf)
	if err != nil {
		return err
	}
	length /= 2
	for i := int64(0); i < length; i++ {
		field, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		value, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		r.output.HSet(key, field, value, expire)
	}
	return nil
}

func (r *RDBParser) loadStreamListPack() (map[string]map[string]string, error) {
	listpacks, _, err := r.loadLen()
	if err != nil {
		return nil, err
	}

	listPackStreams := make(map[string]map[string]map[string]string, listpacks)
	for i := uint64(0); i < listpacks; i++ {
		headerBytes, err := r.loadString()
		if err != nil {
			return nil, err
		}
		header := newBuffer(headerBytes)
		msBytes, err := header.Slice(8) // ms
		if err != nil {
			return nil, err
		}
		seqBytes, err := header.Slice(8) // seq
		if err != nil {
			return nil, err
		}
		messageId := concatStreamId(msBytes, seqBytes)

		listpackBytes, err := r.loadString()
		lp := newBuffer(listpackBytes)
		// Skip the header.
		// 4b total-bytes + 2b num-elements
		lp.Seek(6, 1)
		streams, err := loadStreamItem(lp)
		if err != nil {
			return nil, err
		}
		listPackStreams[messageId] = streams
	}

	length, _, _ := r.loadLen()
	// group
	msi64, _, _ := r.loadLen()
	seqi64, _, _ := r.loadLen()

	lastId := concatStreamId([]byte(strconv.Itoa(int(msi64))), []byte(strconv.Itoa(int(seqi64))))
	groupCount, _, err := r.loadLen()
	if err != nil {
		return nil, err
	}
	fmt.Println(lastId)

	/**
	Redis group struct:
	typedef struct streamCG {
		streamID last_id
		rax *pel
		rax *consumers;
	}
	*/
	groups := make([]map[string]string, 0, groupCount)
	for i := uint64(0); i < groupCount; i++ {
		gName, err := r.loadString()
		if err != nil {
			return nil, err
		}
		msi64, _, _ = r.loadLen()
		seqi64, _, _ = r.loadLen()
		// last_id
		groupLastid := concatStreamId([]byte(strconv.Itoa(int(msi64))), []byte(strconv.Itoa(int(seqi64))))
		groupItem := map[string]string{
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

		// consumer
		consumerCount, _, _ := r.loadLen()
		consumers := make([]map[string]string, 0, consumerCount)
		for i := uint64(0); i < consumerCount; i++ {
			cName, err := r.loadString()
			if err != nil {
				return nil, err
			}
			io.ReadFull(r.handler, buff)
			seenTime := uint64(binary.LittleEndian.Uint64(buff))
			consumerItem := map[string]string{
				"consumer_name": string(cName),
				"seen_time":     strconv.Itoa(int(seenTime)),
			}

			// consumer pel
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

		//
		gpendingString, _ := json.Marshal(groupPendingEntries)
		groupItem["pendings"] = string(gpendingString)
		consumersStr, _ := json.Marshal(consumers)
		groupItem["consumers"] = string(consumersStr)
		groups = append(groups)
	}
	//msiBytes := make([]byte, 8)
	//lastId := concatStreamId()

	return nil, nil
}

func loadStreamItem(lp *buffer) (map[string]map[string]string, error) {
	// Entry format:
	// | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
	countBytes, err := loadListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	count, _ := strconv.ParseInt(string(countBytes), 10, 64)
	deletedBytes, err := loadListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	deleted, _ := strconv.ParseInt(string(deletedBytes), 10, 64)
	fieldsNumBytes, err := loadListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	fieldsNum, _ := strconv.Atoi(string(fieldsNumBytes))
	fieldCollect := make([][]byte, fieldsNum)
	for i := 0; i < fieldsNum; i++ {
		tmp, err := loadListPackEntry(lp)
		if err != nil {
			return nil, err
		}
		fieldCollect[i] = tmp
	}
	loadListPackEntry(lp)

	total := count + deleted
	allStreams := make(map[string]map[string]string, total)
	for i := int64(0); i < total; i++ {
		storage := make(map[string]string)
		flagBytes, err := loadListPackEntry(lp)
		if err != nil {
			return nil, err
		}
		flag, _ := strconv.Atoi(string(flagBytes))
		msBytes, err := loadListPackEntry(lp) // ms
		if err != nil {
			return nil, err
		}
		seqBytes, err := loadListPackEntry(lp) // seq
		if err != nil {
			return nil, err
		}
		messageId := concatStreamId(msBytes, seqBytes)

		if flag&STREAM_ITEM_FLAG_SAMEFIELDS == 0 {
			fieldsNumBytes, err := loadListPackEntry(lp)
			if err != nil {
				return nil, err
			}
			fieldsNum, _ = strconv.Atoi(string(fieldsNumBytes))
		}
		for i := 0; i < fieldsNum; i++ {
			fieldBytes := fieldCollect[i]
			if flag&STREAM_ITEM_FLAG_SAMEFIELDS == 0 {
				fieldBytes, err = loadListPackEntry(lp)
				if err != nil {
					return nil, err
				}
			}
			vBytes, err := loadListPackEntry(lp)
			if err != nil {
				return nil, err
			}
			storage[string(fieldBytes)] = string(vBytes)
		}
		allStreams[messageId] = storage
		loadListPackEntry(lp)
	}
	endBytes, err := lp.ReadByte()
	if endBytes != 255 {
		return nil, errors.New("ListPack expect 255 with end")
	}

	return allStreams, nil
}

func loadZipmapItem(buf *buffer, readFree bool) ([]byte, error) {
	length, free, err := loadZipmapItemLength(buf, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	value, err := buf.Slice(length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Seek(int64(free), 1)
	return value, err
}

func countZipmapItems(buf *buffer) (int, error) {
	n := 0
	for {
		strLen, free, err := loadZipmapItemLength(buf, n%2 != 0)
		if err != nil {
			return 0, err
		}
		if strLen == -1 {
			break
		}
		_, err = buf.Seek(int64(strLen)+int64(free), 1)
		if err != nil {
			return 0, err
		}
		n++
	}
	_, err := buf.Seek(0, 0)
	return n, err
}

func loadZipmapItemLength(buf *buffer, readFree bool) (int, int, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		s, err := buf.Slice(5)
		if err != nil {
			return 0, 0, err
		}
		return int(binary.BigEndian.Uint32(s)), int(s[4]), nil
	case 254:
		return 0, 0, errors.New("Invalid zipmap item length. ")
	case 255:
		return -1, 0, nil
	}
	var free byte
	if readFree {
		free, err = buf.ReadByte()
	}

	return int(b), int(free), err
}

func loadZiplistLength(buf *buffer) (int64, error) {
	buf.Seek(8, 0)
	lenBytes, err := buf.Slice(2)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint16(lenBytes)), nil
}

func loadZiplistEntry(buf *buffer) ([]byte, error) {
	prevLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if prevLen == ZIP_BIG_PREVLEN {
		buf.Seek(4, 1) // skip the 4-byte prevlen
	}

	header, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	switch {
	case header>>6 == ZIP_STR_06B:
		return buf.Slice(int(header & 0x3f))
	case header>>6 == ZIP_STR_14B:
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		return buf.Slice((int(header&0x3f) << 8) | int(b))
	case header>>6 == ZIP_STR_32B:
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return buf.Slice(int(binary.BigEndian.Uint32(lenBytes)))
	case header>>4 == ZIP_INT_4B:
		return []byte(strconv.FormatInt(int64(header&0x0f)-1, 10)), nil
	case header == ZIP_INT_8B:
		b, err := buf.ReadByte()
		return []byte(strconv.FormatInt(int64(int8(b)), 10)), err
	case header == ZIP_INT_16B:
		intBytes, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)), nil
	case header == ZIP_INT_24B:
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10)), nil
	case header == ZIP_INT_32B:
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)), nil
	case header == ZIP_INT_64B:
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10)), nil
	}

	return nil, errors.New(fmt.Sprintf("rdb: unknown ziplist header byte: %d", header))
}

func loadListPackEntry(buf *buffer) ([]byte, error) {
	special, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	res := make([]byte, 0)
	skip := 0
	if special&0x80 == 0 {
		skip = 1
		res = []byte(strconv.FormatInt(int64(special&0x7F), 10))
	} else if special&0xC0 == 0x80 {
		length := special & 0x3F
		skip = 1 + int(length)
		res, err = buf.Slice(int(length))
		if err != nil {
			return nil, err
		}
	} else if special&0xE0 == 0xC0 {
		skip = 2
		next, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64((((special&0x1F)<<8)|next)<<19>>19), 10))
	} else if special&0xFF == 0xF1 {
		skip = 3
		res, err = buf.Slice(2)
		if err != nil {
			return nil, err
		}
	} else if special&0xFF == 0xF2 {
		skip = 4
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10))
	} else if special&0xFF == 0xF3 {
		skip = 5
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10))
	} else if special&0xFF == 0xF4 {
		skip = 9
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint64(intBytes))), 10))
	} else if special&0xF0 == 0xE0 {
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		length := ((special & 0x0F) << 8) | b
		skip = 2 + int(length)
		res, err = buf.Slice(int(length))
		if err != nil {
			return nil, err
		}
	} else if special&0xFF == 0xf0 {
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		length := binary.BigEndian.Uint32(lenBytes)
		skip = 5 + int(length)
		res, err = buf.Slice(int(length))
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("unknown encoding type")
	}

	// element-total-len
	if skip <= 127 {
		buf.Seek(1, 1)
	} else if skip < 16383 {
		buf.Seek(2, 1)
	} else if skip < 2097151 {
		buf.Seek(3, 1)
	} else if skip < 268435455 {
		buf.Seek(4, 1)
	} else {
		buf.Seek(5, 1)
	}
	return res, err
}

func concatStreamId(ms, seq []byte) string {
	return string(ms) + "-" + string(seq)
}

// LZF uncompress
func (r *RDBParser) loadLZFCompress() (res []byte, err error) {
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
	res = LzfDecompress(val, int(ilength), int(ulength))
	return
}

func LzfDecompress(in []byte, inLen, outLen int) []byte {
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

func (r *RDBParser) free(val ...interface{}) {

}
