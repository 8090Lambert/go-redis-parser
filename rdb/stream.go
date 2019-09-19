package rdb

import (
	"encoding/binary"
	"errors"
	"strconv"
)

type Stream struct {
	Entries []map[string]interface{}
	Length  uint64
	LastId  StreamId
	Groups  []Group
}

type StreamId struct {
	Ms       uint64
	Sequence uint64
}

type Group struct {
	Name             string
	LastId           StreamId
	PendingEntryList map[string]map[string]string
}

func loadStreamItem(lp *stream) (map[string]interface{}, error) {
	// Entry format:
	// | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
	countBytes, err := loadStreamListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	count, _ := strconv.ParseInt(string(countBytes), 10, 64)
	deletedBytes, err := loadStreamListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	deleted, _ := strconv.ParseInt(string(deletedBytes), 10, 64)
	fieldsNumBytes, err := loadStreamListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	fieldsNum, _ := strconv.Atoi(string(fieldsNumBytes))
	fieldCollect := make([][]byte, fieldsNum)
	for i := 0; i < fieldsNum; i++ {
		tmp, err := loadStreamListPackEntry(lp)
		if err != nil {
			return nil, err
		}
		fieldCollect[i] = tmp
	}
	loadStreamListPackEntry(lp)

	total := count + deleted
	allStreams := make(map[string]interface{}, total)
	for i := int64(0); i < total; i++ {
		flagBytes, err := loadStreamListPackEntry(lp)
		if err != nil {
			return nil, err
		}
		flag, _ := strconv.Atoi(string(flagBytes))
		msBytes, err := loadStreamListPackEntry(lp) // ms
		if err != nil {
			return nil, err
		}
		seqBytes, err := loadStreamListPackEntry(lp) // seq
		if err != nil {
			return nil, err
		}
		messageId := concatStreamId(msBytes, seqBytes)
		hasDelete := "false"
		if flag&STREAM_ITEM_FLAG_DELETED != 0 {
			hasDelete = "true"
		}
		if flag&STREAM_ITEM_FLAG_SAMEFIELDS == 0 {
			fieldsNumBytes, err := loadStreamListPackEntry(lp)
			if err != nil {
				return nil, err
			}
			fieldsNum, _ = strconv.Atoi(string(fieldsNumBytes))
		}
		fields := make(map[string]interface{}, fieldsNum)
		for i := 0; i < fieldsNum; i++ {
			fieldBytes := fieldCollect[i]
			if flag&STREAM_ITEM_FLAG_SAMEFIELDS == 0 {
				fieldBytes, err = loadStreamListPackEntry(lp)
				if err != nil {
					return nil, err
				}
			}
			vBytes, err := loadStreamListPackEntry(lp)
			if err != nil {
				return nil, err
			}
			fields[string(fieldBytes)] = string(vBytes)
		}
		terms := map[string]interface{}{
			"has_deleted": hasDelete,
			"fields":      fields,
		}
		allStreams[messageId] = terms
		loadStreamListPackEntry(lp)
	}
	endBytes, err := lp.ReadByte()
	if endBytes != 255 {
		return nil, errors.New("ListPack expect 255 with end")
	}

	return allStreams, nil
}

func loadStreamListPackEntry(buf *stream) ([]byte, error) {
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
