package rdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

type Stream struct {
	Entries []map[string]interface{}
	Length  uint64
	LastId  StreamId
	Groups  []StreamGroup
}

type StreamId struct {
	Ms       uint64
	Sequence uint64
}

type StreamGroup struct {
	Name             string
	LastId           string
	PendingEntryList map[string]interface{}
	Consumers        []StreamConsumer
}

type StreamConsumer struct {
	SeenTime         uint64
	Name             string
	PendingEntryList map[string]interface{}
}

type StreamNACK struct {
	Consumer      StreamConsumer
	DeliveryTime  uint64
	DeliveryCount uint64
}

var check string

func loadStreamItem(lp *stream, stId StreamId) (map[string]interface{}, error) {
	// Entry format:
	// | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
	countBytes, err := loadStreamListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	//count := binary.LittleEndian.Uint16(countBytes)
	//count, _ := strconv.ParseInt(string(countBytes), 10, 64)
	count, _ := strconv.ParseUint(string(countBytes), 10, 64)
	deletedBytes, err := loadStreamListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	deleted, _ := strconv.ParseInt(string(deletedBytes), 10, 64)
	//deleted := binary.LittleEndian.Uint64(deletedBytes)
	fieldsNumBytes, err := loadStreamListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	//fieldsNum := binary.LittleEndian.Uint64(fieldsNumBytes)
	fieldsNum, _ := strconv.ParseUint(string(fieldsNumBytes), 10, 64)
	//fieldsNum, _ := strconv.Atoi(string(fieldsNumBytes))
	fieldCollect := make([][]byte, 0, fieldsNum)
	for i := uint64(0); i < fieldsNum; i++ {
		check = "field"
		tmp, err := loadStreamListPackEntry(lp)
		if err != nil {
			return nil, err
		}
		check = ""
		fieldCollect = append(fieldCollect, tmp)
		//fieldCollect[i] = tmp
	}
	loadStreamListPackEntry(lp)

	total := uint64(count) + uint64(deleted)
	allStreams := make(map[string]interface{}, total)
	for i := uint64(0); i < total; i++ {
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

		ms, _ := strconv.ParseUint(string(msBytes), 10, 64)
		seq, _ := strconv.ParseUint(string(seqBytes), 10, 64)
		//messageId := concatStreamId(string(msBytes), string(seqBytes))
		messageId := stId.BuildOn(ms, seq).String()

		hasDelete := "false"
		if flag&StreamItemFlagDeleted != 0 {
			hasDelete = "true"
		}
		if flag&StreamItemFlagSameFields == 0 {
			fieldsNumBytes, err := loadStreamListPackEntry(lp)
			if err != nil {
				return nil, err
			}
			//fieldsNum = binary.LittleEndian.Uint64(fieldsNumBytes)
			//fieldsNum, _ = strconv.Atoi(string(fieldsNumBytes))
			fieldsNum, _ = strconv.ParseUint(string(fieldsNumBytes), 10, 64)
		}
		fields := make(map[string]interface{}, fieldsNum)
		for i := uint64(0); i < fieldsNum; i++ {
			fieldBytes := fieldCollect[i]
			if flag&StreamItemFlagSameFields == 0 {
				//check = "field"
				fieldBytes, err = loadStreamListPackEntry(lp)
				//check = ""
				if err != nil {
					return nil, err
				}
			} else {
				fieldBytes = fieldCollect[i]
				//fmt.Println("outset:", fieldBytes, " i:", i)
			}
			vBytes, err := loadStreamListPackEntry(lp)
			if err != nil {
				return nil, err
			}
			//fmt.Println(fieldBytes, vBytes)
			fields[string(fieldBytes)] = string(vBytes)
			//fields[strconv.FormatUint(binary.LittleEndian.Uint64(fieldBytes), 10)] = strconv.FormatUint(binary.LittleEndian.Uint64(vBytes), 10)
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
	var branch int
	if special&0x80 == 0 {
		branch = 1
		skip = 1
		res = []byte(strconv.FormatInt(int64(special&0x7F), 10))
	} else if special&0xC0 == 0x80 {
		branch = 2
		// return Slice
		length := special & 0x3F
		skip = 1 + int(length)
		res, err = buf.Slice(int(length))
		if err != nil {
			return nil, err
		}
	} else if special&0xE0 == 0xC0 {
		branch = 3
		skip = 2
		next, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		//n := (uint32(special&0x1F)<<8 | uint32(next)) << 19 >> 19
		if check == "field" {
			fmt.Println(special, uint32(special&0x1F)<<8|uint32(next), int32(uint32(special&0x1F)<<8|uint32(next))<<19>>19, int64(int32(uint32(special&0x1F)<<8|uint32(next))<<19>>19))
		}
		//int64(int32(uint32(special&0x1F)<<8|uint32(next)) << 19 >> 19)
		res = []byte(strconv.FormatInt(int64(int32(uint32(special&0x1F)<<8|uint32(next))<<19>>19), 10))
		//res = []byte(strconv.FormatUint(uint64((uint32(special&0x1F)<<8|uint32(next))<<19>>19), 10))
	} else if special&0xFF == 0xF1 {
		branch = 4
		skip = 3
		b, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(b))), 10))
	} else if special&0xFF == 0xF2 {
		branch = 5
		skip = 4
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10))
	} else if special&0xFF == 0xF3 {
		branch = 6
		skip = 5
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10))
	} else if special&0xFF == 0xF4 {
		branch = 7
		skip = 9
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10))
	} else if special&0xF0 == 0xE0 {
		branch = 8
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
		branch = 9
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		length := uint64(binary.BigEndian.Uint32(lenBytes))
		skip = 5 + int(length)
		res, err = buf.Slice(int(length))
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("unknown encoding type")
	}

	if check == "field" {
		fmt.Println(fmt.Sprintf("branch: %d", branch), res)
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

func concatStreamId(ms, seq string) string {
	return ms + "-" + seq
}

func (si StreamId) String() string {
	return strconv.FormatUint(si.Ms, 10) + "-" + strconv.FormatUint(si.Sequence, 10)
}

func (si StreamId) BuildOn(ms, seq uint64) StreamId {
	newMs := si.Ms + ms
	newSequence := si.Sequence + seq
	return StreamId{Ms: newMs, Sequence: newSequence}
}
