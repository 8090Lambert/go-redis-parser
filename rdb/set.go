package rdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Set struct {
	Key     KeyObject
	Len     uint64
	Entries []string
}

func (r *ParseRdb) readSet(key KeyObject) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}
	set := Set{Key: key, Len: length, Entries: make([]string, 0, length)}
	for i := uint64(0); i < length; i++ {
		member, err := r.loadString()
		if err != nil {
			return err
		}
		set.Entries = append(set.Entries, ToString(member))
	}
	r.data <- set

	return nil
}

func (r *ParseRdb) readIntSet(key KeyObject) error {
	b, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newStream(b)
	sizeBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	intSize := binary.LittleEndian.Uint32(sizeBytes)
	if intSize != 2 && intSize != 4 && intSize != 8 {
		return errors.New(fmt.Sprintf("unknown intset encoding: %d", intSize))
	}
	lenBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	cardinality := binary.LittleEndian.Uint32(lenBytes)
	//intSetItem := make([][]byte, 0, cardinality)
	set := Set{Key: key, Len: uint64(cardinality), Entries: make([]string, 0, cardinality)}
	for i := uint32(0); i < cardinality; i++ {
		intBytes, err := buf.Slice(int(intSize))
		if err != nil {
			return err
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
		set.Entries = append(set.Entries, ToString(intString))
		//intSetItem = append(intSetItem, []byte(intString))
	}
	//r.output.SAdd(key, expire, intSetItem...)
	//fmt.Println(set.String())
	r.data <- set
	return nil
}

func (s Set) String() string {
	return fmt.Sprintf("Set{Key: %s, Len: %d, Item: %s}", ToString(s.Key), s.Len, strings.Join(s.Entries, ","))
}
