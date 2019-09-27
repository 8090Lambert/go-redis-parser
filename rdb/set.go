package rdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"strconv"
	"strings"
)

type Set struct {
	Field   KeyObject
	Len     uint64
	Entries []string
}

func (r *ParseRdb) readSet(key KeyObject) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}
	set := Set{Field: key, Len: length, Entries: make([]string, 0, length)}
	for i := uint64(0); i < length; i++ {
		member, err := r.loadString()
		if err != nil {
			return err
		}
		set.Entries = append(set.Entries, ToString(member))
	}
	//r.d1 = append(r.d1, set.String())
	r.d1 = append(r.d1, set)

	return nil
}

func (r *ParseRdb) readIntSet(key KeyObject) error {
	b, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newInput(b)
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
	set := Set{Field: key, Len: uint64(cardinality), Entries: make([]string, 0, cardinality)}
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
	}
	//r.d1 = append(r.d1, set.String())
	r.d1 = append(r.d1, set)
	return nil
}

func (s Set) Type() string {
	return protocol.Set
}

func (s Set) String() string {
	return fmt.Sprintf("{Set: {Key: %s, Len: %d, Item: %s}}", s.Key(), s.Len, s.Value())
}

func (s Set) Key() string {
	return ToString(s.Field)
}

func (s Set) Value() string {
	return ToString(strings.Join(s.Entries, ","))
}

func (s Set) ValueLen() uint64 {
	return uint64(len(s.Entries))
}

// Set 结构计算所有item
func (s Set) ConcreteSize() uint64 {
	return uint64(len([]byte(s.Value())) - (len(s.Entries) - 1))
}
