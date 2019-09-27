package rdb

import (
	"encoding/json"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"strconv"
)

type SortedSet struct {
	Field   KeyObject
	Len     uint64
	Entries []SortedSetEntry
}

type SortedSetEntry struct {
	Field interface{}
	Score float64
}

func (r *ParseRdb) readZSet(key KeyObject, t byte) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}
	sortedSet := SortedSet{Field: key, Len: length, Entries: make([]SortedSetEntry, 0, length)}
	for i := uint64(0); i < length; i++ {
		member, err := r.loadString()
		if err != nil {
			return err
		}
		var score float64
		if t == TypeZset2 {
			score, err = r.loadBinaryFloat()
		} else {
			score, err = r.loadFloat()
		}
		if err != nil {
			return err
		}
		sortedSet.Entries = append(sortedSet.Entries, SortedSetEntry{Field: ToString(member), Score: score})
	}
	//r.d1 = append(r.d1, sortedSet.String())
	r.d1 = append(r.d1, sortedSet)

	return nil
}

func (r *ParseRdb) readZipListSortSet(key KeyObject) error {
	b, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newInput(b)
	cardinality, err := loadZiplistLength(buf)
	if err != nil {
		return err
	}
	cardinality /= 2

	sortedSet := SortedSet{Field: key, Len: uint64(cardinality), Entries: make([]SortedSetEntry, 0, cardinality)}
	for i := int64(0); i < cardinality; i++ {
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
		sortedSet.Entries = append(sortedSet.Entries, SortedSetEntry{Field: ToString(member), Score: score})
	}
	//r.d1 = append(r.d1, sortedSet.String())
	r.d1 = append(r.d1, sortedSet)

	return nil
}

func (zs SortedSet) Type() string {
	return protocol.SortedSet
}

func (zs SortedSet) String() string {
	return fmt.Sprintf("SortedSetMetadata{Key: %s, Len: %d, Entries: %s}", zs.Key(), zs.Len, zs.Value())
}

func (zs SortedSet) Key() string {
	return ToString(zs.Field)
}

func (zs SortedSet) Value() string {
	itemStr, _ := json.Marshal(zs.Entries)
	return ToString(itemStr)
}

func (zs SortedSet) ValueLen() uint64 {
	return uint64(len(zs.Entries))
}

func (zs SortedSet) ConcreteSize() uint64 {
	var size uint64
	if len(zs.Entries) > 0 {
		for _, val := range zs.Entries {
			size += uint64(len([]byte(ToString(val.Field))))
		}
	}
	return size
}
