package rdb

import (
	"encoding/json"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"strings"
)

// Some of HashEntry manager.
type HashMap struct {
	Field KeyObject
	Len   uint64
	Entry []HashEntry
}

// HashTable entry.
type HashEntry struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

func (r *ParseRdb) readHashMap(key KeyObject) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}
	hashTable := HashMap{Field: key, Len: length, Entry: make([]HashEntry, 0, length)}
	for i := uint64(0); i < length; i++ {
		field, err := r.loadString()
		if err != nil {
			return err
		}
		value, err := r.loadString()
		if err != nil {
			return err
		}
		hashTable.Entry = append(hashTable.Entry, HashEntry{Field: ToString(field), Value: ToString(value)})
	}
	r.d2 <- hashTable
	return nil
}

func (r *ParseRdb) readHashMapWithZipmap(key KeyObject) error {
	zipmap, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newInput(zipmap)
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

	hashTable := HashMap{Field: key, Len: uint64(length), Entry: make([]HashEntry, 0, length)}
	for i := 0; i < length; i++ {
		field, err := loadZipmapItem(buf, false)
		if err != nil {
			return err
		}
		value, err := loadZipmapItem(buf, true)
		if err != nil {
			return err
		}
		hashTable.Entry = append(hashTable.Entry, HashEntry{Field: ToString(field), Value: ToString(value)})
	}
	r.d2 <- hashTable
	return nil
}

func (r *ParseRdb) readHashMapZiplist(key KeyObject) error {
	b, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newInput(b)
	length, err := loadZiplistLength(buf)
	if err != nil {
		return err
	}
	length /= 2

	hashTable := HashMap{Field: key, Len: uint64(length), Entry: make([]HashEntry, 0, length)}
	for i := int64(0); i < length; i++ {
		field, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		value, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		hashTable.Entry = append(hashTable.Entry, HashEntry{Field: ToString(field), Value: ToString(value)})
	}
	r.d2 <- hashTable
	return nil
}

func (hm HashMap) Type() string {
	return protocol.Hash
}

func (hm HashMap) String() string {
	return fmt.Sprintf("{HashMap: {Key: %s, Len: %d, Entries: %s}}", hm.Key(), hm.Len, hm.Value())
}

func (hm HashMap) Key() string {
	return ToString(hm.Field)
}

func (hm HashMap) Value() string {
	if len(hm.Entry) > 0 {
		itemStr, _ := json.Marshal(hm.Entry)
		return ToString(itemStr)
	}
	return ""
}

func (hm HashMap) ValueLen() uint64 {
	return uint64(len(hm.Entry))
}

// 计算 hash 结构 field + value 的大小
func (hm HashMap) ConcreteSize() uint64 {
	kv := make([]string, 0, len(hm.Entry))
	for _, val := range hm.Entry {
		tmp := val
		kv = append(kv, tmp.Field+tmp.Value)
	}
	return uint64(len(strings.Join(kv, "")))
}
