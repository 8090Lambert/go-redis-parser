package rdb

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"strings"
)

type ListObject struct {
	Field   KeyObject
	Len     uint64
	Entries []string
}

func (r *ParseRdb) readList(key KeyObject) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}
	listObj := ListObject{Field: key, Len: length, Entries: make([]string, 0, length)}
	for i := uint64(0); i < length; i++ {
		val, err := r.loadString()
		if err != nil {
			return err
		}
		listObj.Entries = append(listObj.Entries, ToString(val))
	}
	//r.d1 = append(r.d1, listObj.String())
	r.d1 = append(r.d1, listObj)

	return nil
}

func (r *ParseRdb) readListWithQuickList(key KeyObject) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}

	for i := uint64(0); i < length; i++ {
		listItems, err := r.loadZipList()
		if err != nil {
			return err
		}
		listObj := ListObject{Field: key, Len: uint64(len(listItems)), Entries: make([]string, 0, len(listItems))}
		for _, v := range listItems {
			listObj.Entries = append(listObj.Entries, ToString(v))
		}
		//r.d1 = append(r.d1, listObj.String())
		r.d1 = append(r.d1, listObj)
	}

	return nil
}

func (r *ParseRdb) readListWithZipList(key KeyObject) error {
	entries, err := r.loadZipList()
	if err != nil {
		return err
	}
	listObj := ListObject{Field: key, Len: uint64(len(entries)), Entries: make([]string, 0, len(entries))}
	for _, v := range entries {
		listObj.Entries = append(listObj.Entries, ToString(v))
	}
	//r.d1 = append(r.d1, listObj.String())
	r.d1 = append(r.d1, listObj)

	return nil
}

func (r *ParseRdb) loadZipList() ([][]byte, error) {
	b, err := r.loadString()
	if err != nil {
		return nil, err
	}
	buf := newInput(b)
	length, err := loadZiplistLength(buf)
	if err != nil {
		return nil, err
	}

	items := make([][]byte, 0, length)
	for i := int64(0); i < length; i++ {
		entry, err := loadZiplistEntry(buf)
		if err != nil {
			return nil, err
		}
		items = append(items, entry)
	}

	return items, nil
}

func (l ListObject) Type() string {
	return protocol.List
}

func (l ListObject) String() string {
	return fmt.Sprintf("{List: {Key: %s, Len: %d, Items: %s}}", l.Key(), l.Len, l.Value())
}

func (l ListObject) Key() string {
	return ToString(l.Field)
}

func (l ListObject) Value() string {
	return strings.Join(l.Entries, ",")
}

// list 结构计算所有item
func (l ListObject) ConcreteSize() uint64 {
	return uint64(len([]byte(l.Value())) - len(l.Entries) - 1) // 减去分隔符占用字节数
}
