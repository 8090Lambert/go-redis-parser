package rdb

import (
	"fmt"
	"strings"
)

type ListObject struct {
	Key     KeyObject
	Len     uint64
	Entries []string
}

func (r *ParseRdb) readList(key KeyObject) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}
	listObj := ListObject{Key: key, Len: length, Entries: make([]string, 0, length)}
	for i := uint64(0); i < length; i++ {
		val, err := r.loadString()
		if err != nil {
			return err
		}
		listObj.Entries = append(listObj.Entries, ToString(val))
	}
	r.data <- listObj

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
		listObj := ListObject{Key: key, Len: uint64(len(listItems)), Entries: make([]string, 0, len(listItems))}
		for _, v := range listItems {
			listObj.Entries = append(listObj.Entries, ToString(v))
		}
		r.data <- listObj
	}

	return nil
}

func (r *ParseRdb) readListWithZipList(key KeyObject) error {
	entries, err := r.loadZipList()
	if err != nil {
		return err
	}
	listObj := ListObject{Key: key, Len: uint64(len(entries)), Entries: make([]string, 0, len(entries))}
	for _, v := range entries {
		listObj.Entries = append(listObj.Entries, ToString(v))
	}
	r.data <- listObj

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

func (l ListObject) String() string {
	return fmt.Sprintf("{List: {Key: %s, Len: %d, Items: %s}}", ToString(l.Key), l.Len, strings.Join(l.Entries, ","))
}
