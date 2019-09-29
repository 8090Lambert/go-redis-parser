package rdb

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
)

type StringObject struct {
	Field KeyObject
	Val   interface{}
}

func (r *ParseRdb) readString(key KeyObject) error {
	valBytes, err := r.loadString()
	if err != nil {
		return err
	}
	valObject := NewStringObject(key, valBytes)
	//r.d1 = append(r.d1, valObject.String())
	r.d1 = append(r.d1, valObject)
	r.d2 <- valObject
	return nil
}

func NewStringObject(key KeyObject, val interface{}) StringObject {
	return StringObject{Field: key, Val: val}
}

func (s StringObject) String() string {
	return fmt.Sprintf("{String: {Key: %s, Value:'%s'}}", s.Key(), s.Value())
}

func (s StringObject) Type() string {
	return protocol.String
}

func (s StringObject) Key() string {
	return ToString(s.Field)
}

func (s StringObject) Value() string {
	return ToString(s.Val)
}

func (s StringObject) ValueLen() uint64 {
	return s.ConcreteSize()
}

// String类型，计算对应value
func (s StringObject) ConcreteSize() uint64 {
	return uint64(len([]byte(s.Value())))
}
