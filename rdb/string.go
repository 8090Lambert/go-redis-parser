package rdb

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
)

type StringObject struct {
	Key   KeyObject
	Value interface{}
}

func (r *ParseRdb) readString(key KeyObject) error {
	valBytes, err := r.loadString()
	if err != nil {
		return err
	}
	valObject := NewStringObject(key, valBytes)
	r.d1 = append(r.d1, valObject.String())
	return nil
}

func NewStringObject(key KeyObject, val interface{}) StringObject {
	return StringObject{Key: key, Value: val}
}

func (s StringObject) String() string {
	return fmt.Sprintf("{String: {Key: %s, Value:'%s'}}", ToString(s.Key), ToString(s.Value))
}

func (s StringObject) Type() protocol.DataType {
	return protocol.String
}

func (s StringObject) ConcreteSize() uint64 {
	return 1
}
