package rdb

import "fmt"

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
	r.data <- valObject
	return nil
}

func NewStringObject(key KeyObject, val interface{}) StringObject {
	return StringObject{Key: key, Value: val}
}

func (s StringObject) String() string {
	return fmt.Sprintf("{String: {Key: %s, Value:'%s'}}", ToString(s.Key), ToString(s.Value))
}
