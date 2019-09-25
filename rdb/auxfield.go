package rdb

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
)

type AuxField struct {
	Name  interface{}
	Field interface{}
}

func AuxFields(key, val []byte) AuxField {
	aux := AuxField{Name: key, Field: val}
	return aux
}

func (af AuxField) String() string {
	return fmt.Sprintf("{Aux: {Key: %s, Value: %s}}", ToString(af.Name), ToString(af.Field))
}

func (af AuxField) Key() string {
	return ToString(af.Name)
}

func (af AuxField) Value() string {
	return ToString(af.Field)
}

func (af AuxField) Type() string {
	return protocol.Aux
}

// 辅助字段全部返回0
func (af AuxField) ConcreteSize() uint64 {
	return 0
}
