package rdb

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
)

type AuxField struct {
	Field interface{}
	Val   interface{}
}

func (r *ParseRdb) AuxFields(key, val []byte) AuxField {
	aux := AuxField{Field: key, Val: val}
	r.d2 <- aux
	return aux
}

func (af AuxField) String() string {
	return fmt.Sprintf("{Aux: {Key: %s, Value: %s}}", ToString(af.Field), ToString(af.Val))
}

func (af AuxField) Key() string {
	return ToString(af.Field)
}

func (af AuxField) Value() string {
	return ToString(af.Val)
}

func (af AuxField) ValueLen() uint64 {
	return 0
}

func (af AuxField) Type() string {
	return protocol.Aux
}

// 辅助字段全部返回0
func (af AuxField) ConcreteSize() uint64 {
	return 0
}
