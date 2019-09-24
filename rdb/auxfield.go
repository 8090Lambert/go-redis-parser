package rdb

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
)

type AuxField struct {
	Key   interface{}
	Value interface{}
}

func (af AuxField) String() string {
	return fmt.Sprintf("{Aux: {Key: %s, Value: %s}}", ToString(af.Key), ToString(af.Value))
}

func (af AuxField) Type() protocol.DataType {
	return protocol.Aux
}

func (r *ParseRdb) AuxFields(key, val []byte) string {
	aux := AuxField{Key: key, Value: val}
	return aux.String()
}
