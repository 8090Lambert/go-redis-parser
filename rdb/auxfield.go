package rdb

import "fmt"

type AuxField struct {
	Key   interface{}
	Value interface{}
}

func (af AuxField) String() string {
	return fmt.Sprintf("{Aux: {Key: %s, Value: %s}}", ToString(af.Key), ToString(af.Value))
}

func (r *ParseRdb) AuxFields(key, val []byte) string {
	aux := AuxField{Key: key, Value: val}
	return aux.String()
}
