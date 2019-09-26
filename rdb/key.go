package rdb

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"time"
)

type KeyObject struct {
	Field  interface{}
	Expire time.Time
}

func NewKeyObject(key interface{}, expire int64) KeyObject {
	k := KeyObject{Field: key}

	if expire > 0 {
		k.Expire = time.Unix(expire/1000, 0).UTC()
	}

	return k
}

// Whether the key has expired until now.
func (k KeyObject) Expired() bool {
	return k.Expire.Before(time.Now())
}

func (k KeyObject) Type() string {
	return protocol.Key
}

func (k KeyObject) String() string {
	if !k.Expire.IsZero() {
		return fmt.Sprintf("{ExpiryTime: %s, Key: %s}", k.Expire, k.Value())
	}

	return fmt.Sprintf("%s", k.Value())
}

func (k KeyObject) Key() string {
	return ""
}

func (k KeyObject) Value() string {
	return ToString(k.Field)
}

func (k KeyObject) ValueLen() uint64 {
	return k.ConcreteSize()
}

// 暂时返回key的长度
func (k KeyObject) ConcreteSize() uint64 {
	return uint64(len([]byte(k.Value())))
}
