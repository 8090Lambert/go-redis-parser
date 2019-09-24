package rdb

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"time"
)

type KeyObject struct {
	Key    interface{}
	Expire time.Time
}

func NewKeyObject(key interface{}, expire int64) KeyObject {
	k := KeyObject{Key: key}

	if expire > 0 {
		k.Expire = time.Unix(expire/1000, 0).UTC()
	}

	return k
}

// Whether the key has expired until now.
func (k KeyObject) Expired() bool {
	return k.Expire.Before(time.Now())
}

func (k KeyObject) Type() protocol.DataType {
	return protocol.Key
}

func (k KeyObject) String() string {
	if !k.Expire.IsZero() {
		return fmt.Sprintf("{ExpiryTime: %s, Key: %s}", k.Expire, ToString(k.Key))
	}

	return fmt.Sprintf("%s", ToString(k.Key))
}
