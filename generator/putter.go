package generator

import (
	"fmt"
	"github.com/8090Lambert/go-redis-parser/constants"
)

type Putter struct {
	writer writer
}

func NewPutter(format int) *Putter {
	if format == constants.FORMAT_JSON {
		return &Putter{Json{}}
	} else if format == constants.FORMAT_CSV {
		return &Putter{Csv{}}
	}
	panic("Unknown format")
}

func (f *Putter) Begin(version string) {

}

func (f *Putter) AuxField(key, value []byte) {
	fmt.Println(fmt.Sprintf("auxkey: %s, auxval: %s", key, value))
}

func (f *Putter) ResizeDB(dbSize, expireSize uint64) {
	// For the moment，should ignore.
	fmt.Println("resize db:", dbSize, expireSize)
}

func (f *Putter) SelectDb(index int) {
	fmt.Println(fmt.Sprintf("select db : %d", index))
}

func (f *Putter) Set(key, value []byte, expire int) {
	fmt.Println(string(key), string(value), expire)
}

func (f *Putter) HSet(key, field, value []byte, expire int) {
	fmt.Println(string(key), string(field), string(value), expire)
}

func (f *Putter) SAdd(key []byte, expire int, member ...[]byte) {
	for _, v := range member {
		fmt.Println(string(key), string(v), expire)
	}
}

func (f *Putter) List(key []byte, expire int, value ...[]byte) {
	for _, v := range value {
		fmt.Println(string(key), string(v), " expire:", expire)
	}
}

func (f *Putter) ZSet(key, value []byte, score float64) {
	fmt.Println(string(key), string(value), score)
}

func (f *Putter) LRU(lru_freq int64) {
	fmt.Println(lru_freq)
}

func (f *Putter) LFU(lfu_freq int64) {
	fmt.Println(lfu_freq)
}
