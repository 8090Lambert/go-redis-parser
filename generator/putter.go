package generator

import "github.com/8090Lambert/go-redis-parser/constants"

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

}

func (f *Putter) ResizeDB(dbSize, expireSize uint64) {
	// For the moment，should ignore.
}

func (f *Putter) SelectDb(index int) {

}

func (f *Putter) Set(key, value []byte, expire int) {

}

func (f *Putter) HSet(key, field, value []byte) {

}

func (f *Putter) SAdd(key []byte, expire int, member ...[]byte) {

}

func (f *Putter) List(key []byte, expire int, value ...[]byte) {

}

func (f *Putter) ZSet(key, value []byte, score float64) {

}
