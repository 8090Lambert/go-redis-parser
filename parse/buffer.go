package parse

import (
	"errors"
	"io"
)

type buffer struct {
	data  []byte
	index int
}

func newBuffer(data []byte) *buffer {
	return &buffer{
		data: data,
	}
}

func (buf *buffer) Slice(n int) ([]byte, error) {
	if buf.index+n > len(buf.data) {
		return nil, io.EOF
	}
	b := buf.data[buf.index : buf.index+n]
	buf.index = buf.index + n
	return b, nil
}

func (buf *buffer) ReadByte() (byte, error) {
	if buf.index >= len(buf.data) {
		return 0, io.EOF
	}
	b := buf.data[buf.index]
	buf.index++
	return b, nil
}

func (buf *buffer) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if buf.index >= len(buf.data) {
		return 0, io.EOF
	}
	n := copy(b, buf.data[buf.index:])
	buf.index = buf.index + n
	return n, nil
}

func (buf *buffer) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = int64(buf.index) + offset
	case 2:
		abs = int64(len(buf.data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("negative position")
	}
	if abs >= 1<<31 {
		return 0, errors.New("position out of range")
	}
	buf.index = int(abs)
	return abs, nil
}
