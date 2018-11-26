package binary

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"reflect"
	"unsafe"
)

type Encoder struct {
	Output  io.Writer
	scratch [binary.MaxVarintLen64]byte
}

func (enc *Encoder) Int32(v int32) error {
	return enc.uint32(uint32(v))
}

func (enc *Encoder) Float64(v float64) error {
	return enc.uint64(math.Float64bits(v))
}

func (enc *Encoder) Uint8(v uint8) error {
	enc.scratch[0] = v
	if _, err := enc.Output.Write(enc.scratch[:1]); err != nil {
		return err
	}
	return nil
}

var ErrStringTooLong = errors.New("string too long")

func (enc *Encoder) String(str string) error {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&str))
	header.Len = len(str)
	header.Cap = header.Len
	if header.Len > math.MaxUint8 {
		return ErrStringTooLong
	}
	if err := enc.Uint8(uint8(header.Len)); err != nil {
		return err
	}
	if _, err := enc.Output.Write(*(*[]byte)(unsafe.Pointer(header))); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) uint64(v uint64) error {
	enc.scratch[0] = byte(v)
	enc.scratch[1] = byte(v >> 8)
	enc.scratch[2] = byte(v >> 16)
	enc.scratch[3] = byte(v >> 24)
	enc.scratch[4] = byte(v >> 32)
	enc.scratch[5] = byte(v >> 40)
	enc.scratch[6] = byte(v >> 48)
	enc.scratch[7] = byte(v >> 56)
	if _, err := enc.Output.Write(enc.scratch[:8]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) uint32(v uint32) error {
	enc.scratch[0] = byte(v)
	enc.scratch[1] = byte(v >> 8)
	enc.scratch[2] = byte(v >> 16)
	enc.scratch[3] = byte(v >> 24)
	if _, err := enc.Output.Write(enc.scratch[:4]); err != nil {
		return err
	}
	return nil
}
