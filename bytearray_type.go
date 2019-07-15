package go_parquet

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type byteArrayPlainDecoder struct {
	r io.Reader
	// if the length is set, then this is a fix size array decoder, unless it reads the len first
	length int
}

func (b *byteArrayPlainDecoder) init(r io.Reader) error {
	b.r = r
	return nil
}

func (b *byteArrayPlainDecoder) next() ([]byte, error) {
	var l = int32(b.length)
	if l == 0 {
		if err := binary.Read(b.r, binary.LittleEndian, &l); err != nil {
			return nil, err
		}
	}

	buf := make([]byte, l)
	_, err := io.ReadFull(b.r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (b *byteArrayPlainDecoder) decodeValues(dst []interface{}) (err error) {
	for i := range dst {
		if dst[i], err = b.next(); err != nil {
			return
		}
	}
	return nil
}

type byteArrayDeltaLengthDecoder struct {
	r        io.Reader
	position int
	lens     []int32
}

func (b *byteArrayDeltaLengthDecoder) init(r io.Reader) error {
	b.r = r
	b.position = 0
	lensDecoder := deltaBitPackDecoder{bitWidth: 32}
	if err := lensDecoder.init(r); err != nil {
		return err
	}

	b.lens = make([]int32, lensDecoder.numValues)
	return decodeInt32(&lensDecoder, b.lens)
}

func (b *byteArrayDeltaLengthDecoder) next() (value []byte, err error) {
	if b.position >= len(b.lens) {
		return nil, io.ErrUnexpectedEOF
	}
	size := int(b.lens[b.position])
	value = make([]byte, size)
	if _, err := io.ReadFull(b.r, value); err != nil {
		return nil, err
	}
	b.position++

	return value, err
}

func (b *byteArrayDeltaLengthDecoder) decodeValues(dst []interface{}) error {
	for i := 0; i < len(dst); i++ {
		v, err := b.next()
		if err != nil {
			return err
		}
		dst[i] = v
	}
	return nil
}

type byteArrayDeltaDecoder struct {
	suffixDecoder byteArrayDeltaLengthDecoder

	prefixLens []int32

	value []byte
}

func (d *byteArrayDeltaDecoder) init(r io.Reader) error {
	lensDecoder := deltaBitPackDecoder{bitWidth: 32}
	if err := lensDecoder.init(r); err != nil {
		return err
	}

	d.prefixLens = make([]int32, lensDecoder.numValues)
	if err := decodeInt32(&lensDecoder, d.prefixLens); err != nil {
		return err
	}
	if err := d.suffixDecoder.init(r); err != nil {
		return err
	}

	if len(d.prefixLens) != len(d.suffixDecoder.lens) {
		return errors.New("bytearray/delta: different number of suffixes and prefixes")
	}

	d.value = make([]byte, 0)

	return nil
}

func (d *byteArrayDeltaDecoder) decodeValues(dst []interface{}) error {
	for i := 0; i < len(dst); i++ {
		suffix, err := d.suffixDecoder.next()
		if err != nil {
			return err
		}
		prefixLen := int(d.prefixLens[d.suffixDecoder.position-1])
		value := make([]byte, 0, prefixLen+len(suffix))
		value = append(value, d.value[:prefixLen]...)
		value = append(value, suffix...)
		d.value = value
		dst[i] = value
	}
	return nil
}
