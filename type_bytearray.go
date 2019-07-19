package go_parquet

import (
	"bytes"
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

		if l < 0 {
			return nil, errors.New("bytearray/plain: len is negative")
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

type byteArrayPlainEncoder struct {
	w io.Writer

	length int
}

func (b *byteArrayPlainEncoder) init(w io.Writer) error {
	b.w = w

	return nil
}

func (b *byteArrayPlainEncoder) writeBytes(data []byte) error {
	l := b.length
	if l == 0 { // variable length
		l = len(data)
		l32 := int32(l)
		if err := binary.Write(b.w, binary.LittleEndian, l32); err != nil {
			return err
		}
	} else if len(data) != l {
		return errors.Errorf("the byte array should be with length %d but is %d", l, len(data))
	}

	return writeFull(b.w, data)
}

func (b *byteArrayPlainEncoder) encodeValues(values []interface{}) error {
	for i := range values {
		if err := b.writeBytes(values[i].([]byte)); err != nil {
			return err
		}
	}

	return nil
}

func (*byteArrayPlainEncoder) Close() error {
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
	lensDecoder := int32DeltaBPDecoder{}
	if err := lensDecoder.init(r); err != nil {
		return err
	}

	b.lens = make([]int32, lensDecoder.valuesCount)
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

type byteArrayDeltaLengthEncoder struct {
	w    io.Writer
	buf  *bytes.Buffer
	lens []interface{}
}

func (b *byteArrayDeltaLengthEncoder) init(w io.Writer) error {
	b.w = w
	b.buf = &bytes.Buffer{}
	return nil
}

func (b *byteArrayDeltaLengthEncoder) encodeValues(values []interface{}) error {
	if b.lens == nil {
		// this is just for the first time, maybe we need to copy and increase the cap in the next calls?
		b.lens = make([]interface{}, 0, len(values))
	}
	for i := range values {
		data := values[i].([]byte)
		b.lens = append(b.lens, int32(len(data)))
		if err := writeFull(b.buf, data); err != nil {
			return err
		}
	}

	return nil
}

func (b *byteArrayDeltaLengthEncoder) Close() error {
	// Do we need to change this values? (128 and 4)
	enc := &int32DeltaBPEncoder{
		deltaBitPackEncoder32{
			blockSize:      128,
			miniBlockCount: 4,
		},
	}
	if err := enc.init(b.w); err != nil {
		return err
	}

	if err := enc.encodeValues(b.lens); err != nil {
		return err
	}

	if err := enc.Close(); err != nil {
		return err
	}

	return writeFull(b.w, b.buf.Bytes())
}

type byteArrayDeltaDecoder struct {
	suffixDecoder byteArrayDeltaLengthDecoder

	prefixLens []int32

	value []byte
}

func (d *byteArrayDeltaDecoder) init(r io.Reader) error {
	lensDecoder := deltaBitPackDecoder32{}
	if err := lensDecoder.init(r); err != nil {
		return err
	}

	d.prefixLens = make([]int32, lensDecoder.valuesCount)
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
