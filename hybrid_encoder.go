package goparquet

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type hybridEncoder struct {
	w io.Writer

	left       []int32
	original   io.Writer
	bitWidth   int
	unpackerFn pack8int32Func
}

func newHybridEncoder(bitWidth int) *hybridEncoder {
	return &hybridEncoder{
		bitWidth:   bitWidth,
		unpackerFn: pack8Int32FuncByWidth[bitWidth],
	}
}

func (he *hybridEncoder) init(w io.Writer) error {
	he.w = w
	he.left = nil
	he.original = nil

	return nil
}

func (he *hybridEncoder) initSize(w io.Writer) error {
	_ = he.init(&bytes.Buffer{})
	he.original = w

	return nil
}

func (he *hybridEncoder) write(items ...[]byte) error {
	for i := range items {
		if err := writeFull(he.w, items[i]); err != nil {
			return err
		}
	}

	return nil
}

// TODO: this function is not used, since we are using the bp always.
//func (he *hybridEncoder) rleEncode(count int, value int32) error {
//	header := count << 1 // indicate this is a rle run
//
//	buf := make([]byte, 4) // big enough for int
//	cnt := binary.PutUvarint(buf, uint64(header))
//
//	return he.write(buf[:cnt], encodeRLEValue(value, he.bitWidth))
//}

func (he *hybridEncoder) bpEncode(data []int32) error {
	// If the bit width is zero, no need to write any
	if he.bitWidth == 0 {
		return nil
	}

	l := len(data)
	if l%8 != 0 {
		return errors.Errorf("bit-pack should be multiple of 8 values at a time, but it's %q", l)
	}

	res := make([]byte, 0, (l/8)*he.bitWidth)
	for i := 0; i < l; i += 8 {
		toW := [...]int32{
			data[i], data[i+1], data[i+2], data[i+3], data[i+4], data[i+5], data[i+6], data[i+7],
		}
		res = append(res, he.unpackerFn(toW)...)
	}

	header := ((l / 8) << 1) | 1
	buf := make([]byte, 4) // big enough for int
	cnt := binary.PutUvarint(buf, uint64(header))

	return he.write(buf[:cnt], res)
}

func (he *hybridEncoder) encode(data ...int32) error {
	// TODO: Not sure how to decide on the bitpack or rle, so just bp is supported
	if len(he.left) != 0 {
		// TODO: this might result in allocation and unused memory under a slice
		data = append(he.left, data...)
	}
	ln := len(data)
	// TODO: if the ln is small, just leave it for the next call
	l := ln % 8
	if l != 0 {
		he.left = data[ln-l:]
	} else {
		he.left = nil
	}

	if ln-l == 0 {
		// Nothing to write
		return nil
	}

	return he.bpEncode(data[:ln-l])
}

func (he *hybridEncoder) flush() error {
	if len(he.left) == 0 {
		return nil
	}

	l := len(he.left) % 8
	data := append(he.left, make([]int32, 8-l)...)
	he.left = nil
	return he.bpEncode(data)
}

func (he *hybridEncoder) Close() error {
	if err := he.flush(); err != nil {
		return err
	}

	if he.original != nil {
		data := he.w.(*bytes.Buffer).Bytes()
		var size = uint32(len(data))
		if err := binary.Write(he.original, binary.LittleEndian, size); err != nil {
			return err
		}
		return writeFull(he.original, data)
	}

	return nil
}
