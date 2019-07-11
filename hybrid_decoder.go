package go_parquet

// This file is based on the code from https://github.com/kostya-sh/parquet-go
// Copyright (c) 2015 Konstantin Shaposhnikov

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/pkg/errors"
)

type hybridDecoder struct {
	r  io.Reader
	br *byteReader

	bitWidth     int
	unpackerFn   unpack8int32Func
	rleValueSize int

	rleCount uint32
	rleValue int32

	bpCount  uint32
	bpRunPos uint8
	bpRun    [8]int32
}

func newHybridDecoder(r io.Reader, bitWidth int) *hybridDecoder {
	reader := &hybridDecoder{
		r:  r,
		br: &byteReader{Reader: r},

		bitWidth:   bitWidth,
		unpackerFn: unpack8Int32FuncByWidth[bitWidth],

		rleValueSize: (bitWidth + 7) / 8,
	}

	return reader
}

func (hd *hybridDecoder) next() (next int32, err error) {
	if hd.rleCount == 0 && hd.bpCount == 0 && hd.bpRunPos == 0 {
		if err = hd.readRunHeader(); err != nil {
			return 0, err
		}
	}

	switch {
	case hd.rleCount > 0:
		next = hd.rleValue
		hd.rleCount--
	case hd.bpCount > 0 || hd.bpRunPos > 0:
		if hd.bpRunPos == 0 {
			if err = hd.readBitPackedRun(); err != nil {
				return 0, err
			}
			hd.bpCount--
		}
		next = hd.bpRun[hd.bpRunPos]
		hd.bpRunPos = (hd.bpRunPos + 1) % 8
	default:
		return 0, io.EOF
	}

	return next, err
}

func (hd *hybridDecoder) readRLERunValue() error {
	v := make([]byte, hd.rleValueSize)
	n, err := hd.r.Read(v)
	if err != nil {
		return err
	}
	if n != hd.rleValueSize {
		return io.ErrUnexpectedEOF
	}

	hd.rleValue = decodeRLEValue(v)
	if bits.LeadingZeros32(uint32(hd.rleValue)) < 32-hd.bitWidth {
		return errors.New("rle: RLE run value is too large")
	}
	return nil
}

func (hd *hybridDecoder) readBitPackedRun() error {
	data := make([]byte, hd.bitWidth)
	_, err := hd.r.Read(data)
	if err != nil {
		return err
	}
	hd.bpRun = hd.unpackerFn(data)
	return nil
}

func (hd *hybridDecoder) readRunHeader() error {
	h, err := binary.ReadUvarint(hd.br)
	if err != nil || h > math.MaxUint32 {
		return errors.New("rle: invalid run header")
	}

	// The lower bit indicate if this is bitpack or rle
	if h&1 == 1 {
		hd.bpCount = uint32(h >> 1)
		if hd.bpCount == 0 {
			return fmt.Errorf("rle: empty bit-packed run")
		}
		hd.bpRunPos = 0
	} else {
		hd.rleCount = uint32(h >> 1)
		if hd.rleCount == 0 {
			return fmt.Errorf("rle: empty RLE run")
		}
		return hd.readRLERunValue()
	}
	return nil
}

func (hd *hybridDecoder) decode(data []int32) error {
	var err error
	for i := range data {
		data[i], err = hd.next()
		if err != nil {
			return err
		}
	}

	return nil
}
