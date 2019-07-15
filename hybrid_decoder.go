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

type decoder interface {
	init(io.Reader) error
	next() (int32, error)
}

type hybridDecoder struct {
	bitWidth     int
	unpackerFn   unpack8int32Func
	rleValueSize int

	r io.Reader

	rleCount uint32
	rleValue int32

	bpCount  uint32
	bpRunPos uint8
	bpRun    [8]int32
}

func newHybridDecoder(bitWidth int) *hybridDecoder {
	return &hybridDecoder{
		bitWidth:   bitWidth,
		unpackerFn: unpack8Int32FuncByWidth[bitWidth],

		rleValueSize: (bitWidth + 7) / 8,
	}
}

func (hd *hybridDecoder) init(r io.Reader) error {
	var size uint32
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return err
	}
	hd.r = io.LimitReader(r, int64(size))
	return nil
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
	h, err := binary.ReadUvarint(&byteReader{Reader: hd.r})
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

type constDecoder int32

func (cd constDecoder) init(io.Reader) error {
	return nil
}

func (cd constDecoder) next() (int32, error) {
	return int32(cd), nil
}
