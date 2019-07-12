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
	next(*hybridContext) (int32, error)
}

type hybridContext struct {
	r io.Reader

	rleCount uint32
	rleValue int32

	bpCount  uint32
	bpRunPos uint8
	bpRun    [8]int32
}

type hybridDecoder struct {
	bitWidth     int
	unpackerFn   unpack8int32Func
	rleValueSize int
}

func newHybridContext(r io.Reader) *hybridContext {
	return &hybridContext{
		r: r,
	}
}

func newHybridDecoder(bitWidth int) *hybridDecoder {
	return &hybridDecoder{
		bitWidth:   bitWidth,
		unpackerFn: unpack8Int32FuncByWidth[bitWidth],

		rleValueSize: (bitWidth + 7) / 8,
	}
}

func (hd *hybridDecoder) next(ctx *hybridContext) (next int32, err error) {
	if ctx == nil {
		return 0, errors.Errorf("invalid context")
	}

	if ctx.rleCount == 0 && ctx.bpCount == 0 && ctx.bpRunPos == 0 {
		if err = hd.readRunHeader(ctx); err != nil {
			return 0, err
		}
	}

	switch {
	case ctx.rleCount > 0:
		next = ctx.rleValue
		ctx.rleCount--
	case ctx.bpCount > 0 || ctx.bpRunPos > 0:
		if ctx.bpRunPos == 0 {
			if err = hd.readBitPackedRun(ctx); err != nil {
				return 0, err
			}
			ctx.bpCount--
		}
		next = ctx.bpRun[ctx.bpRunPos]
		ctx.bpRunPos = (ctx.bpRunPos + 1) % 8
	default:
		return 0, io.EOF
	}

	return next, err
}

func (hd *hybridDecoder) readRLERunValue(ctx *hybridContext) error {
	v := make([]byte, hd.rleValueSize)
	n, err := ctx.r.Read(v)
	if err != nil {
		return err
	}
	if n != hd.rleValueSize {
		return io.ErrUnexpectedEOF
	}

	ctx.rleValue = decodeRLEValue(v)
	if bits.LeadingZeros32(uint32(ctx.rleValue)) < 32-hd.bitWidth {
		return errors.New("rle: RLE run value is too large")
	}
	return nil
}

func (hd *hybridDecoder) readBitPackedRun(ctx *hybridContext) error {
	data := make([]byte, hd.bitWidth)
	_, err := ctx.r.Read(data)
	if err != nil {
		return err
	}
	ctx.bpRun = hd.unpackerFn(data)
	return nil
}

func (hd *hybridDecoder) readRunHeader(ctx *hybridContext) error {
	h, err := binary.ReadUvarint(&byteReader{Reader: ctx.r})
	if err != nil || h > math.MaxUint32 {
		return errors.New("rle: invalid run header")
	}

	// The lower bit indicate if this is bitpack or rle
	if h&1 == 1 {
		ctx.bpCount = uint32(h >> 1)
		if ctx.bpCount == 0 {
			return fmt.Errorf("rle: empty bit-packed run")
		}
		ctx.bpRunPos = 0
	} else {
		ctx.rleCount = uint32(h >> 1)
		if ctx.rleCount == 0 {
			return fmt.Errorf("rle: empty RLE run")
		}
		return hd.readRLERunValue(ctx)
	}
	return nil
}

type constDecoder int32

func (cd constDecoder) next(ctx *hybridContext) (next int32, err error) {
	if ctx != nil {
		return 0, errors.Errorf("invalid context, it should be nil")
	}
	return int32(cd), nil
}
