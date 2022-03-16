package goparquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type deltaBitPackEncoder[T intType, I internalIntType[T]] struct {
	impl I

	deltas   []T
	bitWidth []uint8
	packed   [][]byte
	w        io.Writer

	// this value should be there before the init
	blockSize      int // Must be multiple of 128
	miniBlockCount int // blockSize % miniBlockCount should be 0

	miniBlockValueCount int

	valuesCount int
	buffer      *bytes.Buffer

	firstValue    T // the first value to write
	minDelta      T
	previousValue T
}

func (d *deltaBitPackEncoder[T, I]) init(w io.Writer) error {
	d.w = w

	if d.blockSize%128 != 0 || d.blockSize <= 0 {
		return fmt.Errorf("invalid block size, it should be multiply of 128, it is %d", d.blockSize)
	}

	if d.miniBlockCount <= 0 || d.blockSize%d.miniBlockCount != 0 {
		return fmt.Errorf("invalid mini block count, it is %d", d.miniBlockCount)
	}

	d.miniBlockValueCount = d.blockSize / d.miniBlockCount
	if d.miniBlockValueCount%8 != 0 {
		return fmt.Errorf("invalid mini block count, the mini block value count should be multiply of 8, it is %d", d.miniBlockCount)
	}

	d.firstValue = 0
	d.valuesCount = 0
	d.minDelta = d.impl.MaxValue()
	d.deltas = make([]T, 0, d.blockSize)
	d.previousValue = 0
	d.buffer = &bytes.Buffer{}
	d.bitWidth = make([]uint8, 0, d.miniBlockCount)
	return nil
}

func (d *deltaBitPackEncoder[T, I]) flush() error {
	// Technically, based on the spec after this step all values are positive, but NO, it's not. the problem is when
	// the min delta is small enough (lets say MinInt) and one of deltas are MaxInt, the the result of MaxInt-MinInt is
	// -1, get the idea, there is a lot of numbers here because of overflow can produce negative value
	for i := range d.deltas {
		d.deltas[i] -= d.minDelta
	}

	if err := writeVariant(d.buffer, int64(d.minDelta)); err != nil {
		return err
	}

	d.bitWidth, d.packed = d.impl.PackDeltas(d.deltas, d.miniBlockValueCount)

	for len(d.bitWidth) < d.miniBlockCount {
		d.bitWidth = append(d.bitWidth, 0)
	}

	if err := binary.Write(d.buffer, binary.LittleEndian, d.bitWidth); err != nil {
		return err
	}

	for i := range d.packed {
		if err := writeFull(d.buffer, d.packed[i]); err != nil {
			return err
		}
	}

	d.minDelta = d.impl.MaxValue()
	d.deltas = d.deltas[:0]

	return nil
}

func (d *deltaBitPackEncoder[T, I]) addValue(i T) error {
	d.valuesCount++
	if d.valuesCount == 1 {
		d.firstValue = i
		d.previousValue = i
		return nil
	}

	delta := i - d.previousValue
	d.previousValue = i
	d.deltas = append(d.deltas, delta)
	if delta < d.minDelta {
		d.minDelta = delta
	}

	if len(d.deltas) == d.blockSize {
		// flush
		return d.flush()
	}

	return nil
}

func (d *deltaBitPackEncoder[T, I]) write() error {
	if d.valuesCount == 1 || len(d.deltas) > 0 {
		if err := d.flush(); err != nil {
			return err
		}
	}

	if err := writeUVariant(d.w, uint64(d.blockSize)); err != nil {
		return err
	}

	if err := writeUVariant(d.w, uint64(d.miniBlockCount)); err != nil {
		return err
	}

	if err := writeUVariant(d.w, uint64(d.valuesCount)); err != nil {
		return err
	}

	if err := writeVariant(d.w, int64(d.firstValue)); err != nil {
		return err
	}

	return writeFull(d.w, d.buffer.Bytes())
}

func (d *deltaBitPackEncoder[T, I]) Close() error {
	return d.write()
}

func (d *deltaBitPackEncoder[T, I]) encodeValues(values []interface{}) error {
	for i := range values {
		if err := d.addValue(values[i].(T)); err != nil {
			return err
		}
	}

	return nil
}
