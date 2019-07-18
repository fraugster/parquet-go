package go_parquet

import (
	"io"

	"github.com/pkg/errors"
)

type deltaBitPackDecoder struct {
	bitWidth uint8
	r        io.Reader

	numMiniBlocks int32
	miniBlockSize int32
	numValues     int32

	minDelta        interface{}
	miniBlockWidths []uint8

	position          int
	value             interface{}
	miniBlock         int
	miniBlockWidth    int
	unpacker32        unpack8int32Func
	unpacker64        unpack8int64Func
	miniBlockPos      int
	miniBlockValues32 [8]int32
	miniBlockValues64 [8]int64
}

func (d *deltaBitPackDecoder) init(r io.Reader) error {
	if d.bitWidth != 32 && d.bitWidth != 64 {
		return errors.New("int/delta: invalid bitwidth")
	}
	d.r = r

	if err := d.readPageHeader(); err != nil {
		return err
	}
	if err := d.readBlockHeader(); err != nil {
		return err
	}

	return nil
}

// there is no need for implement the init size here
func (d *deltaBitPackDecoder) initSize(r io.Reader) error {
	return d.init(r)
}

// page-header := <block size in values> <number of miniblocks in a block> <total value count> <first value>
func (d *deltaBitPackDecoder) readPageHeader() error {
	blockSize, err := readUVariant32(d.r)
	if err != nil {
		return errors.Wrap(err, "int/delta: failed to read block size")
	}
	if blockSize <= 0 {
		// TODO: maybe validate blockSize % 8 = 0
		return errors.New("int/delta: invalid block size")
	}

	d.numMiniBlocks, err = readUVariant32(d.r)
	if err != nil {
		return errors.Wrap(err, "int/delta: failed to read number of mini blocks")
	}

	if d.numMiniBlocks <= 0 || d.numMiniBlocks > blockSize || blockSize%d.numMiniBlocks != 0 {
		// TODO: maybe blockSize/8 % d.numMiniBlocks = 0
		return errors.New("int/delta: invalid number of mini blocks")
	}

	d.numValues, err = readUVariant32(d.r)
	if err != nil {
		return errors.Wrapf(err, "int/delta: failed to read total value count")
	}
	if d.numValues < 0 {
		return errors.New("int/delta: invalid total value count")
	}

	if d.bitWidth == 32 {
		d.value, err = readVariant32(d.r)
	} else {
		d.value, err = readVariant64(d.r)
	}
	if err != nil {
		return errors.Wrap(err, "int/delta: failed to read first value")
	}

	// TODO: re-use if possible
	d.miniBlockWidths = make([]byte, d.numMiniBlocks)
	d.miniBlockSize = blockSize / d.numMiniBlocks

	return nil
}

// block := <min delta> <list of bitwidths of miniblocks> <miniblocks>
// min delta : zig-zag var int encoded
// bitWidthsOfMiniBlock : 1 byte little endian
func (d *deltaBitPackDecoder) readBlockHeader() error {
	var err error

	if d.bitWidth == 32 {
		d.minDelta, err = readVariant32(d.r)
	} else {
		d.minDelta, err = readVariant64(d.r)
	}
	if err != nil {
		return errors.Wrap(err, "int/delta: failed to read min delta")
	}

	if _, err = io.ReadFull(d.r, d.miniBlockWidths); err != nil {
		return errors.Wrap(err, "int/delta: not enough data to read all miniblock bit widths")
	}

	for i := range d.miniBlockWidths {
		if d.miniBlockWidths[i] < 0 || d.miniBlockWidths[i] > d.bitWidth {
			return errors.New("int/delta: invalid miniblock bit width")
		}
	}

	d.miniBlock = 0

	return nil
}

func (d *deltaBitPackDecoder) nextInterface() (interface{}, error) {
	if d.position >= int(d.numValues) {
		return 0, io.EOF
	}

	if d.position%8 == 0 {
		if d.position%int(d.miniBlockSize) == 0 {
			if d.miniBlock >= int(d.numMiniBlocks) {
				if err := d.readBlockHeader(); err != nil {
					return 0, err
				}
			}

			d.miniBlockWidth = int(d.miniBlockWidths[d.miniBlock])
			if d.bitWidth == 32 {
				d.unpacker32 = unpack8Int32FuncByWidth[d.miniBlockWidth]
			} else {
				d.unpacker64 = unpack8Int64FuncByWidth[d.miniBlockWidth]
			}
			d.miniBlockPos = 0
			d.miniBlock++
		}

		// read next 8 values
		w := d.miniBlockWidth
		buf := make([]byte, d.miniBlockWidth)
		_, err := io.ReadFull(d.r, buf)
		if err != nil {
			return 0, err
		}

		if d.bitWidth == 32 {
			d.miniBlockValues32 = d.unpacker32(buf)
			d.miniBlockPos += w
			if d.position+8 >= int(d.numValues) {
				buf := make([]byte, int(d.miniBlockSize)/8*w-d.miniBlockPos)
				// make sure that all data is consumed
				// this is needed for byte array decoders
				// TODO: (f0rud) What is this code here?
				_, _ = d.r.Read(buf)
			}
		} else {
			d.miniBlockValues64 = d.unpacker64(buf)
			d.miniBlockPos += w
			if d.position+8 >= int(d.numValues) {
				buf := make([]byte, int(d.miniBlockSize)/8*w-d.miniBlockPos)
				// make sure that all data is consumed
				// this is needed for byte array decoders
				// TODO: (f0rud) What is this code here?
				_, _ = d.r.Read(buf)
			}
		}
	}

	var ret interface{}
	if d.bitWidth == 32 {
		ret = d.value
		d.value = ret.(int32) + d.miniBlockValues32[d.position%8] + d.minDelta.(int32)
	} else {
		ret = d.value
		d.value = ret.(int64) + d.miniBlockValues64[d.position%8] + d.minDelta.(int64)
	}
	d.position++

	return ret, nil
}

func (d *deltaBitPackDecoder) next() (int32, error) {
	if d.bitWidth != 32 {
		return 0, errors.New("32 bit iteration over 64 bit decoder")
	}

	n, err := d.nextInterface()
	if err != nil {
		return 0, err
	}

	return n.(int32), nil
}
