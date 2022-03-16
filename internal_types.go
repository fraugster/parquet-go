package goparquet

import (
	"encoding/binary"
	"io"
	"math"
	"math/bits"

	"github.com/fraugster/parquet-go/parquet"
)

type numberType interface {
	floatType | intType
}

type intType interface {
	int32 | int64
}

type internalIntType[T intType] interface {
	internalType[T]
	PackDeltas(deltas []T, miniBlockValueCount int) (bitWidths []uint8, packedData [][]byte)
	GetUnpacker(bw int) func([]byte) [8]T
}

type floatType interface {
	float32 | float64
}

type internalNumberType[T numberType] interface {
	internalType[T]
}

type internalType[T numberType] interface {
	MaxValue() T
	MinValue() T
	ParquetType() parquet.Type
	ToBytes(v T) []byte
	EncodeBinaryValues(w io.Writer, values []interface{}) error
	DecodeBinaryValues(r io.Reader, dst []interface{}) (int, error)
	Sizeof() int
}

type internalFloat32 struct{}

func (f internalFloat32) MinValue() float32 {
	return -math.MaxFloat32
}

func (f internalFloat32) MaxValue() float32 {
	return math.MaxFloat32
}

func (f internalFloat32) ParquetType() parquet.Type {
	return parquet.Type_FLOAT
}

func (f internalFloat32) ToBytes(v float32) []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, math.Float32bits(float32(v)))
	return ret
}

func (f internalFloat32) EncodeBinaryValues(w io.Writer, values []interface{}) error {
	data := make([]uint32, len(values))
	for i := range values {
		data[i] = math.Float32bits(values[i].(float32))
	}

	return binary.Write(w, binary.LittleEndian, data)
}

func (f internalFloat32) DecodeBinaryValues(r io.Reader, dst []interface{}) (int, error) {
	var data uint32
	for i := range dst {
		if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dst[i] = math.Float32frombits(data)
	}
	return len(dst), nil
}

func (f internalFloat32) Sizeof() int {
	return 4
}

type internalFloat64 struct{}

func (f internalFloat64) MinValue() float64 {
	return -math.MaxFloat64
}

func (f internalFloat64) MaxValue() float64 {
	return math.MaxFloat64
}

func (f internalFloat64) ParquetType() parquet.Type {
	return parquet.Type_DOUBLE
}

func (f internalFloat64) ToBytes(v float64) []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, math.Float64bits(float64(v)))
	return ret
}

func (f internalFloat64) EncodeBinaryValues(w io.Writer, values []interface{}) error {
	data := make([]uint64, len(values))
	for i := range values {
		data[i] = math.Float64bits(values[i].(float64))
	}

	return binary.Write(w, binary.LittleEndian, data)
}

func (f internalFloat64) DecodeBinaryValues(r io.Reader, dst []interface{}) (int, error) {
	var data uint64
	for i := range dst {
		if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dst[i] = math.Float64frombits(data)
	}
	return len(dst), nil
}

func (f internalFloat64) Sizeof() int {
	return 8
}

type internalInt32 struct{}

func (i internalInt32) MinValue() int32 {
	return math.MinInt32
}

func (i internalInt32) MaxValue() int32 {
	return math.MaxInt32
}

func (i internalInt32) ParquetType() parquet.Type {
	return parquet.Type_INT32
}

func (i internalInt32) ToBytes(v int32) []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, uint32(v))
	return ret
}

func (i internalInt32) EncodeBinaryValues(w io.Writer, values []interface{}) error {
	d := make([]int32, len(values))
	for j := range values {
		d[j] = values[j].(int32)
	}
	return binary.Write(w, binary.LittleEndian, d)
}

func (i internalInt32) DecodeBinaryValues(r io.Reader, dst []interface{}) (int, error) {
	var d int32
	for idx := range dst {
		if err := binary.Read(r, binary.LittleEndian, &d); err != nil {
			return idx, err
		}
		dst[idx] = d
	}
	return len(dst), nil
}

func (i internalInt32) PackDeltas(deltas []int32, miniBlockValueCount int) (bitWidths []uint8, packedData [][]byte) {
	for i := 0; i < len(deltas); i += miniBlockValueCount {
		end := i + miniBlockValueCount
		if end >= len(deltas) {
			end = len(deltas)
		}
		var bw int
		buf := make([][8]int32, miniBlockValueCount/8)

		// The cast to uint32 here, is the key. or the max not works at all
		max := uint32(deltas[i])
		for j := i; j < end; j++ {
			if max < uint32(deltas[j]) {
				max = uint32(deltas[j])
			}
			t := j - i
			buf[t/8][t%8] = deltas[j]
		}
		bw = bits.Len32(uint32(max))

		bitWidths = append(bitWidths, uint8(bw))
		data := make([]byte, 0, bw*len(buf))
		packerFunc := pack8Int32FuncByWidth[bw]
		for j := range buf {
			data = append(data, packerFunc(buf[j])...)
		}
		packedData = append(packedData, data)
	}
	return
}

func (f internalInt32) Sizeof() int {
	return 4
}

func (f internalInt32) GetUnpacker(bw int) func([]byte) [8]int32 {
	return unpack8Int32FuncByWidth[bw]
}

type internalInt64 struct{}

func (i internalInt64) MinValue() int64 {
	return math.MinInt64
}

func (i internalInt64) MaxValue() int64 {
	return math.MaxInt64
}

func (i internalInt64) ParquetType() parquet.Type {
	return parquet.Type_INT64
}

func (i internalInt64) ToBytes(v int64) []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, uint64(v))
	return ret
}

func (i internalInt64) EncodeBinaryValues(w io.Writer, values []interface{}) error {
	d := make([]int64, len(values))
	for j := range values {
		d[j] = values[j].(int64)
	}
	return binary.Write(w, binary.LittleEndian, d)
}

func (i internalInt64) DecodeBinaryValues(r io.Reader, dst []interface{}) (int, error) {
	var d int64
	for idx := range dst {
		if err := binary.Read(r, binary.LittleEndian, &d); err != nil {
			return idx, err
		}
		dst[idx] = d
	}
	return len(dst), nil
}

func (i internalInt64) PackDeltas(deltas []int64, miniBlockValueCount int) (bitWidths []uint8, packedData [][]byte) {
	for i := 0; i < len(deltas); i += miniBlockValueCount {
		end := i + miniBlockValueCount
		if end >= len(deltas) {
			end = len(deltas)
		}
		var bw int
		buf := make([][8]int64, miniBlockValueCount/8)

		// The cast to uint64 here, is the key. or the max not works at all
		max := uint64(deltas[i])
		for j := i; j < end; j++ {
			if max < uint64(deltas[j]) {
				max = uint64(deltas[j])
			}
			t := j - i
			buf[t/8][t%8] = deltas[j]
		}
		bw = bits.Len64(uint64(max))

		bitWidths = append(bitWidths, uint8(bw))
		data := make([]byte, 0, bw*len(buf))
		packerFunc := pack8Int64FuncByWidth[bw]
		for j := range buf {
			data = append(data, packerFunc(buf[j])...)
		}
		packedData = append(packedData, data)
	}
	return
}

func (f internalInt64) Sizeof() int {
	return 8
}

func (f internalInt64) GetUnpacker(bw int) func([]byte) [8]int64 {
	return unpack8Int64FuncByWidth[bw]
}
