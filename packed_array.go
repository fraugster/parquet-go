package goparquet

import "errors"

type packedArray struct {
	count int
	bw    int
	data  []byte

	buf    [8]int32
	bufPos int

	writer pack8int32Func
	reader unpack8int32Func
}

func (pa *packedArray) toArray() []int32 {
	if pa.bufPos != 0 {
		panic("not flushed")
	}
	ret := make([]int32, 0, pa.count)
	l := len(pa.data)
	cnt := pa.count
	for i := 0; i < l; i += pa.bw {
		a := pa.reader(pa.data[i : i+pa.bw])
		for j := range a {
			if cnt == 0 {
				break
			}
			cnt--
			ret = append(ret, a[j])
		}
	}

	return ret
}

func (pa *packedArray) reset(bw int) {
	if bw < 0 || bw > 32 {
		panic("invalid bit width")
	}
	pa.bw = bw
	pa.count = 0
	pa.bufPos = 0
	pa.data = pa.data[:0]
	pa.writer = pack8Int32FuncByWidth[bw]
	pa.reader = unpack8Int32FuncByWidth[bw]

}

func (pa *packedArray) flush() {
	for i := pa.bufPos; i < 8; i++ {
		pa.buf[i] = 0
	}
	pa.data = append(pa.data, pa.writer(pa.buf)...)
	pa.bufPos = 0
}

func (pa *packedArray) appendSingle(v int32) {
	if pa.bufPos == 8 {
		pa.flush()
	}
	pa.buf[pa.bufPos] = v
	pa.bufPos++
	pa.count++
}

func (pa *packedArray) at(pos int) (int32, error) {
	if pos < 0 || pos >= pa.count {
		return 0, errors.New("out of range")
	}
	if pa.bw == 0 {
		return 0, nil
	}
	block := (pos / 8) * pa.bw
	idx := pos % 8

	buf := pa.reader(pa.data[block : block+pa.bw])
	return buf[idx], nil
}

func (pa *packedArray) appendArray(other *packedArray) {
	if pa.bw != other.bw {
		panic("can not append array with different bit width")
	}

	if pa.bufPos != 0 || other.bufPos != 0 {
		panic("both array should be flushed")
	}

	pa.bufPos = pa.count % 8
	if pa.bufPos != 0 {
		l := len(pa.data)
		pa.buf = pa.reader(pa.data[l-pa.bw:])
		pa.data = pa.data[:l-pa.bw]
	}
	// Good
	if pa.bufPos == 0 {
		pa.data = append(pa.data, other.data...)
		pa.count += other.count
		return
	}
	// Bad
	l := len(other.data)
	if l%pa.bw != 0 {
		panic("invalid array")
	}
	cnt := other.count

	for i := 0; i < l; i += pa.bw {
		pack := pa.reader(other.data[i : i+pa.bw])
		for i := range pack {
			if cnt == 0 {
				break
			}
			cnt--
			pa.appendSingle(pack[i])
		}
	}

	pa.flush()
}
