package go_parquet

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"testing"

	"github.com/golang/snappy"

	"github.com/stretchr/testify/assert"
	"github.com/fraugster/parquet-go/parquet"
)

func compressBlock(in []byte, method parquet.CompressionCodec) []byte {
	switch method {
	case parquet.CompressionCodec_UNCOMPRESSED:
		ret := make([]byte, len(in))
		copy(ret, in)
		return ret
	case parquet.CompressionCodec_GZIP:
		buf := &bytes.Buffer{}
		cmp := gzip.NewWriter(buf)
		if _, err := cmp.Write(in); err != nil {
			panic(err)
		}
		if err := cmp.Close(); err != nil {
			panic(err)
		}

		return buf.Bytes()
	case parquet.CompressionCodec_SNAPPY:
		return snappy.Encode(nil, in)
	}

	panic("invalid method")
}

func TestCompressor(t *testing.T) {
	block := []byte(`lorem ipsum dolor sit amet, consectetur adipiscing elit, 
sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut 
aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in 
voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint 
occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.`)

	methods := []parquet.CompressionCodec{
		parquet.CompressionCodec_GZIP,
		parquet.CompressionCodec_SNAPPY,
		parquet.CompressionCodec_UNCOMPRESSED,
	}

	for _, m := range methods {
		b := compressBlock(block, m)
		r, err := newBlockReader(bytes.NewReader(b), m, int32(len(b)), int32(len(block)))
		assert.NoError(t, err)
		buf, err := ioutil.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, buf, block)
	}
}
