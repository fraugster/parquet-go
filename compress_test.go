package goparquet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/fraugster/parquet-go/parquet"
)

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
		b, err := compressBlock(block, m)
		require.NoError(t, err)
		b2, err := decompressBlock(b, m)
		require.NoError(t, err)
		assert.Equal(t, block, b2)
	}
}
