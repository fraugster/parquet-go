package go_parquet

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"sync"

	"github.com/golang/snappy"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

var (
	compressors    = make(map[parquet.CompressionCodec]BlockCompressor)
	compressorLock sync.RWMutex
)

// BlockCompressor is an interface for handling the compressors for the parquet file
type (
	BlockCompressor interface {
		Reader(reader io.Reader, expected int32) (io.Reader, error)
	}

	plainCompressor  struct{}
	snappyCompressor struct{}
	gzipCompressor   struct{}
)

func (gzipCompressor) Reader(reader io.Reader, expected int32) (io.Reader, error) {
	r, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}

	ret, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	if len(ret) != int(expected) {
		return nil, errors.Errorf("gzip: decompress size is not correct it should be %d is %d", expected, len(ret))
	}

	return bytes.NewReader(ret), nil
}

func (snappyCompressor) Reader(reader io.Reader, expected int32) (io.Reader, error) {
	// Snappy compressor is not an stream compressor (the block compressor is different than stream compressor and parquet uses
	// block compressor)
	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	ret, err := snappy.Decode(nil, buf)
	if err != nil {
		return nil, err
	}

	if len(ret) != int(expected) {
		return nil, errors.Errorf("snappy: decompress size is not correct it should be %d is %d", expected, len(ret))
	}

	return bytes.NewReader(ret), nil
}

func (plainCompressor) Reader(reader io.Reader, expected int32) (io.Reader, error) {
	ret, err := ioutil.ReadAll(io.LimitReader(reader, int64(expected)))
	if err != nil {
		return nil, err
	}

	if len(ret) != int(expected) {
		return nil, errors.Errorf("plain: stream size is not correct it should be %d is %d", expected, len(ret))
	}

	return bytes.NewReader(ret), nil
}

// RegisterBlockCompressor can plug new kind of block compressor to the library
func RegisterBlockCompressor(method parquet.CompressionCodec, compressor BlockCompressor) {
	compressorLock.Lock()
	defer compressorLock.Unlock()

	compressors[method] = compressor
}

func getBlockCompressor(method parquet.CompressionCodec) BlockCompressor {
	compressorLock.RLock()
	defer compressorLock.RUnlock()

	return compressors[method]
}

func newBlockReader(in io.Reader, codec parquet.CompressionCodec, compressedSize int32, uncompressedSize int32) (io.Reader, error) {
	bc := getBlockCompressor(codec)
	if bc == nil {
		return nil, errors.Errorf("the codec %q is not implemented", codec)
	}

	lr := io.LimitReader(in, int64(compressedSize))
	return bc.Reader(lr, uncompressedSize)
}

func init() {
	RegisterBlockCompressor(parquet.CompressionCodec_UNCOMPRESSED, plainCompressor{})
	RegisterBlockCompressor(parquet.CompressionCodec_GZIP, gzipCompressor{})
	RegisterBlockCompressor(parquet.CompressionCodec_SNAPPY, snappyCompressor{})
}
