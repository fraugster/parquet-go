package go_parquet

import (
	"bytes"
	"compress/gzip"
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
		CompressBlock([]byte) ([]byte, error)
		DecompressBlock([]byte) ([]byte, error)
	}

	plainCompressor  struct{}
	snappyCompressor struct{}
	gzipCompressor   struct{}
)

func (plainCompressor) CompressBlock(block []byte) ([]byte, error) {
	return block, nil
}

func (plainCompressor) DecompressBlock(block []byte) ([]byte, error) {
	return block, nil
}

func (snappyCompressor) CompressBlock(block []byte) ([]byte, error) {
	return snappy.Encode(nil, block), nil
}

func (snappyCompressor) DecompressBlock(block []byte) ([]byte, error) {
	return snappy.Decode(nil, block)
}

func (gzipCompressor) CompressBlock(block []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	if _, err := w.Write(block); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (gzipCompressor) DecompressBlock(block []byte) ([]byte, error) {
	buf := bytes.NewReader(block)
	r, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}

	ret, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return ret, r.Close()
}

func compressBlock(block []byte, method parquet.CompressionCodec) ([]byte, error) {
	compressorLock.RLock()
	defer compressorLock.RUnlock()

	c, ok := compressors[method]
	if !ok {
		return nil, errors.Errorf("method %q is not supported", method.String())
	}

	return c.CompressBlock(block)
}

func decompressBlock(block []byte, method parquet.CompressionCodec) ([]byte, error) {
	compressorLock.RLock()
	defer compressorLock.RUnlock()

	c, ok := compressors[method]
	if !ok {
		return nil, errors.Errorf("method %q is not supported", method.String())
	}

	return c.DecompressBlock(block)
}

// RegisterBlockCompressor can plug new kind of block compressor to the library
func RegisterBlockCompressor(method parquet.CompressionCodec, compressor BlockCompressor) {
	compressorLock.Lock()
	defer compressorLock.Unlock()

	compressors[method] = compressor
}

func init() {
	RegisterBlockCompressor(parquet.CompressionCodec_UNCOMPRESSED, plainCompressor{})
	RegisterBlockCompressor(parquet.CompressionCodec_GZIP, gzipCompressor{})
	RegisterBlockCompressor(parquet.CompressionCodec_SNAPPY, snappyCompressor{})
}
