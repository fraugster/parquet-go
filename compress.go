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
	ReaderCounter interface {
		io.Reader
		Count() int
		Expected() int
	}

	BlockCompressor interface {
		Reader(reader io.Reader, expected int32) (ReaderCounter, error)
	}

	plainCompressor  struct{}
	snappyCompressor struct{}
	gzipCompressor   struct{}

	streamReaderCounter struct {
		r     io.Reader
		n     int
		total int
	}
)

func (s *streamReaderCounter) Expected() int {
	return s.total
}

func (gzipCompressor) Reader(reader io.Reader, expected int32) (ReaderCounter, error) {
	// TODO: maybe its better to load all data into memory
	r, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}

	return &streamReaderCounter{
		r:     r,
		total: int(expected),
	}, nil
}

func (snappyCompressor) Reader(reader io.Reader, expected int32) (ReaderCounter, error) {
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

	return &streamReaderCounter{
		r:     bytes.NewReader(ret),
		total: int(expected),
	}, nil
}

func (plainCompressor) Reader(reader io.Reader, expected int32) (ReaderCounter, error) {
	return &streamReaderCounter{
		r:     reader,
		total: int(expected),
	}, nil
}

func (s *streamReaderCounter) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	s.n += n
	// We read more data?
	if s.total < s.n {
		return n, errors.Errorf("this should be %d byte but it was %d", s.total, s.n)
	}
	if err != nil {
		if s.total == s.n {
			// OK
			return n, err
		}
		if s.total > s.n {
			return n, errors.Wrapf(err, "required %d byte to read, but read only %d byte", s.total, s.n)
		}
	}

	return n, nil
}

func (s *streamReaderCounter) Count() int {
	return s.n
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

func NewBlockReader(in io.Reader, codec parquet.CompressionCodec, compressedSize int32, uncompressedSize int32) (ReaderCounter, error) {
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
