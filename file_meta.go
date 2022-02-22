package goparquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/fraugster/parquet-go/parquet"
)

var magic = []byte{'P', 'A', 'R', '1'}

// ReadFileMetaData reads and returns the meta data of a parquet file. You can use this function
// to read and inspect the meta data before starting to read the whole parquet file.
func ReadFileMetaData(r io.ReadSeeker, extraValidation bool) (*parquet.FileMetaData, error) {
	return ReadFileMetaDataWithContext(context.Background(), r, extraValidation)
}

// ReadFileMetaDataWithContext reads and returns the meta data of a parquet file. You can use this function
// to read and inspect the meta data before starting to read the whole parquet file.
func ReadFileMetaDataWithContext(ctx context.Context, r io.ReadSeeker, extraValidation bool) (*parquet.FileMetaData, error) {
	if extraValidation {
		if _, err := r.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek for the file magic header failed: %w", err)
		}

		buf := make([]byte, 4)
		// read and validate header
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read the file magic header failed: %w", err)
		}
		if !bytes.Equal(buf, magic) {
			return nil, errors.New("invalid parquet file header")
		}

		// read and validate footer
		if _, err := r.Seek(-4, io.SeekEnd); err != nil {
			return nil, fmt.Errorf("seek for the file magic footer failed: %w", err)
		}

		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read the file magic header failed: %w", err)
		}
		if !bytes.Equal(buf, magic) {
			return nil, errors.New("invalid parquet file footer")
		}
	}

	// read footer length
	if _, err := r.Seek(-8, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek for the footer len failed: %w", err)
	}
	var fl int32
	if err := binary.Read(r, binary.LittleEndian, &fl); err != nil {
		return nil, fmt.Errorf("read the footer len failed: %w", err)
	}
	if fl <= 0 {
		return nil, fmt.Errorf("invalid footer len %d", fl)
	}

	// read file metadata
	if _, err := r.Seek(-8-int64(fl), io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek file meta data failed: %w", err)
	}
	meta := &parquet.FileMetaData{}
	if err := readThrift(ctx, meta, io.LimitReader(r, int64(fl))); err != nil {
		return nil, fmt.Errorf("read file meta failed: %w", err)
	}

	return meta, nil
}
