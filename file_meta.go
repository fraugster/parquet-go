package goparquet

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

var magic = []byte{'P', 'A', 'R', '1'}

func ReadFileMetaData(r io.ReadSeeker, extraValidation bool) (*parquet.FileMetaData, error) {
	if extraValidation {
		if _, err := r.Seek(0, io.SeekStart); err != nil {
			return nil, errors.Wrap(err, "seek for the file magic header failed")
		}

		buf := make([]byte, 4)
		// read and validate header
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, errors.Wrap(err, "read the file magic header failed")
		}
		if !bytes.Equal(buf, magic) {
			return nil, errors.Errorf("invalid parquet file header")
		}

		// read and validate footer
		if _, err := r.Seek(-4, io.SeekEnd); err != nil {
			return nil, errors.Wrap(err, "seek for the file magic footer failed")
		}

		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, errors.Wrap(err, "read the file magic header failed")
		}
		if !bytes.Equal(buf, magic) {
			return nil, errors.Errorf("invalid parquet file footer")
		}
	}

	// read footer length
	if _, err := r.Seek(-8, io.SeekEnd); err != nil {
		return nil, errors.Wrap(err, "seek for the footer len failed")
	}
	var fl int32
	if err := binary.Read(r, binary.LittleEndian, &fl); err != nil {
		return nil, errors.Wrap(err, "read the footer len failed")
	}
	if fl <= 0 {
		return nil, errors.Errorf("invalid footer len %d", fl)
	}

	// read file metadata
	if _, err := r.Seek(-8-int64(fl), io.SeekEnd); err != nil {
		return nil, errors.Wrap(err, "seek file meta data failed")
	}
	meta := &parquet.FileMetaData{}
	if err := readThrift(meta, io.LimitReader(r, int64(fl))); err != nil {
		return nil, errors.Wrap(err, "read file meta failed")
	}

	return meta, nil
}
