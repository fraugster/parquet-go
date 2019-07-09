package go_parquet

import (
	"io"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

// File is the parquet file
type FileReader struct {
	meta   *parquet.FileMetaData
	schema *Schema
	reader io.ReadSeeker
}

// NewFileReader try to create a reader from a stream
func NewFileReader(r io.ReadSeeker) (*FileReader, error) {
	meta, err := ReadFileMetaData(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading file meta data failed")
	}

	schema, err := MakeSchema(meta)
	if err != nil {
		return nil, errors.Wrap(err, "creating schema failed")
	}

	return &FileReader{
		meta:   meta,
		schema: schema,
		reader: r,
	}, nil
}
