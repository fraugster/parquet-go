package go_parquet

import (
	"io"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

// FileReader is the parquet file reader
type FileReader struct {
	meta *parquet.FileMetaData
	// TODO: make it internal, its not good to expose the schema here
	SchemaReader
	reader io.ReadSeeker

	rowGroupPosition int
}

// NewFileReader try to create a reader from a stream
func NewFileReader(r io.ReadSeeker) (*FileReader, error) {
	meta, err := readFileMetaData(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading file meta data failed")
	}

	schema, err := makeSchema(meta)
	if err != nil {
		return nil, errors.Wrap(err, "creating schema failed")
	}
	// Reset the reader to the beginning of the file
	if _, err := r.Seek(4, io.SeekStart); err != nil {
		return nil, err
	}
	return &FileReader{
		meta:         meta,
		SchemaReader: schema,
		reader:       r,
	}, nil
}

// ReadRowGroup read the next row group into memory
func (f *FileReader) ReadRowGroup() error {
	if len(f.meta.RowGroups) <= f.rowGroupPosition {
		return io.EOF
	}
	f.rowGroupPosition++
	return readRowGroup(f.reader, f.SchemaReader, f.meta.RowGroups[f.rowGroupPosition-1])
}

// RawGroupCount return the number of row groups in file
func (f *FileReader) RawGroupCount() int {
	return len(f.meta.RowGroups)
}
