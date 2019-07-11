package go_parquet

import (
	"fmt"
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
	// Reset the reader to the beginning of the file
	if _, err := r.Seek(4, io.SeekStart); err != nil {
		return nil, err
	}
	return &FileReader{
		meta:   meta,
		schema: schema,
		reader: r,
	}, nil
}

func (f *FileReader) Schema() *Schema {
	return f.schema
}

func (f *FileReader) RawGroupCount() int {
	return len(f.meta.RowGroups)
}

func (f *FileReader) NewReader(col Column, rg int) (*ColumnChunkReader, error) {
	if rg >= len(f.meta.RowGroups) {
		return nil, fmt.Errorf("no such rowgroup: %d", rg)
	}
	chunks := f.meta.RowGroups[rg].Columns
	if col.Index() >= len(chunks) {
		return nil, fmt.Errorf("rowgroup %d has %d column chunks, column %d requested",
			rg, len(chunks), col.Index())
	}
	return newColumnChunkReader(f.reader, f.meta, col, chunks[col.Index()])
}
