package goparquet

import (
	"io"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

// FileReader is the parquet file reader
type FileReader struct {
	meta *parquet.FileMetaData
	schemaReader
	reader io.ReadSeeker

	rowGroupPosition int
	currentRecord    int64
	skipRowGroup     bool
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
		schemaReader: schema,
		reader:       r,
	}, nil
}

// readRowGroup read the next row group into memory
func (f *FileReader) readRowGroup() error {
	if len(f.meta.RowGroups) <= f.rowGroupPosition {
		return io.EOF
	}
	f.rowGroupPosition++
	return readRowGroup(f.reader, f.schemaReader, f.meta.RowGroups[f.rowGroupPosition-1])
}

// RawGroupCount return the number of row groups in file
func (f *FileReader) RawGroupCount() int {
	return len(f.meta.RowGroups)
}

// NumRows return the number of rows in the current file reader, based on parquet meta data without loading the row groups
func (f *FileReader) NumRows() int64 {
	return f.meta.NumRows
}

func (f *FileReader) advanceIfNeeded() error {
	if f.rowGroupPosition == 0 || f.currentRecord >= f.schemaReader.rowGroupNumRecords() || f.skipRowGroup {
		if err := f.readRowGroup(); err != nil {
			f.skipRowGroup = true
			return err
		}
		f.currentRecord = 0
		f.skipRowGroup = false
	}

	return nil
}

// RowGroupNumRows returns the number of rows in the current active RowGroup
func (f *FileReader) RowGroupNumRows() (int64, error) {
	if err := f.advanceIfNeeded(); err != nil {
		return 0, err
	}

	return f.schemaReader.rowGroupNumRecords(), nil
}

// NextRow try to read next row from the parquet file, it loads the next row group if required
func (f *FileReader) NextRow() (map[string]interface{}, error) {
	if err := f.advanceIfNeeded(); err != nil {
		return nil, err
	}

	f.currentRecord++
	return f.schemaReader.getData()
}

// SkipRowGroup skips the current loaded row group and advance to the next row group
func (f *FileReader) SkipRowGroup() {
	f.skipRowGroup = true
}

// PreLoad is used to load the row group if required. it does nothing if the row group is already loaded
func (f *FileReader) PreLoad() error {
	return f.advanceIfNeeded()
}
