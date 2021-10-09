package goparquet

import (
	"fmt"
	"io"
	"strings"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

// FileReader is used to read data from a parquet file. Always use NewFileReader to create
// such an object.
type FileReader struct {
	meta *parquet.FileMetaData
	SchemaReader
	reader io.ReadSeeker

	rowGroupPosition int
	currentRecord    int64
	skipRowGroup     bool
}

// NewFileReader creates a new FileReader. You can limit the columns that are read by providing
// the names of the specific columns to read using dotted notation. If no columns are provided,
// then all columns are read.
func NewFileReader(r io.ReadSeeker, columns ...string) (*FileReader, error) {
	meta, err := ReadFileMetaData(r, true)
	if err != nil {
		return nil, errors.Wrap(err, "reading file meta data failed")
	}

	schema, err := makeSchema(meta)
	if err != nil {
		return nil, errors.Wrap(err, "creating schema failed")
	}

	schema.SetSelectedColumns(columns...)
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

func NewFileReaderWithMetaData(r io.ReadSeeker, meta *parquet.FileMetaData, columns ...string) (*FileReader, error) {
	var err error
	if meta == nil {
		meta, err = ReadFileMetaData(r, true)
		if err != nil {
			return nil, errors.Wrap(err, "reading file meta data failed")
		}
	}

	schema, err := makeSchema(meta)
	if err != nil {
		return nil, errors.Wrap(err, "creating schema failed")
	}

	schema.SetSelectedColumns(columns...)
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

func (f *FileReader) SeekToRowGroup(rowGroupPosition int) error {
	f.rowGroupPosition = rowGroupPosition - 1
	f.currentRecord = 0
	return f.readRowGroup()
}

// readRowGroup read the next row group into memory
func (f *FileReader) readRowGroup() error {
	if len(f.meta.RowGroups) <= f.rowGroupPosition {
		return io.EOF
	}
	f.rowGroupPosition++
	return readRowGroup(f.reader, f.SchemaReader, f.meta.RowGroups[f.rowGroupPosition-1])
}

// CurrentRowGroup returns information about the current row group.
func (f *FileReader) CurrentRowGroup() *parquet.RowGroup {
	if f == nil || f.meta == nil || f.meta.RowGroups == nil || f.rowGroupPosition-1 >= len(f.meta.RowGroups) {
		return nil
	}
	return f.meta.RowGroups[f.rowGroupPosition-1]
}

// RowGroupCount returns the number of row groups in the parquet file.
func (f *FileReader) RowGroupCount() int {
	return len(f.meta.RowGroups)
}

// NumRows returns the number of rows in the parquet file. This information is directly taken from
// the file's meta data.
func (f *FileReader) NumRows() int64 {
	return f.meta.NumRows
}

func (f *FileReader) advanceIfNeeded() error {
	if f.rowGroupPosition == 0 || f.currentRecord >= f.SchemaReader.rowGroupNumRecords() || f.skipRowGroup {
		if err := f.readRowGroup(); err != nil {
			f.skipRowGroup = true
			return err
		}
		f.currentRecord = 0
		f.skipRowGroup = false
	}

	return nil
}

// RowGroupNumRows returns the number of rows in the current RowGroup.
func (f *FileReader) RowGroupNumRows() (int64, error) {
	if err := f.advanceIfNeeded(); err != nil {
		return 0, err
	}

	return f.SchemaReader.rowGroupNumRecords(), nil
}

// NextRow reads the next row from the parquet file. If required, it will load the next row group.
func (f *FileReader) NextRow() (map[string]interface{}, error) {
	if err := f.advanceIfNeeded(); err != nil {
		return nil, err
	}

	f.currentRecord++
	return f.SchemaReader.getData()
}

// SkipRowGroup skips the currently loaded row group and advances to the next row group.
func (f *FileReader) SkipRowGroup() {
	f.skipRowGroup = true
}

// PreLoad is used to load the row group if required. It does nothing if the row group is already loaded.
func (f *FileReader) PreLoad() error {
	return f.advanceIfNeeded()
}

// MetaData returns a map of metadata key-value pairs stored in the parquet file.
func (f *FileReader) MetaData() map[string]string {
	return keyValueMetaDataToMap(f.meta.KeyValueMetadata)
}

// ColumnMetaData returns a map of metadata key-value pairs for the provided column in the current
// row group. The column name has to be provided in its dotted notation.
func (f *FileReader) ColumnMetaData(colName string) (map[string]string, error) {
	for _, col := range f.CurrentRowGroup().Columns {
		if colName == strings.Join(col.MetaData.PathInSchema, ".") {
			return keyValueMetaDataToMap(col.MetaData.KeyValueMetadata), nil
		}
	}
	return nil, fmt.Errorf("column %q not found", colName)
}

func keyValueMetaDataToMap(kvMetaData []*parquet.KeyValue) map[string]string {
	data := make(map[string]string)
	for _, kv := range kvMetaData {
		if kv.Value != nil {
			data[kv.Key] = *kv.Value
		}
	}
	return data
}
