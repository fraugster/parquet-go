package goparquet

import (
	"context"
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

	ctx context.Context
}

// NewFileReader creates a new FileReader. You can limit the columns that are read by providing
// the names of the specific columns to read using dotted notation. If no columns are provided,
// then all columns are read.
func NewFileReader(r io.ReadSeeker, columns ...string) (*FileReader, error) {
	return NewFileReaderWithContext(context.Background(), r, columns...)
}

// NewFileReaderWithContext creates a new FileReader. You can limit the columns that are read by providing
// the names of the specific columns to read using dotted notation. If no columns are provided,
// then all columns are read. The provided context.Context overrides the default context (which is a context.Background())
// for use in other methods of the *FileReader type.
func NewFileReaderWithContext(ctx context.Context, r io.ReadSeeker, columns ...string) (*FileReader, error) {
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
		ctx:          ctx,
	}, nil
}

// NewFileReaderWithMetaData creates a new FileReader with custom file meta data. You can limit the columns that
// are read by providing the names of the specific columns to read using dotted notation. If no columns are provided,
// then all columns are read.
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

// SeekToRowGroup seeks to a particular row group, identified by its index.
func (f *FileReader) SeekToRowGroup(rowGroupPosition int) error {
	return f.SeekToRowGroupWithContext(f.ctx, rowGroupPosition)
}

// SeekToRowGroupWithContext seeks to a particular row group, identified by its index.
func (f *FileReader) SeekToRowGroupWithContext(ctx context.Context, rowGroupPosition int) error {
	f.rowGroupPosition = rowGroupPosition - 1
	f.currentRecord = 0
	return f.readRowGroup(ctx)
}

// readRowGroup read the next row group into memory
func (f *FileReader) readRowGroup(ctx context.Context) error {
	if len(f.meta.RowGroups) <= f.rowGroupPosition {
		return io.EOF
	}
	f.rowGroupPosition++
	return readRowGroup(ctx, f.reader, f.SchemaReader, f.meta.RowGroups[f.rowGroupPosition-1])
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

func (f *FileReader) advanceIfNeeded(ctx context.Context) error {
	if f.rowGroupPosition == 0 || f.currentRecord >= f.SchemaReader.rowGroupNumRecords() || f.skipRowGroup {
		if err := f.readRowGroup(ctx); err != nil {
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
	return f.RowGroupNumRowsWithContext(f.ctx)
}

// RowGroupNumRowsWithContext returns the number of rows in the current RowGroup.
func (f *FileReader) RowGroupNumRowsWithContext(ctx context.Context) (int64, error) {
	if err := f.advanceIfNeeded(ctx); err != nil {
		return 0, err
	}

	return f.SchemaReader.rowGroupNumRecords(), nil
}

// NextRow reads the next row from the parquet file. If required, it will load the next row group.
func (f *FileReader) NextRow() (map[string]interface{}, error) {
	return f.NextRowWithContext(f.ctx)
}

// NextRowWithContext reads the next row from the parquet file. If required, it will load the next row group.
func (f *FileReader) NextRowWithContext(ctx context.Context) (map[string]interface{}, error) {
	if err := f.advanceIfNeeded(ctx); err != nil {
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
	return f.PreLoadWithContext(f.ctx)
}

// PreLoadWithContext is used to load the row group if required. It does nothing if the row group is already loaded.
func (f *FileReader) PreLoadWithContext(ctx context.Context) error {
	return f.advanceIfNeeded(ctx)
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
