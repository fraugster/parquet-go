package goparquet

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

// FileWriter is used to write data to a parquet file. Always use NewFileWriter
// to create such an object.
type FileWriter struct {
	w writePos

	version int32
	SchemaWriter

	totalNumRecords int64
	kvStore         map[string]string
	createdBy       string

	rowGroupFlushSize int64

	rowGroups []*parquet.RowGroup

	codec parquet.CompressionCodec

	newPage newDataPageFunc
}

// FileWriterOption describes an option function that is applied to a FileWriter when it is created.
type FileWriterOption func(fw *FileWriter)

// NewFileWriter creates a new FileWriter. You can provide FileWriterOptions to influence the
// file writer's behaviour.
func NewFileWriter(w io.Writer, options ...FileWriterOption) *FileWriter {
	fw := &FileWriter{
		w: &writePosStruct{
			w:   w,
			pos: 0,
		},
		version:      1,
		SchemaWriter: &schema{},
		kvStore:      make(map[string]string),
		rowGroups:    []*parquet.RowGroup{},
		createdBy:    "parquet-go",
		newPage:      newDataPageV1Writer,
	}

	for _, opt := range options {
		opt(fw)
	}

	return fw
}

// FileVersion sets the version of the file itself.
func FileVersion(version int32) FileWriterOption {
	return func(fw *FileWriter) {
		fw.version = version
	}
}

// CreatedBy sets the creator of the file.
func CreatedBy(createdBy string) FileWriterOption {
	return func(fw *FileWriter) {
		fw.createdBy = createdBy
	}
}

// CompressionCodec sets the compression codec used when writing the file.
func CompressionCodec(codec parquet.CompressionCodec) FileWriterOption {
	return func(fw *FileWriter) {
		fw.codec = codec
	}
}

// MetaData sets the meta data on the file.
func MetaData(data map[string]string) FileWriterOption {
	return func(fw *FileWriter) {
		if data != nil {
			fw.kvStore = data
			return
		}
		fw.kvStore = make(map[string]string)
	}
}

// MaxRowGroupSize sets the rough maximum size of a row group before it shall
// be flushed automatically.
func MaxRowGroupSize(size int64) FileWriterOption {
	return func(fw *FileWriter) {
		fw.rowGroupFlushSize = size
	}
}

// UseSchemaDefinition sets the schema definition to use for this parquet file.
func UseSchemaDefinition(sd *parquetschema.SchemaDefinition) FileWriterOption {
	return func(fw *FileWriter) {
		if err := fw.SetSchemaDefinition(sd); err != nil {
			panic(err)
		}
	}
}

// WithDataPageV2 enables the writer to write pages in the new V2 format. By default,
// the library is using the V1 format. Please be aware that this may cause compatibility
// issues with older implementations of parquet.
func WithDataPageV2() FileWriterOption {
	return func(fw *FileWriter) {
		fw.newPage = newDataPageV2Writer
	}
}

// FlushRowGroup writes the current row group to the parquet file.
func (fw *FileWriter) FlushRowGroup() error {
	// Write the entire row group
	if fw.rowGroupNumRecords() == 0 {
		return errors.New("nothing to write")
	}

	if fw.w.Pos() == 0 {
		if err := writeFull(fw.w, magic); err != nil {
			return err
		}
	}

	cc, err := writeRowGroup(fw.w, fw.SchemaWriter, fw.codec, fw.newPage)
	if err != nil {
		return err
	}

	fw.rowGroups = append(fw.rowGroups, &parquet.RowGroup{
		Columns:        cc,
		TotalByteSize:  0,
		NumRows:        fw.rowGroupNumRecords(),
		SortingColumns: nil,
	})
	fw.totalNumRecords += fw.rowGroupNumRecords()
	// flush the schema
	fw.SchemaWriter.resetData()

	return nil
}

// AddData adds a new record to the current row group and flushes it if auto-flush is enabled and the size
// is equal to or greater than the configured maximum row group size.
func (fw *FileWriter) AddData(m map[string]interface{}) error {
	if err := fw.SchemaWriter.AddData(m); err != nil {
		return err
	}

	if fw.rowGroupFlushSize > 0 && fw.SchemaWriter.DataSize() >= fw.rowGroupFlushSize {
		return fw.FlushRowGroup()
	}

	return nil
}

// Close flushes the current row group if necessary and writes the meta data footer
// to the file. Please be aware that this only finalizes the writing process. If you
// provided a file as io.Writer when creating the FileWriter, you still need to Close
// that file handle separately.
func (fw *FileWriter) Close() error {
	if fw.rowGroupNumRecords() > 0 {
		if err := fw.FlushRowGroup(); err != nil {
			return err
		}
	}

	kv := make([]*parquet.KeyValue, 0, len(fw.kvStore))
	for i := range fw.kvStore {
		v := fw.kvStore[i]
		addr := &v
		if v == "" {
			addr = nil
		}
		kv = append(kv, &parquet.KeyValue{
			Key:   i,
			Value: addr,
		})
	}
	meta := &parquet.FileMetaData{
		Version:          fw.version,
		Schema:           fw.getSchemaArray(),
		NumRows:          fw.totalNumRecords,
		RowGroups:        fw.rowGroups,
		KeyValueMetadata: kv,
		CreatedBy:        &fw.createdBy,
		ColumnOrders:     nil,
	}

	pos := fw.w.Pos()
	if err := writeThrift(meta, fw.w); err != nil {
		return err
	}

	ln := int32(fw.w.Pos() - pos)
	if err := binary.Write(fw.w, binary.LittleEndian, &ln); err != nil {
		return err
	}

	return writeFull(fw.w, magic)
}

// CurrentRowGroupSize returns a rough estimation of the uncompressed size of the current row group data. If you selected
// a compression format other than UNCOMPRESSED, the final size will most likely be smaller and will dpeend on how well
// your data can be compressed.
func (fw *FileWriter) CurrentRowGroupSize() int64 {
	return fw.SchemaWriter.DataSize()
}

// CurrentFileSize returns the amount of data written to the file so far. This does not include data that is in the
// current row group and has not been flushed yet. After closing the file, the size will be even larger since the
// footer is appended to the file upon closing.
func (fw *FileWriter) CurrentFileSize() int64 {
	return fw.w.Pos()
}
