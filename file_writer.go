package go_parquet

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/fraugster/parquet-go/parquet"
)

// FileWriter is a parquet file writer
type FileWriter struct {
	w writePos

	version int32
	// TODO: make it internal, its not good to expose the schema here
	SchemaWriter

	totalNumRecords int64
	kvStore         map[string]string
	createdBy       string

	rowGroupFlushSize int64

	rowGroups []*parquet.RowGroup

	codec parquet.CompressionCodec
}

// FileWriterOption describes an option function that is applied to a FileWriter when it is created.
type FileWriterOption func(fw *FileWriter)

// NewFileWriter create a new writer.
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
	}

	for _, opt := range options {
		opt(fw)
	}

	return fw
}

// FileVersion set the version of the file itself.
func FileVersion(version int32) FileWriterOption {
	return func(fw *FileWriter) {
		fw.version = version
	}
}

// CreatedBy sets the creator of the file
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

// MetaData set the meta data on the file
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

// FlushRowGroup is to write the row group into the file
func (fw *FileWriter) FlushRowGroup() error {
	// Write the entire row group
	if fw.NumRecords() == 0 {
		// TODO: maybe simply return nil?
		return errors.New("nothing to write")
	}

	if fw.w.Pos() == 0 {
		if err := writeFull(fw.w, magic); err != nil {
			return err
		}
	}

	cc, err := writeRowGroup(fw.w, fw.SchemaWriter, fw.codec)
	if err != nil {
		return err
	}

	fw.rowGroups = append(fw.rowGroups, &parquet.RowGroup{
		Columns:        cc,
		TotalByteSize:  0,
		NumRows:        fw.NumRecords(),
		SortingColumns: nil, // TODO: support Sorting
	})
	fw.totalNumRecords += fw.NumRecords()
	// flush the schema
	fw.SchemaWriter.resetData()

	return nil
}

// AddData add a new record to the current row group and flush it if the auto flush is enabled and the size
// is more than the auto flush size
func (fw *FileWriter) AddData(m map[string]interface{}) error {
	if err := fw.SchemaWriter.AddData(m); err != nil {
		return err
	}

	if fw.rowGroupFlushSize > 0 && fw.SchemaWriter.DataSize() >= fw.rowGroupFlushSize {
		return fw.FlushRowGroup()
	}

	return nil
}

// Close is the finalizer for the parquet file, you SHOULD call it to finalize the write
func (fw *FileWriter) Close() error {
	if fw.NumRecords() > 0 {
		if err := fw.FlushRowGroup(); err != nil {
			return err
		}
	}

	kv := make([]*parquet.KeyValue, 0, len(fw.kvStore))
	for i := range fw.kvStore {
		v := fw.kvStore[i] // TODO: nil value support
		kv = append(kv, &parquet.KeyValue{
			Key:   i,
			Value: &v,
		})
	}
	meta := &parquet.FileMetaData{
		Version:          fw.version,
		Schema:           fw.getSchemaArray(),
		NumRows:          fw.totalNumRecords,
		RowGroups:        fw.rowGroups,
		KeyValueMetadata: kv,
		CreatedBy:        &fw.createdBy,
		ColumnOrders:     nil, // TODO: support for column order
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

// CurrentRowGroupSize is the size of current row group data (not including definition/repetition levels and parquet headers
// just a rough estimation of data size in plain format, uncompressed. if the encoding is different than plain, the finall
// size depends on the data
func (fw *FileWriter) CurrentRowGroupSize() int64 {
	return fw.SchemaWriter.DataSize()
}
