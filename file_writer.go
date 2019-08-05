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
	SchemaWriter

	totalNumRecords int64
	kvStore         map[string]string
	createdBy       string

	rowGroups []*parquet.RowGroup
}

// NewFileWriter create a new writer. the version is the version of file itself
func NewFileWriter(w io.Writer, version int32) *FileWriter {
	return &FileWriter{
		w: &writePosStruct{
			w:   w,
			pos: 0,
		},
		version:      version,
		SchemaWriter: &schema{},
		kvStore:      make(map[string]string),
		rowGroups:    []*parquet.RowGroup{},
		createdBy:    "parquet-go", // TODO : better info
	}
}

// AddMeteData is for adding meta key value to the file
func (fw *FileWriter) AddMeteData(key string, value string) {
	fw.kvStore[key] = value
}

// FlushRowGroup is to write the row group into the file
func (fw *FileWriter) FlushRowGroup(codec parquet.CompressionCodec) error {
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

	cc, err := writeRowGroup(fw.w, fw.SchemaWriter, codec)
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

// Close is the finalizer for the parquet file, you SHOULD call it to finalize the write
func (fw *FileWriter) Close() error {
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
