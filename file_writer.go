package go_parquet

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/fraugster/parquet-go/parquet"
)

type FileWriter struct {
	w writePos

	version int32
	Schema

	totalNumRecords int64
	kvStore         map[string]string
	createdBy       string

	rowGroups []*parquet.RowGroup
}

func NewFileWriter(w io.Writer, version int32) *FileWriter {
	return &FileWriter{
		w: &writePosStruct{
			w:   w,
			pos: 0,
		},
		version:   version,
		Schema:    Schema{},
		kvStore:   make(map[string]string),
		rowGroups: []*parquet.RowGroup{},
		createdBy: "parquet-go", // TODO : better info
	}
}

func (fw *FileWriter) AddMeteData(key string, value string) {
	fw.kvStore[key] = value
}

func (fw *FileWriter) FlushRowGroup(codec parquet.CompressionCodec) error {
	// Write the entire row group
	if fw.numRecords == 0 {
		// TODO: maybe simply return nil?
		return errors.New("nothing to write")
	}

	if fw.w.Pos() == 0 {
		if err := writeFull(fw.w, magic); err != nil {
			return err
		}
	}

	cc, err := writeRowGroup(fw.w, &fw.Schema, codec)
	if err != nil {
		return err
	}

	fw.rowGroups = append(fw.rowGroups, &parquet.RowGroup{
		Columns:        cc,
		TotalByteSize:  0,
		NumRows:        fw.numRecords,
		SortingColumns: nil, // TODO: support Sorting
	})
	fw.totalNumRecords += fw.Schema.numRecords
	// flush the schema
	fw.Schema.resetData()

	return nil
}

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
		Schema:           fw.Schema.getSchemaArray(),
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
