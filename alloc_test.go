package goparquet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stretchr/testify/require"
)

func TestAllocTrackerTriggerError(t *testing.T) {
	var buf bytes.Buffer

	sd, err := parquetschema.ParseSchemaDefinition(`message test {
		required binary foo (STRING);
	}`)
	require.NoError(t, err)

	wr := NewFileWriter(&buf,
		WithSchemaDefinition(sd),
		WithMaxRowGroupSize(150*1024*1024),
		WithMaxPageSize(150*1024*1024),
		WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	// this should produce ~20 MiB easily compressible data
	for i := 0; i < 20*1024; i++ {
		err := wr.AddData(map[string]interface{}{
			"foo": func() []byte {
				var data [512]byte
				data[0] = byte(i % 256)
				data[1] = byte(i / 256)
				return []byte(fmt.Sprintf("%x", data[:]))
			}(),
		})
		require.NoError(t, err)
	}
	require.NoError(t, wr.FlushRowGroup())
	require.NoError(t, wr.Close())

	t.Logf("buf size: %d", buf.Len())

	// we set a maximum memory size for that file of 10 MiB, so fully reading the file created earlier should fail.
	r, err := NewFileReaderWithOptions(bytes.NewReader(buf.Bytes()), WithMaximumMemorySize(10*1024*1024))
	require.NoError(t, err)

	_, err = r.NextRow()
	require.Error(t, err)
	require.Contains(t, err.Error(), "bytes is greater than configured maximum of 10485760 bytes")
}

func TestAllocTrackerTriggerNoError(t *testing.T) {
	var buf bytes.Buffer

	sd, err := parquetschema.ParseSchemaDefinition(`message test {
		required binary foo (STRING);
	}`)
	require.NoError(t, err)

	wr := NewFileWriter(&buf,
		WithSchemaDefinition(sd),
		WithMaxPageSize(1024*1024),
		WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	// this should produce ~20 MiB easily compressible data
	for i := 0; i < 20*1024; i++ {
		err := wr.AddData(map[string]interface{}{
			"foo": func() []byte {
				var data [512]byte
				data[0] = byte(i % 256)
				data[1] = byte(i / 256)
				return []byte(fmt.Sprintf("%x", data[:]))
			}(),
		})
		require.NoError(t, err)
	}
	require.NoError(t, wr.FlushRowGroup())
	require.NoError(t, wr.Close())

	t.Logf("buf size: %d", buf.Len())

	// we set a maximum memory size for that file of 100 MiB, so fully reading the file created earlier should not fail.
	r, err := NewFileReaderWithOptions(bytes.NewReader(buf.Bytes()), WithMaximumMemorySize(100*1024*1024))
	require.NoError(t, err)

	for i := 0; ; i++ {
		_, err := r.NextRow()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("NextRow %d returned error: %v", i, err)
		}
	}
}
