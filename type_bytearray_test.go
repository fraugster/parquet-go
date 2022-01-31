package goparquet

import (
	"bytes"
	"testing"

	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stretchr/testify/require"
)

func TestFuzzCrashByteArrayPlainDecoderNext(t *testing.T) {
	data := []byte("PAR1\x15\x00\x15\xac\x02\x15\xac\x02,\x150\x15\x00\x15\x06\x15" +
		"00\x01\x15\x02\x19,H\f00000000000" +
		"0\x1500\x15\x0e\x15\x1d\x150\x18\x0500000%0\x15" +
		"0\x1500\x160\x19\x1c\x19\x1c&0\x1c\x15\x0e\x190000" +
		"\x19\x18\x0500000\x15\x00\x160\x16\xfa0\x16\xfa\x02&\b" +
		"<\x18\x06000000\x18\x06000000\x1600" +
		"\x19\x1c\x150\x150\x150000\x16\xfa0\x1600000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"0000000000000000000P" +
		"\x01\x00\x00PAR1")

	readAllData(t, data)
}

func TestRepeatedBinaryWithNil(t *testing.T) {
	// this is here to somehow reproduce the issue discussed in https://github.com/fraugster/parquet-go/pull/8
	sd, err := parquetschema.ParseSchemaDefinition(`message msg {
		repeated binary foo;
	}`)
	require.NoError(t, err)

	var buf bytes.Buffer
	fw := NewFileWriter(&buf, WithSchemaDefinition(sd))

	err = fw.AddData(map[string]interface{}{
		"foo": [][]byte{
			[]byte("hello"),
			nil,
			[]byte("world!"),
		},
	})
	require.NoError(t, err)

	require.NoError(t, fw.Close())

	r, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	row, err := r.NextRow()
	require.NoError(t, err)

	// here's a problem: we added nil, but got a []byte{}.
	require.Equal(t, [][]byte{
		[]byte("hello"),
		{},
		[]byte("world!"),
	}, row["foo"])
}
