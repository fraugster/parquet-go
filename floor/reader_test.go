package floor

import (
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
)

func TestReadFile(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/readtest.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := goparquet.NewFileWriter(wf, goparquet.CompressionCodec(parquet.CompressionCodec_SNAPPY), goparquet.CreatedBy("floor-unittest"))

	sd, err := goparquet.ParseSchemaDefinition(
		`message test_msg {
			required int64 foo;
			optional binary bar (STRING);
			optional group baz {
				required int64 value;
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	w.SetSchemaDefinition(sd)

	hlWriter := NewWriter(w)

	type bazMsg struct {
		Value uint32
	}

	type testMsg struct {
		Foo int64
		Bar *string
		Baz *bazMsg
	}

	// Baz doesn't seem to get written correctly. when dumping the resulting file, baz.value is wrong.
	require.NoError(t, hlWriter.Write(testMsg{Foo: 1, Bar: strPtr("hello"), Baz: &bazMsg{Value: 9001}}))
	require.NoError(t, hlWriter.Write(&testMsg{Foo: 23}))
	require.NoError(t, hlWriter.Write(testMsg{Foo: 42, Bar: strPtr("world!")}))
	require.NoError(t, hlWriter.Close())

	rf, err := os.Open("files/readtest.parquet")
	require.NoError(t, err)
	defer rf.Close()

	reader, err := goparquet.NewFileReader(rf)
	require.NoError(t, err)

	hlReader := NewReader(reader)

	count := 0

	var result []testMsg

	for hlReader.Next() {
		var msg testMsg

		require.NoError(t, hlReader.Scan(&msg), "%d. Scan failed", count)
		t.Logf("%d. data = %#v", count, hlReader.data)

		result = append(result, msg)

		count++
	}

	t.Logf("count = %d", count)
	t.Logf("result = %s", spew.Sdump(result))

	require.NoError(t, hlReader.Err(), "hlReader returned an error")
}
