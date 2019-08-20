package floor

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	goparquet "github.com/fraugster/parquet-go"
)

func TestReadFile(t *testing.T) {
	rf, err := os.Open("files/snappy.parquet")
	require.NoError(t, err)
	defer rf.Close()

	reader, err := goparquet.NewFileReader(rf)
	require.NoError(t, err)

	hlReader := NewReader(reader)

	count := 0
	for hlReader.Next() {
		//t.Logf("%d. data = %#v", count, hlReader.data)
		count++
	}

	t.Logf("count = %d", count)

	require.NoError(t, hlReader.Err(), "hlReader returned an error")
}
