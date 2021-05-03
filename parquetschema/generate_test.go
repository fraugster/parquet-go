package parquetschema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertGeneratedSchemaMatches(t *testing.T, expectedSchema string, i interface{}) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Error(r)
		}
	}()

	s, err := Generate(i)
	require.NoError(t, err)

	sExpected, err := ParseSchemaDefinition(expectedSchema)
	require.NoError(t, err)

	assert.EqualValues(t, sExpected.String(), s.String())
}

func TestGenerate(t *testing.T) {

	t.Run("int", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int64 val (INT(64,true));}`, new(struct {
			Val int
		}))
	})
	t.Run("int64", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int64 val (INT(64,true));}`, new(struct {
			Val int64
		}))
	})
	t.Run("int32", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int32 val (INT(32,true));}`, new(struct {
			Val int32
		}))
	})
	t.Run("int16", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int32 val (INT(16,true));}`, new(struct {
			Val int16
		}))
	})
	t.Run("int8", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int32 val (INT(8,true));}`, new(struct {
			Val int8
		}))
	})
	t.Run("uint", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int64 val (INT(64,false));}`, new(struct {
			Val uint
		}))
	})
	t.Run("uint64", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int64 val (INT(64,false));}`, new(struct {
			Val uint64
		}))
	})
	t.Run("uint32", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int32 val (INT(32,false));}`, new(struct {
			Val uint32
		}))
	})
	t.Run("uint16", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int32 val (INT(16,false));}`, new(struct {
			Val uint16
		}))
	})
	t.Run("uint8", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int32 val (INT(8,false));}`, new(struct {
			Val uint8
		}))
	})
	t.Run("float32", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required float val;}`, new(struct {
			Val float32
		}))
	})
	t.Run("float64", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required double val;}`, new(struct {
			Val float64
		}))
	})
	t.Run("bool", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required boolean val;}`, new(struct {
			Val bool
		}))
	})
	t.Run("string", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required binary val (STRING);}`, new(struct {
			Val string
		}))
	})
	t.Run("[]byte", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required binary val;}`, new(struct {
			Val []byte
		}))
	})
	t.Run("[n]byte", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required fixed_len_byte_array(7) val;}`, new(struct {
			Val [7]byte
		}))
	})
	t.Run("Time", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {required int64 val (TIMESTAMP(NANOS,true));}`, new(struct {
			Val time.Time
		}))
	})
	t.Run("struct", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {
			required group val {
				required boolean x;
			}
		}`, new(struct {
			Val struct {
				X bool
			}
		}))
	})
	t.Run("slice", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {
			optional group val (LIST) {
				repeated group list {
					required boolean element;
				}
			}
		}`, new(struct {
			Val []bool
		}))
	})
	t.Run("map", func(t *testing.T) {
		assertGeneratedSchemaMatches(t, `message root {
			optional group val (MAP) {
				repeated group key_value (MAP_KEY_VALUE) {
					required boolean key;
					required boolean value;
				}
			}
		}`, new(struct {
			Val map[bool]bool
		}))
	})

}
