package go_parquet

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
)

func TestSchemaParser(t *testing.T) {
	testData := []struct {
		Msg       string
		ExpectErr bool
	}{
		{`message foo { }`, false},
		{`message foo {`, true}, // missing closing brace
		{`message foo { required int64 bar; }`, false},
		{`message foo { repeated int64 bar; }`, false},
		{`message foo { optional int64 bar; }`, false},
		{`message foo { justwrong int64 bar; }`, true}, // incorrect repetition type
		{`message foo { optional int64 bar }`, true},   // missing semicolon after column name
		{`message foo { required binary the_id = 1; required binary client = 2; }`, false},
		{`message foo { optional boolean is_fraud; }`, false},
		{`message foo {
			required binary the_id (UTF8) = 1;
			required binary client (UTF8) = 2;
			required binary request_body = 3;
			required int64 ts = 4;
			required group data_enriched (MAP) {
				repeated group key_value (MAP_KEY_VALUE) {
					required binary key = 5;
					required binary value = 6;
				}
			}
			optional boolean is_fraud = 7;
		}`, false},
		{`message $ { }`, true},                              // $ is not the start of a valid token.
		{`message foo { optional int128 bar; }`, true},       // invalid type
		{`message foo { optional int64 bar (BLUB); }`, true}, // invalid converted type
		{`message foo { optional int32 bar; }`, false},
		{`message foo { optional double bar; }`, false},
		{`message foo { optional float bar; }`, false},
		{`message foo { optional int96 bar; }`, false},
	}

	for idx, tt := range testData {
		p := newSchemaParser(tt.Msg)
		err := p.parse()

		if tt.ExpectErr {
			assert.Error(t, err, "%d. expected error, got none; parsed message: %s", idx, spew.Sdump(p.root))
		} else {
			assert.NoError(t, err, "%d. expected no error, got error instead", idx)
		}
		t.Logf("%d. msg = %s expect err = %t err = %v ; parsed message: %s", idx, tt.Msg, tt.ExpectErr, err, spew.Sdump(p.root))
	}
}
