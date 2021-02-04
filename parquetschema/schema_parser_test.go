package parquetschema

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/stretchr/testify/assert"
)

func TestSchemaParser(t *testing.T) {
	testData := []struct {
		Msg       string
		ExpectErr bool
		Strict    bool
	}{
		// 0.
		{`message foo { }`, false, false},
		{`message foo {`, true, false}, // missing closing brace
		{`message foo { required int64 bar; }`, false, false},
		{`message foo { repeated int64 bar; }`, false, false},
		{`message foo { optional int64 bar; }`, false, false},
		{`message foo { optional int64 bar.buzz; }`, false, false},
		{`message foo { justwrong int64 bar; }`, true, false}, // incorrect repetition type
		{`message foo { optional int64 bar }`, true, false},   // missing semicolon after column name
		{`message foo { required binary the_id = 1; required binary client = 2; }`, false, false},
		{`message foo { optional boolean is_fraud; }`, false, false},
		{`message foo {
			required binary the_id (STRING) = 1;
			required binary client (STRING) = 2;
			required binary request_body = 3;
			required int64 ts = 4;
			required group data_enriched (MAP) {
				repeated group key_value (MAP_KEY_VALUE) {
					required binary key = 5;
					required binary value = 6;
				}
			}
			optional boolean is_fraud = 7;
		}`, false, false},
		// 10.
		{`message $ { }`, true, false},                              // $ is not the start of a valid token.
		{`message foo { optional int128 bar; }`, true, false},       // invalid type
		{`message foo { optional int64 bar (BLUB); }`, true, false}, // invalid logical type
		{`message foo { optional int32 bar; }`, false, false},
		{`message foo { optional double bar; }`, false, false},
		{`message foo { optional float bar; }`, false, false},
		{`message foo { optional int96 bar; }`, false, false},
		{`message foo {
			required group ids (LIST) {
				repeated group list {
					required int64 element;
				}
			}
		}`, false, false},
		{`message foo {
			optional group array_of_arrays (LIST) {
				repeated group list {
					required group element (LIST) {
						repeated group list {
							required int32 element;
						}
					}
				}
			}
		}`, false, false},
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int32 key;
					required int32 value;
				}
			}
		}`, false, false},
		// 20.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					required int64 element;
				}
			}
		}`, false, false},
		{`message foo {
			optional group bar (LIST) {
				repeated group element {
					required int64 element;
				}
			}
		}`, false, false}, // repeated group is called "element", not "list"; but that's valid under the backwards compatibility rules.
		{`message foo {
			optional group bar (LIST) {
				repeated int64 list;
			}
		}`, true, false}, // repeated list is not a group.
		{`message foo {
			repeated group bar (LIST) {
				repeated group list {
					optional int64 element;
				}
			}
		}`, true, false}, // bar is LIST but has repetition type repeated.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					optional int64 element;
					optional int64 element2;
				}
			}
		}`, true, false}, // bar.list has 2 children.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					optional int64 invalid;
				}
			}
		}`, true, false}, // bar.list has 1 child, but it's called invalid, not element.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					repeated int64 element;
				}
			}
		}`, true, false}, // bar.list.element is of the wrong repetition type.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					required int64 baz;
				}
				optional int64 list_size;
			}
		}`, true, false}, // only element underneath (LIST) allowed is repeated group list; list_size is invalid.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 key;
					optional int32 value;
				}
			}
		}`, false, false},
		{`message foo {
			optional group bar (MAP) {
				repeated group stuff {
					required int64 key;
					optional int32 value;
				}
			}
		}`, true, true}, // repeated group underneath (MAP) is not called key_value.
		// 30.
		{`message foo {
			optional group bar (MAP) {
				repeated int64 key_value;
			}
		}`, true, false}, // repeated key_value is not a group.
		{`message foo {
			optional group bar (MAP) {
			}
		}`, true, false}, // empty group bar.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 key;
					optional int32 value;
					optional int32 another_value;
				}
			}
		}`, true, false}, // inside key_value, only key and value are allowed.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					optional int64 key;
					optional int32 value;
				}
			}
		}`, true, true}, // bar.key_value.key must be required.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 key;
				}
			}
		}`, true, false}, // bar.key_value.value is missing.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 key;
					optional int32 key;
				}
			}
		}`, true, true}, // bar.key_value has 2 children but child value is missing.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 value;
					optional int32 value;
				}
			}
		}`, true, true}, // strict: bar.key_value has 2 children but child key is missing.
		{`message foo {
			required int32 date (DATE);
		}`, false, false},
		{`message foo {
			required int64 date (DATE);
		}`, true, false}, // date is annotated as DATE but data type is int64.
		{`message foo {
			required int64 ts (TIMESTAMP(MILLIS, true));
		}`, false, false},
		// 40.
		{`message foo {
			required int64 ts (TIMESTAMP(MICROS, false));
		}`, false, false},
		{`message foo {
			required int64 ts (TIMESTAMP(NANOS, false));
		}`, false, false},
		{`message foo {
			required int96 ts (TIMESTAMP(NANOS, false));
		}`, false, false},
		{`message foo {
			required int32 ts (TIMESTAMP(NANOS, false));
		}`, true, false}, // all TIMESTAMPs must be int64.
		{`message foo {
			required int64 ts (TIMESTAMP(,));
		}`, true, false}, // invalid annotation syntax for TIMESTAMP.
		{`message foo {
			required int64 ts (TIMESTAMP(FOO,false));
		}`, true, false}, // invalid TIMESTAMP unit.
		{`message foo {
			required int64 ts (TIMESTAMP(MILLIS,bla));
		}`, true, false}, // invalid TIMESTAMP isAdjustedToUTC.
		{`message foo {
			required fixed_len_byte_array(16) theid (UUID);
		}`, false, false},
		{`message foo {
			required fixed_len_byte_array theid;
		}`, true, false}, // no length provided.
		{`message foo {
			required fixed_len_byte_array(-1) theid;
		}`, true, false}, // negative length.
		{`message foo {
			required binary group (STRING);
		}`, false, false},
		// 50.
		{`message foo {
			required int64 ts (TIME(NANOS, true));
		}`, false, false},
		{`message foo {
			required int64 ts (TIME(MICROS, true));
		}`, false, false},
		{`message foo {
			required int32 ts (TIME(MILLIS, true));
		}`, false, false},
		{`message foo {
			required int64 ts (TIME(MILLIS, true));
		}`, true, false}, // TIME(MILLIS, ...) must be used with int32.
		{`message foo {
			required int64 ts (TIME(FOOS, true));
		}`, true, false}, // invalid unit FOOS.
		{`message foo {
			required int64 ts (TIME(MICROS, bloob));
		}`, true, false}, // invalid boolean bloob
		{`message foo {
			required int32 foo (INT(8, true));
		}`, false, false},
		{`message foo {
			required int32 foo (INT(16, false));
		}`, false, false},
		{`message foo {
			required int32 foo (INT(32, true));
		}`, false, false},
		{`message foo {
			required int64 foo (INT(64, true));
		}`, false, false},
		// 60.
		{`message foo {
			required int32 foo (INT(64, true));
		}`, true, false}, // int32 can't be annotated as INT(64, true)
		{`message foo {
			required int64 foo (INT(32, true));
		}`, true, false}, // int64 can't be annotated as INT(32, true)
		{`message foo {
			required int32 foo (INT(28, true));
		}`, true, false}, // invalid bitwidth
		{`message foo {
			required int32 foo (INT(32, foobar));
		}`, true, false}, // invalid isSigned
		{`message foo {
			required int32 foo (DECIMAL(5, 3));
		}`, false, false},
		{`message foo {
			required int32 foo (DECIMAL(12, 3));
		}`, true, false}, // precision out of bounds.
		{`message foo {
			required int64 foo (DECIMAL(12, 3));
		}`, false, false},
		{`message foo {
			required int64 foo (DECIMAL(20, 3));
		}`, true, false}, // precision out of bounds.
		{`message foo {
			required int64 foo (DECIMAL);
		}`, true, false}, // no precision, scale parameters.
		{`message foo {
			required fixed_len_byte_array(10) foo (DECIMAL(20,10));
		}`, false, false},
		// 70.
		{`message foo {
			required fixed_len_byte_array(10) foo (DECIMAL(23,10));
		}`, true, false}, // 23 is out of bounds; maximum for 10 is 22.
		{`message foo {
			required binary foo (DECIMAL(100,10));
		}`, false, false},
		{`message foo {
			required binary foo (DECIMAL(0,10));
		}`, true, false}, // invalid precision.
		{`message foo {
			required float foo (DECIMAL(1,10));
		}`, true, false}, // invalid data type.
		{`message foo {
			required binary foo (JSON);
		}`, false, false},
		{`message foo {
			required int64 foo (JSON);
		}`, true, false}, // only binary can be annotated as JSON.
		{`message foo {
			required binary foo (BSON);
		}`, false, false},
		{`message foo {
			required int32 foo (BSON);
		}`, true, false}, // only binary can be annotated as BSON.
		{`message foo {
			required fixed_len_byte_array(32) foo (UUID);
		}`, true, false}, // invalid length for UUID.
		{`message foo {
			required int64 foo (ENUM);
		}`, true, false}, // invalid type for ENUM.
		// 80.
		{`message foo {
			required int64 foo (UTF8);
		}`, true, false}, // invalid type for UTF8.
		{`message foo {
			required double foo (TIME_MILLIS);
		}`, true, false}, // invalid type for TIME_MILLIS.
		{`message foo {
			required float foo (TIME_MICROS);
		}`, true, false}, // invalid type for TIME_MICROS.
		{`message foo {
			required double foo (TIMESTAMP_MILLIS);
		}`, true, false}, // invalid type for TIMESTAMP_MILLIS.
		{`message foo {
			required double foo (TIMESTAMP_MICROS);
		}`, true, false}, // invalid type for TIMESTAMP_MICROS.
		{`message foo {
			required double foo (UINT_8);
		}`, true, false}, // invalid type for UINT_8.
		{`message foo {
			required double foo (INT_64);
		}`, true, false}, // invalid type for INT_64.
		{`message foo {
			required double foo (INTERVAL);
		}`, true, false}, // invalid type for INTERVAL.
		{`message foo {
			required double foo (TIME(NANOS, true));
		}`, true, false}, // invalid type for TIME(NANOS, true).
		{`message foo {
			required double foo (TIME(MICROS, true));
		}`, true, false}, // invalid type for TIME(MICROS, true).
		// 90.
		{`message foo {
			required double foo (MAP);
		}`, true, false}, // invalid type for MAP.
		{`message foo {
			required double foo (LIST);
		}`, true, false}, // invalid type for LIST.
		{`
message foo { }`, false, false}, // this is necessary because we once had a parser bug when the first character of the parsed text was a newline.
		{`message foo {
			required group bar (MAP) {
				repeated group key_value (MAP_KEY_VALUE) {
					required int64 key;
					required int64 value;
				}
				optional double baz;
			}
		}`, true, false}, // underneath the MAP group there is not only a key_value (MAP_KEY_VALUE), but also the field baz, which should not be there.
		{`message foo {
			required fixed_len_byte_array(100000000000000000000000000000000000000000000000000000000) theid (UUID);
		}`, true, false}, // length couldn't be parsed properly.
		{`message foo {
			required int64 bar = 20000000000000000000000;
		}`, true, false}, // field ID couldn't be parsed properly
		{`message hive_schema {
			optional group foo_list (LIST) {
			  repeated group bag {
				optional binary array_element (STRING);
			  }
			}
		  }
		  `, false, false}, // this is to test the backward-compatibility rules for lists when reading schemas.
		{`message foo {
			optional group foo_list (LIST) {
				repeated int64 data;
			}
		}`, false, false}, // backwards compat rule 1.
		{`message foo {
			optional group foo_list (LIST) {
				repeated group bag {
				}
			}
		}`, true, false}, // empty repeated group child element.
		{`message foo {
			optional group foo_list (LIST) {
				repeated group foobar {
					optional int64 a;
					optional int64 b;
				}
			}
		}`, false, false}, // backwards compat rule 2.
		// 100.
		{`message foo {
			optional group foo_list (LIST) {
				repeated group array {
					optional int64 data;
				}
			}
		}`, false, false}, // backwards compat rule 3.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 foo;
					optional int32 bar;
				}
			}
		}`, false, false},
		{`message foo {
			optional group foo_list (LIST) {
				repeated group array {
					optional int64 data;
				}
			}
		}`, true, true}, // backwards compat rule 3 should fail in strict mode.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 foo;
					optional int32 bar;
				}
			}
		}`, true, true}, // key and value missing.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 key;
				}
			}
		}`, true, true}, // value is missing.
		{`message foo {
			optional group bar (MAP_KEY_VALUE) {
				repeated group map {
					required binary key (UTF8);
					optional int32 value;
				}
			}
		}`, false, false},
		{`message foo {
			optional group bar (MAP_KEY_VALUE) {
				repeated group map {
					required binary key (UTF8);
					optional int32 value;
				}
			}
		}`, true, true}, // incorrectly annotated MAP_KEY_VALUE in strict mode.
		{`message foo {
			optional group bar (MAP) {
				repeated group map {
					required boolean key (STRING);
					optional int32 value;
				}
			}
		}`, true, false}, // type and logical type don't match for key.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					required int64 element (STRING);
				}
			}
		}`, true, false}, // type and logical type don't match for element.
		{`message foo {
			optional group bar (INVALID) {

			}
		}`, false, true}, // invalid ConvertedType
	}

	for idx, tt := range testData {
		p := newSchemaParser(tt.Msg)
		err := p.parse()

		if tt.Strict {
			schemaDef := &SchemaDefinition{RootColumn: p.root}
			err = schemaDef.ValidateStrict()
		}

		if tt.ExpectErr {
			assert.Error(t, err, "%d. expected error, got none; parsed message: %s", idx, spew.Sdump(p.root))
		} else {
			assert.NoError(t, err, "%d. expected no error, got error instead", idx)
		}
	}
}

func TestLineNumber(t *testing.T) {
	msg := `message foo {
		optional group signals (LIST) {
			repeated group list {
			  required group element {
				required binary name (STRING);
				optional binary category (STRING);
				required binary condition (STRING);
				optional binary group (STRING);
				optional binary text (STRING);
				required binary type (ENUM);
				repeated binary highlight (STRING);
				required binary strength (ENUM)
			  }
			}
		  }
	`
	p := newSchemaParser(msg)
	err := p.parse()
	assert.Error(t, err)

	assert.Contains(t, err.Error(), "line 13:")
}

func TestValidate(t *testing.T) {
	testData := []struct {
		schemaDef *SchemaDefinition
		expectErr bool
	}{
		{
			schemaDef: nil,
			expectErr: true,
		},
		{
			schemaDef: &SchemaDefinition{},
			expectErr: true,
		},
		{
			schemaDef: &SchemaDefinition{
				RootColumn: &ColumnDefinition{},
			},
			expectErr: true,
		},
		{
			schemaDef: &SchemaDefinition{
				RootColumn: &ColumnDefinition{
					SchemaElement: &parquet.SchemaElement{},
				},
			},
			expectErr: true,
		},
		{
			schemaDef: &SchemaDefinition{
				RootColumn: &ColumnDefinition{
					SchemaElement: &parquet.SchemaElement{
						Name: "foo",
					},
				},
			},
			expectErr: false,
		},
		{
			schemaDef: &SchemaDefinition{
				RootColumn: &ColumnDefinition{
					SchemaElement: &parquet.SchemaElement{
						Name: "foo",
					},
					Children: []*ColumnDefinition{
						{
							SchemaElement: &parquet.SchemaElement{
								Name: "bar",
							},
						},
					},
				},
			},
			expectErr: true,
		},
		{
			schemaDef: &SchemaDefinition{
				RootColumn: &ColumnDefinition{
					SchemaElement: &parquet.SchemaElement{
						Name: "foo",
					},
					Children: []*ColumnDefinition{
						{
							SchemaElement: &parquet.SchemaElement{
								Name: "bar",
								Type: parquet.TypePtr(parquet.Type_BOOLEAN),
							},
						},
					},
				},
			},
			expectErr: false,
		},
		{
			schemaDef: &SchemaDefinition{
				RootColumn: &ColumnDefinition{
					SchemaElement: &parquet.SchemaElement{
						Name: "foo",
					},
					Children: []*ColumnDefinition{
						{
							SchemaElement: &parquet.SchemaElement{
								Name: "bar",
								Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
							},
							Children: []*ColumnDefinition{
								{
									SchemaElement: &parquet.SchemaElement{
										Name: "baz",
										Type: parquet.TypePtr(parquet.Type_BOOLEAN),
									},
								},
							},
						},
					},
				},
			},
			expectErr: true,
		},
	}

	for idx, tt := range testData {
		err := tt.schemaDef.Validate()
		if tt.expectErr {
			assert.Error(t, err, "%d. validation didn't fail", idx)
		} else {
			assert.NoError(t, err, "%d. validation failed", idx)
		}
		err = tt.schemaDef.ValidateStrict()
		if tt.expectErr {
			assert.Error(t, err, "%d. validation didn't fail", idx)
		} else {
			assert.NoError(t, err, "%d. validation failed", idx)
		}
	}
}
