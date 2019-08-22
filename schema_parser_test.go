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
		// 0.
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
		}`, false},
		// 10.
		{`message $ { }`, true},                              // $ is not the start of a valid token.
		{`message foo { optional int128 bar; }`, true},       // invalid type
		{`message foo { optional int64 bar (BLUB); }`, true}, // invalid logical type
		{`message foo { optional int32 bar; }`, false},
		{`message foo { optional double bar; }`, false},
		{`message foo { optional float bar; }`, false},
		{`message foo { optional int96 bar; }`, false},
		{`message foo {
			required group ids (LIST) {
				repeated group list {
					required int64 element;
				}
			}
		}`, false},
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
		}`, false},
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int32 key;
					required int32 value;
				}
			}
		}`, false},
		// 20.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					required int64 element;
				}
			}
		}`, false},
		{`message foo {
			optional group bar (LIST) {
				repeated group element {
					required int64 element;
				}
			}
		}`, true}, // repeated group is called "element", not "list".
		{`message foo {
			optional group bar (LIST) {
				repeated int64 list;
			}
		}`, true}, // repeated list is not a group.
		{`message foo {
			repeated group bar (LIST) {
				repeated group list {
					optional int64 element;
				}
			}
		}`, true}, // bar is LIST but has repetition type repeated.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					optional int64 element;
					optional int64 element2;
				}
			}
		}`, true}, //bar.list has 2 children.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					optional int64 invalid;
				}
			}
		}`, true}, //bar.list has 1 child, but it's called invalid, not element.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					repeated int64 element;
				}
			}
		}`, true}, //bar.list.element is of the wrong repetition type.
		{`message foo {
			optional group bar (LIST) {
				repeated group list {
					required int64 baz;
				}
				optional int64 list_size;
			}
		}`, true}, // only element underneath (LIST) allowed is repeated group list; list_size is invalid.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 key;
					optional int32 value;
				}
			}
		}`, false},
		{`message foo {
			optional group bar (MAP) {
				repeated group stuff {
					required int64 key;
					optional int32 value;
				}
			}
		}`, true}, // repeated group underneath (MAP) is not called key_value.
		// 30.
		{`message foo {
			optional group bar (MAP) {
				repeated int64 key_value;
			}
		}`, true}, // repeated key_value is not a group.
		{`message foo {
			optional group bar (MAP) {
			}
		}`, true}, // empty group bar.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 key;
					optional int32 value;
					optional int32 another_value;
				}
			}
		}`, true}, // inside key_value, only key and value are allowed.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					optional int64 key;
					optional int32 value;
				}
			}
		}`, true}, // bar.key_value.key must be required.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 key;
				}
			}
		}`, true}, // bar.key_value.value is missing.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 key;
					optional int32 key;
				}
			}
		}`, true}, // bar.key_value has 2 children but child value is missing.
		{`message foo {
			optional group bar (MAP) {
				repeated group key_value {
					required int64 value;
					optional int32 value;
				}
			}
		}`, true}, // bar.key_value has 2 children but child key is missing.
		{`message foo {
			required int32 date (DATE);
		}`, false},
		{`message foo {
			required int64 date (DATE);
		}`, true}, // date is annotated as DATE but data type is int64.
		{`message foo {
			required int64 ts (TIMESTAMP(MILLIS, true));
		}`, false},
		{`message foo {
			required int64 ts (TIMESTAMP(,));
		}`, true}, // invalid annotation syntax for TIMESTAMP.
	}

	for idx, tt := range testData {
		p := newSchemaParser(tt.Msg)
		err := p.parse()

		if tt.ExpectErr {
			assert.Error(t, err, "%d. expected error, got none; parsed message: %s", idx, spew.Sdump(p.root))
		} else {
			assert.NoError(t, err, "%d. expected no error, got error instead", idx)
		}
		//t.Logf("%d. msg = %s expect err = %t err = %v ; parsed message: %s", idx, tt.Msg, tt.ExpectErr, err, spew.Sdump(p.root))
	}
}
