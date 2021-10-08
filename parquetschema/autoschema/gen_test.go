package autoschema

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestGenerateSchema(t *testing.T) {

	tests := map[string]struct {
		Input          interface{}
		ExpectErr      bool
		ExpectedOutput string
	}{
		"simple types": {
			Input: struct {
				Foo  int
				Bar  uint
				Baz  string
				Quux float32
				Bla  float64
				Abc  int8
				Def  int16
				Ghi  int32
				Jkl  int64
				Mno  uint8
				Pqr  uint16
				Stu  uint32
				Vwx  uint64
				Yz   bool
			}{},
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required int32 foo (INT(32, true));\n  required int32 bar (INT(32, false));\n  required binary baz (STRING);\n  required float quux;\n  required double bla;\n  required int32 abc (INT(8, true));\n  required int32 def (INT(16, true));\n  required int32 ghi (INT(32, true));\n  required int64 jkl (INT(64, true));\n  required int32 mno (INT(8, false));\n  required int32 pqr (INT(16, false));\n  required int32 stu (INT(32, false));\n  required int64 vwx (INT(64, false));\n  required boolean yz;\n}\n",
		},
		"optional type": {
			Input: struct {
				Foo *int
			}{},
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  optional int32 foo (INT(32, true));\n}\n",
		},
		"struct pointer": {
			Input: (*struct {
				Foo int
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required int32 foo (INT(32, true));\n}\n",
		},
		"structs within struct": {
			Input: (*struct {
				Foo *struct {
					Bar int32
				}
				Baz struct {
					Quux int64
				}
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  optional group foo {\n    required int32 bar (INT(32, true));\n  }\n  required group baz {\n    required int64 quux (INT(64, true));\n  }\n}\n",
		},
		"slices": {
			Input: (*struct {
				Foo []int
				Bar []*int
				Baz []struct {
					Quux int
				}
				Bla []*struct {
					Fasel *int
				}
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required group foo (LIST) {\n    repeated group list {\n      required int32 element (INT(32, true));\n    }\n  }\n  optional group bar (LIST) {\n    repeated group list {\n      required int32 element (INT(32, true));\n    }\n  }\n  required group baz (LIST) {\n    repeated group list {\n      required group element {\n        required int32 quux (INT(32, true));\n      }\n    }\n  }\n  optional group bla (LIST) {\n    repeated group list {\n      required group element {\n        optional int32 fasel (INT(32, true));\n      }\n    }\n  }\n}\n",
		},
		"byte slices": {
			Input: (*struct {
				Foo []byte
				Bar *[]byte
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required binary foo;\n  optional binary bar;\n}\n",
		},
		"struct tags": {
			Input: (*struct {
				Foo []byte  `parquet:"foobar"`
				Bar *[]byte `parquet:"barman"`
				Baz int64   `parquet:"bazinga"`
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required binary foobar;\n  optional binary barman;\n  required int64 bazinga (INT(64, true));\n}\n",
		},
		"byte array": {
			Input: (*struct {
				Foo [23]byte
				Bar *[42]byte
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required fixed_len_byte_array(23) foo;\n  optional fixed_len_byte_array(42) bar;\n}\n",
		},
		"simple map": {
			Input: (*struct {
				Foo map[string]int64
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  optional group foo (MAP) {\n    repeated group key_value (MAP_KEY_VALUE) {\n      required binary key (STRING);\n      required int64 value (INT(64, true));\n    }\n  }\n}\n",
		},
		"chan": {
			Input: (*struct {
				Foo chan int
			})(nil),
			ExpectErr: true,
		},
		"func": {
			Input: (*struct {
				Foo func()
			})(nil),
			ExpectErr: true,
		},
		"interface": {
			Input: (*struct {
				Foo interface{}
			})(nil),
			ExpectErr: true,
		},
		"unsafe.Pointer": {
			Input: (*struct {
				Foo unsafe.Pointer
			})(nil),
			ExpectErr: true,
		},
		"complex64": {
			Input: (*struct {
				Foo complex64
			})(nil),
			ExpectErr: true,
		},
		"complex128": {
			Input: (*struct {
				Foo complex128
			})(nil),
			ExpectErr: true,
		},
		"uintptr": {
			Input: (*struct {
				Foo uintptr
			})(nil),
			ExpectErr: true,
		},
		"invalid struct within struct": {
			Input: (*struct {
				Foo struct {
					Bar uintptr
				}
			})(nil),
			ExpectErr: true,
		},
		"invalid slice": {
			Input: (*struct {
				Foo []chan int
			})(nil),
			ExpectErr: true,
		},
		"invalid pointer": {
			Input: (*struct {
				Foo *complex128
			})(nil),
			ExpectErr: true,
		},
		"invalid map key": {
			Input: (*struct {
				Foo map[complex128]string
			})(nil),
			ExpectErr: true,
		},
		"invalid map value": {
			Input: (*struct {
				Foo map[string]complex64
			})(nil),
			ExpectErr: true,
		},
		"non-struct input": {
			Input:     int64(42),
			ExpectErr: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			output, err := GenerateSchema(testData.Input)
			if testData.ExpectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testData.ExpectedOutput, output.String())
			}
		})
	}
}
