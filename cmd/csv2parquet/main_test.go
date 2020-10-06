package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTypeHints(t *testing.T) {
	tests := map[string]struct {
		Input          string
		ExpectedOutput map[string]string
		ExpectErr      bool
	}{
		"simple": {
			Input:          "foo=boolean,bar=string",
			ExpectedOutput: map[string]string{"foo": "boolean", "bar": "string"},
		},
		"simply-with-spaces": {
			Input: "   foo  =  boolean ,	bar=string	 ",
			ExpectedOutput: map[string]string{"foo": "boolean", "bar": "string"},
		},
		"empty": {
			Input:          "",
			ExpectedOutput: map[string]string{},
		},
		"invalid-type": {
			Input:     "foo=invalid-type",
			ExpectErr: true,
		},
		"invalid-field": {
			Input:     "foo=boolean=invalid",
			ExpectErr: true,
		},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			output, err := parseTypeHints(tt.Input)
			if tt.ExpectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.ExpectedOutput, output)
			}
		})
	}
}

func TestTypeHandlers(t *testing.T) {
	tests := map[string]struct {
		Input          string
		Func           func(string) (interface{}, error)
		ExpectedOutput interface{}
		ExpectErr      bool
	}{
		"byte-array":        {"hello", byteArrayHandler, []byte("hello"), false},
		"boolean-true":      {"true", booleanHandler, true, false},
		"boolean-false":     {"false", booleanHandler, false, false},
		"boolean-invalid":   {"invalid", booleanHandler, false, true},
		"bool-UPPERCASE":    {"TRUE", booleanHandler, true, false},
		"bool-num-1":        {"1", booleanHandler, true, false},
		"bool-num-0":        {"0", booleanHandler, false, false},
		"uint-32":           {"1234", uintHandler(32), uint32(1234), false},
		"uint-invalid":      {"hello!", uintHandler(32), 0, true},
		"uint-invalid-bits": {"1234", uintHandler(28), 0, true},
		"uint-64":           {"1000000000000", uintHandler(64), uint64(1000000000000), false},
		"int-32":            {"-1234", intHandler(32), int32(-1234), false},
		"int-invalid":       {"goodbye!", intHandler(32), 0, true},
		"int-invalid-bits":  {"1234", intHandler(42), 0, true},
		"int-64":            {"1000000000000", intHandler(64), int64(1000000000000), false},
		"float":             {"3.4", floatHandler, float32(3.4), false},
		"double":            {"4.2", doubleHandler, float64(4.2), false},
		"json-simple":       {`{"hello":"world"}`, jsonHandler, []byte(`{"hello":"world"}`), false},
		"json-invalid":      {`{"hello":"world`, jsonHandler, nil, true},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			output, err := tt.Func(tt.Input)
			if tt.ExpectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.ExpectedOutput, output)
			}
		})
	}
}
