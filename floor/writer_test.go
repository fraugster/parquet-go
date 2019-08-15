package floor

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeStruct(t *testing.T) {
	testData := []struct {
		Input          interface{}
		ExpectedOutput map[string]interface{}
		ExpectErr      bool
	}{
		{
			Input:          struct{ Foo int16 }{Foo: 42},
			ExpectedOutput: map[string]interface{}{"foo": int32(42)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo int }{Foo: 43},
			ExpectedOutput: map[string]interface{}{"foo": int32(43)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo int8 }{Foo: 44},
			ExpectedOutput: map[string]interface{}{"foo": int32(44)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo int32 }{Foo: 100000},
			ExpectedOutput: map[string]interface{}{"foo": int32(100000)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo uint64 }{Foo: 1125899906842624},
			ExpectedOutput: map[string]interface{}{"foo": int64(1125899906842624)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo uint }{Foo: 200000},
			ExpectedOutput: map[string]interface{}{"foo": int32(200000)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo float32 }{Foo: 42.5},
			ExpectedOutput: map[string]interface{}{"foo": float32(42.5)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo float64 }{Foo: 23.5},
			ExpectedOutput: map[string]interface{}{"foo": float64(23.5)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo byte }{Foo: 1},
			ExpectedOutput: map[string]interface{}{"foo": int32(1)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo string }{Foo: "bar"},
			ExpectedOutput: map[string]interface{}{"foo": "bar"},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo *string }{Foo: new(string)},
			ExpectedOutput: map[string]interface{}{"foo": ""},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo *string }{},
			ExpectedOutput: map[string]interface{}{},
			ExpectErr:      false,
		},
		{
			Input:          int(23),
			ExpectedOutput: nil,
			ExpectErr:      true,
		},
		{
			Input: struct {
				Foo struct {
					Bar int64
				}
				Baz  uint32
				Quux *bool
				Blub bool
			}{},
			ExpectedOutput: map[string]interface{}{"foo": map[string]interface{}{"bar": int64(0)}, "baz": int64(0), "blub": false},
			ExpectErr:      false,
		},
		{
			Input: struct {
				Foo []bool
			}{
				Foo: []bool{false, true, false},
			},
			ExpectedOutput: map[string]interface{}{"foo": []interface{}{false, true, false}},
			ExpectErr:      false,
		},
		{
			Input: struct {
				Foo [5]uint16
			}{
				Foo: [5]uint16{1, 1, 2, 3, 5},
			},
			ExpectedOutput: map[string]interface{}{"foo": []interface{}{int32(1), int32(1), int32(2), int32(3), int32(5)}},
			ExpectErr:      false,
		},
		{
			Input: struct {
				C chan int
			}{},
			ExpectedOutput: nil,
			ExpectErr:      true,
		},
		{
			Input: struct {
				Foo struct {
					C chan int
				}
			}{},
			ExpectedOutput: nil,
			ExpectErr:      true,
		},
		{
			Input: struct {
				Foo []chan int
			}{Foo: []chan int{make(chan int)}},
			ExpectedOutput: nil,
			ExpectErr:      true,
		},
		{
			Input: &struct {
				Bla int
			}{Bla: 616},
			ExpectedOutput: map[string]interface{}{"bla": int32(616)},
			ExpectErr:      false,
		},
		{
			Input: (*struct {
				Bla int
			})(nil),
			ExpectedOutput: nil,
			ExpectErr:      true,
		},
	}

	for idx, tt := range testData {
		output, err := decodeStruct(reflect.ValueOf(tt.Input))
		if tt.ExpectErr {
			assert.Error(t, err, "%d. expected error, but found none", idx)
		} else {
			assert.NoError(t, err, "%d. expected no error, but found one", idx)
			assert.Equal(t, tt.ExpectedOutput, output, "%d. output mismatch", idx)
		}
	}
}
