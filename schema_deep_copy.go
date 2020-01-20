package goparquet

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/fraugster/parquet-go/parquet"

	"github.com/apache/thrift/lib/go/thrift"
)

type thriftObject interface {
	thriftReader
	thriftWriter
}

// copyThrift is a hacky approach to copy the thrift object, it uses reflection to create the copy, and the
// thrift serialization/deserialization to copy the data
func copyThrift(in thriftObject) (thriftObject, error) {
	pattern := reflect.TypeOf(in)
	if pattern.Kind() == reflect.Ptr {
		pattern = pattern.Elem()
	}

	if reflect.ValueOf(in).IsNil() {
		return in, nil
	}

	cp := reflect.New(pattern).Elem().Addr().Interface().(thriftObject)
	buf := &bytes.Buffer{}
	if err := writeThrift(in, buf); err != nil {
		return nil, err
	}

	if err := readThrift(cp, bytes.NewReader(buf.Bytes())); err != nil {
		return nil, err
	}

	return cp, nil
}

func copyColumnParameter(in *ColumnParameters) (*ColumnParameters, error) {
	if in == nil {
		return nil, nil
	}
	safeInt32 := func(in *int32) *int32 {
		if in == nil {
			return nil
		}
		return thrift.Int32Ptr(*in)
	}
	out := &ColumnParameters{
		LogicalType: nil,
		TypeLength:  safeInt32(in.TypeLength),
		FieldID:     safeInt32(in.FieldID),
		Scale:       safeInt32(in.Scale),
		Precision:   safeInt32(in.Precision),
	}
	if in.ConvertedType != nil {
		out.ConvertedType = parquet.ConvertedTypePtr(*in.ConvertedType)
	}

	iface, err := copyThrift(in.LogicalType)
	if err != nil {
		return nil, err
	}

	out.LogicalType = iface.(*parquet.LogicalType)
	return out, nil
}

func copyColumnStore(in *ColumnStore) (*ColumnStore, error) {
	if in == nil {
		return nil, nil
	}

	param, err := copyColumnParameter(in.params())
	if err != nil {
		return nil, err
	}
	switch in.typedColumnStore.(type) {
	case *booleanStore:
		return NewBooleanStore(in.enc, param)
	case *int32Store:
		return NewInt32Store(in.enc, in.allowDict, param)
	case *int64Store:
		return NewInt64Store(in.enc, in.allowDict, param)
	case *int96Store:
		return NewInt96Store(in.enc, in.allowDict, param)
	case *floatStore:
		return NewFloatStore(in.enc, in.allowDict, param)
	case *doubleStore:
		return NewFloatStore(in.enc, in.allowDict, param)
	case *byteArrayStore:
		return NewByteArrayStore(in.enc, in.allowDict, param)
	default:
		return nil, fmt.Errorf("invalid type %T", in)
	}
}

func copyStringSlice(in []string) []string {
	out := make([]string, len(in))
	for i := range out {
		out[i] = in[i]
	}

	return out
}

func copyColumn(in *Column) (*Column, error) {
	var err error
	cs, err := copyColumnStore(in.data)
	if err != nil {
		return nil, err
	}

	elem, err := copyThrift(in.element)
	if err != nil {
		return nil, err
	}

	param, err := copyColumnParameter(in.params)
	if err != nil {
		return nil, err
	}

	out := &Column{
		index:    in.index,
		name:     in.name,
		flatName: in.flatName,

		nameArray: copyStringSlice(in.nameArray),
		data:      cs,
		rep:       in.rep,

		maxR: in.maxR,
		maxD: in.maxD,

		parent:  in.parent,
		element: elem.(*parquet.SchemaElement),

		params: param,
	}
	if len(in.children) == 0 {
		return out, nil
	}
	out.children = make([]*Column, len(in.children))

	for i := range in.children {
		out.children[i], err = copyColumn(in.children[i])
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// CopySchema is a helper function to create a deep copy of the schema without messing with the internal data in the schema
// This function uses reflection and is useful for copying the schema from a file to another.
func CopySchema(in *SchemaDefinition) (*SchemaDefinition, error) {
	col, err := copyColumn(in.col)
	if err != nil {
		return nil, err
	}

	return &SchemaDefinition{col: col}, nil
}
