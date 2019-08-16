package floor

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	goparquet "github.com/fraugster/parquet-go"
)

// NewWriter creates a new high-level writer for parquet.
func NewWriter(w *goparquet.FileWriter) *Writer {
	return &Writer{
		w: w,
	}
}

// Writer represents a high-level writer for parquet files.
type Writer struct {
	w *goparquet.FileWriter
}

// Write adds a new object to be written to the parquet file.
func (w *Writer) Write(obj interface{}) error {
	value := reflect.ValueOf(obj)

	data, err := decodeStruct(value)
	if err != nil {
		return err
	}

	if err := w.w.AddData(data); err != nil {
		return err
	}

	return nil
}

func decodeStruct(value reflect.Value) (map[string]interface{}, error) {
	if value.Type().Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil, errors.New("object is nil")
		}
		value = value.Elem()
	}

	typ := value.Type()

	if typ.Kind() != reflect.Struct {
		return nil, errors.New("object needs to be a struct or a *struct")
	}

	data := make(map[string]interface{})

	numFields := typ.NumField()
	for i := 0; i < numFields; i++ {
		fieldValue := value.Field(i)

		fieldName := strings.ToLower(typ.Field(i).Name) // TODO: derive field name differently.

		v, err := decodeValue(fieldValue)
		if err != nil {
			return nil, err
		}

		if v != nil {
			data[fieldName] = v
		}

	}

	return data, nil
}

func decodeValue(value reflect.Value) (interface{}, error) {
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil, nil
		}
		value = value.Elem()
	}

	switch value.Kind() {
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.Int:
		return int32(value.Int()), nil
	case reflect.Int8:
		return int32(value.Int()), nil
	case reflect.Int16:
		return int32(value.Int()), nil
	case reflect.Int32:
		return int32(value.Int()), nil
	case reflect.Int64:
		return value.Int(), nil
	case reflect.Uint:
		return int32(value.Uint()), nil
	case reflect.Uint8:
		return int32(value.Uint()), nil
	case reflect.Uint16:
		return int32(value.Uint()), nil
	case reflect.Uint32:
		return int64(value.Uint()), nil
	case reflect.Uint64:
		return int64(value.Uint()), nil // TODO: a uint64 doesn't necessarily fit in an int64
	case reflect.Float32:
		return float32(value.Float()), nil
	case reflect.Float64:
		return value.Float(), nil
	case reflect.Array, reflect.Slice:
		return decodeSliceOrArray(value)
	case reflect.Map:
		return nil, errors.New("map support not implemented yet")
	case reflect.String:
		return value.String(), nil
	case reflect.Struct:
		structData, err := decodeStruct(value)
		if err != nil {
			return nil, err
		}
		return structData, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", value.Type())
	}
}

func decodeSliceOrArray(value reflect.Value) (interface{}, error) {
	containedType := value.Type()
	mappedType, err := mapType(containedType.Elem())
	if err != nil {
		return nil, err
	}
	slice := reflect.MakeSlice(reflect.SliceOf(mappedType), 0, value.Len())

	for j := 0; j < value.Len(); j++ {
		v, err := decodeValue(value.Index(j))
		if err != nil {
			return nil, err
		}
		slice = reflect.Append(slice, reflect.ValueOf(v))
	}
	return slice.Interface(), nil
}

func mapType(typ reflect.Type) (reflect.Type, error) {
	switch typ.Kind() {
	case reflect.Bool:
		return reflect.TypeOf(false), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16:
		return reflect.TypeOf(int32(0)), nil
	case reflect.Int64, reflect.Uint32, reflect.Uint64:
		return reflect.TypeOf(int64(0)), nil
	case reflect.Float32:
		return reflect.TypeOf(float32(0)), nil
	case reflect.Float64:
		return reflect.TypeOf(float64(0)), nil
	case reflect.Struct:
		return reflect.TypeOf(map[string]interface{}{}), nil
	case reflect.Array, reflect.Slice:
		mappedType, err := mapType(typ.Elem())
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(mappedType), nil
	case reflect.Map:
		return nil, errors.New("map type support not implemented yet")
	case reflect.String:
		return typ, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", typ)
	}
}

// Close flushes outstanding data and closes the underlying
// parquet writer.
func (w *Writer) Close() error {
	return w.w.Close()
}
