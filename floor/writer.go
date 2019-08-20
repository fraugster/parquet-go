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
		mapData, err := decodeMap(value)
		if err != nil {
			return nil, err
		}
		return mapData, nil
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
	if value.Kind() == reflect.Slice && value.IsNil() {
		return nil, nil
	}

	data := map[string]interface{}{}

	list := []map[string]interface{}{}

	for i := 0; i < value.Len(); i++ {
		v, err := decodeValue(value.Index(i))
		if err != nil {
			return nil, err
		}
		list = append(list, map[string]interface{}{"element": v})
	}

	data["list"] = list

	return data, nil
}

func decodeMap(value reflect.Value) (interface{}, error) {
	if value.IsNil() {
		return nil, nil
	}

	data := map[string]interface{}{}

	keyValueList := []map[string]interface{}{}

	iter := value.MapRange()

	for iter.Next() {
		key, err := decodeValue(iter.Key())
		if err != nil {
			return nil, err
		}

		value, err := decodeValue(iter.Value())
		if err != nil {
			return nil, err
		}

		keyValueList = append(keyValueList, map[string]interface{}{"key": key, "value": value})
	}

	data["key_value"] = keyValueList

	return data, nil
}

// Close flushes outstanding data and closes the underlying
// parquet writer.
func (w *Writer) Close() error {
	return w.w.Close()
}
