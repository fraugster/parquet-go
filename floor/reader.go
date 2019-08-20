package floor

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	goparquet "github.com/fraugster/parquet-go"
)

// NewReader returns a new high-level parquet file reader.
func NewReader(r *goparquet.FileReader) *Reader {
	return &Reader{
		r: r,
	}
}

// Reader represents a high-level reader for parquet files.
type Reader struct {
	r *goparquet.FileReader

	initialized  bool
	currentGroup int
	currentRow   int64
	data         map[string]interface{}
	err          error
	eof          bool
}

// Next reads the next object so that it is ready to be scanned.
// Returns true if fetching the next object was successful, false
// otherwise, e.g. in case of an error or when EOF was reached.
func (r *Reader) Next() bool {
	if r.eof {
		return false
	}

	r.init()
	if r.err != nil {
		return false
	}

	r.readNext()
	if r.err != nil || r.eof {
		return false
	}

	return true
}

func (r *Reader) readNext() {
	if r.currentRow == r.r.NumRecords() {

		if r.currentGroup == r.r.RawGroupCount() {
			r.eof = true
			r.err = nil
			return
		}

		r.err = r.r.ReadRowGroup()
		if r.err != nil {
			return
		}
		r.currentGroup++
		r.currentRow = 0
	}

	r.data, r.err = r.r.GetData()
	if r.err != nil {
		return
	}
	r.currentRow++
}

func (r *Reader) init() {
	if r.initialized {
		return
	}
	r.initialized = true

	r.err = r.r.ReadRowGroup()
	if r.err != nil {
		return
	}
	r.currentGroup++
}

// Scan fills obj with the data from the record last fetched.
// Returns an error if there is no data available or if the
// structure of obj doesn't fit the data. obj needs to be
// a pointer to an object.
func (r *Reader) Scan(obj interface{}) error {
	objValue := reflect.ValueOf(obj)

	if objValue.Kind() != reflect.Ptr {
		return errors.New("you didn't provide a pointer to an object") // TODO: improve error message
	}

	objValue = objValue.Elem()
	if objValue.Kind() != reflect.Struct {
		return errors.New("provided object is not a struct")
	}

	if err := fillStruct(objValue, r.data); err != nil {
		return err
	}

	return nil
}

func fillStruct(value reflect.Value, data map[string]interface{}) error {
	typ := value.Type()

	numFields := typ.NumField()
	for i := 0; i < numFields; i++ {
		fieldValue := value.Field(i)

		fieldName := strings.ToLower(typ.Field(i).Name)

		fieldData, ok := data[fieldName]
		if !ok {
			// TODO: couple this somehow with schema and check whether field is required.
			continue
		}

		if err := fillValue(fieldValue, fieldData); err != nil {
			return err
		}
	}

	return nil
}

func fillValue(value reflect.Value, data interface{}) error {
	if value.Kind() == reflect.Ptr {
		value.Set(reflect.New(value.Type().Elem()))
		value = value.Elem()
	}

	switch value.Kind() {
	case reflect.Bool:
		b, err := getBoolValue(data)
		if err != nil {
			return err
		}
		value.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := getIntValue(data)
		if err != nil {
			return err
		}
		value.SetInt(i)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u, err := getUintValue(data)
		if err != nil {
			return err
		}
		value.SetUint(u)
	case reflect.Float32, reflect.Float64:
		f, err := getFloatValue(data)
		if err != nil {
			return err
		}
		value.SetFloat(f)
	case reflect.Array, reflect.Slice:

	case reflect.Map:

	case reflect.String:
		s, err := getStringValue(data)
		if err != nil {
			return err
		}
		value.SetString(s)
	case reflect.Struct:
		structData, ok := data.(map[string]interface{})
		if !ok {
			return fmt.Errorf("expected map[string]interface{} to fill struct, got %T", data)
		}
		if err := fillStruct(value, structData); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported type %s", value.Type())
	}

	return nil
}

func getBoolValue(data interface{}) (bool, error) {
	b, ok := data.(bool)
	if !ok {
		return false, fmt.Errorf("expected bool, got %T", data)
	}
	return b, nil
}

func getStringValue(data interface{}) (string, error) {
	s, ok := data.(string)
	if !ok {
		return "", fmt.Errorf("expected string, got %T", data)
	}

	return s, nil
}

func getIntValue(data interface{}) (int64, error) {
	switch x := data.(type) {
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	default:
		return 0, fmt.Errorf("expected integer, got %T", data)
	}
}

func getUintValue(data interface{}) (uint64, error) {
	switch x := data.(type) {
	case uint:
		return uint64(x), nil
	case uint8:
		return uint64(x), nil
	case uint16:
		return uint64(x), nil
	case uint32:
		return uint64(x), nil
	case uint64:
		return x, nil
	default:
		return 0, fmt.Errorf("expected unsigned integer, got %T", data)
	}
}

func getFloatValue(data interface{}) (float64, error) {
	switch x := data.(type) {
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	default:
		return 0, fmt.Errorf("expected float, got %T", data)
	}
}

// Err returns an error in case Next returned false due to an error.
// If Next returned false due to EOF, Err returns nil.
func (r *Reader) Err() error {
	return r.err
}

// Close closes the reader including the underlying object.
func (r *Reader) Close() error {
	// TODO: implement
	return nil
}
