package floor

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/fraugster/parquet-go/parquet"

	goparquet "github.com/fraugster/parquet-go"
)

// NewReader returns a new high-level parquet file reader.
func NewReader(r *goparquet.FileReader) *Reader {
	return &Reader{
		r: r,
	}
}

// NewFileReader returns a new high-level parquet file reader
// that directly reads from the provided file.
func NewFileReader(file string) (*Reader, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	r, err := goparquet.NewFileReader(f)
	if err != nil {
		return nil, err
	}

	return &Reader{
		r: r,
		f: f,
	}, nil
}

// Reader represents a high-level reader for parquet files.
type Reader struct {
	r *goparquet.FileReader
	f io.Closer

	initialized  bool
	currentGroup int
	currentRow   int64
	data         map[string]interface{}
	err          error
	eof          bool
}

// Close closes the reader.
func (r *Reader) Close() error {
	if r.f != nil {
		return r.f.Close()
	}

	return nil
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

	schemaDef := r.r.GetSchemaDefinition()

	if err := fillStruct(objValue, r.data, schemaDef); err != nil {
		return err
	}

	return nil
}

func fillStruct(value reflect.Value, data map[string]interface{}, schemaDef *goparquet.SchemaDefinition) error {
	typ := value.Type()

	numFields := typ.NumField()
	for i := 0; i < numFields; i++ {
		fieldValue := value.Field(i)

		fieldName := strings.ToLower(typ.Field(i).Name)

		fieldSchemaDef := schemaDef.SubSchema(fieldName)

		fieldData, ok := data[fieldName]
		if !ok {
			if elem := fieldSchemaDef.SchemaElement(); elem.GetRepetitionType() == parquet.FieldRepetitionType_REQUIRED {
				return fmt.Errorf("field %s is %s but couldn't be found in data", fieldName, elem.GetRepetitionType())
			}
			continue
		}

		if err := fillValue(fieldValue, fieldData, fieldSchemaDef); err != nil {
			return err
		}
	}

	return nil
}

func fillValue(value reflect.Value, data interface{}, schemaDef *goparquet.SchemaDefinition) error {
	if value.Kind() == reflect.Ptr {
		value.Set(reflect.New(value.Type().Elem()))
		value = value.Elem()
	}

	if value.Type().ConvertibleTo(reflect.TypeOf(time.Time{})) {
		if schemaDef.SchemaElement().LogicalType != nil && schemaDef.SchemaElement().GetLogicalType().IsSetDATE() {
			i, err := getIntValue(data)
			if err != nil {
				return err
			}

			date := time.Unix(0, 0).UTC().Add(24 * time.Hour * time.Duration(i))
			value.Set(reflect.ValueOf(date))
			return nil
		}
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
		u, err := getIntValue(data)
		if err != nil {
			return err
		}
		value.SetUint(uint64(u))
	case reflect.Float32, reflect.Float64:
		f, err := getFloatValue(data)
		if err != nil {
			return err
		}
		value.SetFloat(f)
	case reflect.Array, reflect.Slice:
		return fillArrayOrSlice(value, data, schemaDef)
	case reflect.Map:
		return fillMap(value, data, schemaDef)
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
		if err := fillStruct(value, structData, schemaDef); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported type %s", value.Type())
	}

	return nil
}

func fillMap(value reflect.Value, data interface{}, schemaDef *goparquet.SchemaDefinition) error {
	if elem := schemaDef.SchemaElement(); elem.GetConvertedType() != parquet.ConvertedType_MAP {
		return fmt.Errorf("filling map but schema element %s is not annotated as MAP", elem.GetName())
	}

	mapData, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected map[string]interface{} to fill map, got %T", data)
	}
	keyValueList, ok := mapData["key_value"].([]map[string]interface{})
	if !ok {
		return fmt.Errorf("expected key_value child of type []map[string]interface{}, got %T", mapData["key_value"])
	}
	value.Set(reflect.MakeMap(value.Type()))

	keyValueSchemaDef := schemaDef.SubSchema("key_value")
	keySchemaDef := keyValueSchemaDef.SubSchema("key")
	valueSchemaDef := keyValueSchemaDef.SubSchema("value")

	for _, keyValueMap := range keyValueList {
		keyData, ok := keyValueMap["key"]
		if !ok {
			return errors.New("got key_value element without key")
		}

		keyValue := reflect.New(value.Type().Key()).Elem()
		if err := fillValue(keyValue, keyData, keySchemaDef); err != nil {
			return fmt.Errorf("couldn't fill key with key data: %v", err)
		}

		valueData, ok := keyValueMap["value"]
		if !ok {
			return errors.New("got key_value element without value")
		}
		valueValue := reflect.New(value.Type().Elem()).Elem()
		if err := fillValue(valueValue, valueData, valueSchemaDef); err != nil {
			return fmt.Errorf("couldn't fill value with value data: %v", err)
		}

		value.SetMapIndex(keyValue, valueValue)
	}

	return nil
}

func fillArrayOrSlice(value reflect.Value, data interface{}, schemaDef *goparquet.SchemaDefinition) error {
	if elem := schemaDef.SchemaElement(); elem.GetConvertedType() != parquet.ConvertedType_LIST {
		return fmt.Errorf("filling slice or array but schema element %s is not annotated as LIST", elem.GetName())
	}

	listData, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected map[string]interface{} to fill list, got %T", data)
	}
	elemList, ok := listData["list"].([]map[string]interface{})
	if !ok {
		return fmt.Errorf("expected list child of type []map[string]interface{}, got %T", listData["list"])
	}
	elementList := []interface{}{}
	for _, elemMap := range elemList {
		elem, ok := elemMap["element"]
		if !ok {
			return errors.New("got list element without element child")
		}
		elementList = append(elementList, elem)
	}
	if value.Kind() == reflect.Slice {
		value.Set(reflect.MakeSlice(value.Type(), len(elementList), len(elementList)))
	}

	listSchemaDef := schemaDef.SubSchema("list")
	elemSchemaDef := listSchemaDef.SubSchema("element")

	for idx, elem := range elementList {
		if idx < value.Len() {
			if err := fillValue(value.Index(idx), elem, elemSchemaDef); err != nil {
				return err
			}
		}
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

// GetSchemaDefinition returns the schema definition of the parquet
// file.
func (r *Reader) GetSchemaDefinition() *goparquet.SchemaDefinition {
	return r.r.GetSchemaDefinition()
}
