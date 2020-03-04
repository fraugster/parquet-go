package floor

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
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

	data map[string]interface{}
	err  error
	eof  bool
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
	r.data, r.err = r.r.NextRow()
	if r.err == io.EOF {
		r.eof = true
		r.err = nil
		return false
	}
	if r.err != nil {
		return false
	}

	return true
}

// Scan fills obj with the data from the record last fetched.
// Returns an error if there is no data available or if the
// structure of obj doesn't fit the data. obj needs to be
// a pointer to an object, or alternatively implement the
// Unmarshaller interface.
func (r *Reader) Scan(obj interface{}) error {
	um, ok := obj.(interfaces.Unmarshaller)
	if !ok {
		um = &reflectUnmarshaller{obj: obj, schemaDef: r.r.GetSchemaDefinition()}
	}

	return um.UnmarshalParquet(interfaces.NewUnmarshallObject(r.data))
}

type reflectUnmarshaller struct {
	obj       interface{}
	schemaDef *parquetschema.SchemaDefinition
}

func (um *reflectUnmarshaller) UnmarshalParquet(record interfaces.UnmarshalObject) error {
	objValue := reflect.ValueOf(um.obj)

	if objValue.Kind() != reflect.Ptr {
		return fmt.Errorf("you need to provide an object of type *%T to unmarshal into", um.obj)
	}

	objValue = objValue.Elem()
	if objValue.Kind() != reflect.Struct {
		return fmt.Errorf("provided object of type %T is not a struct", um.obj)
	}

	if err := um.fillStruct(objValue, record, um.schemaDef); err != nil {
		return err
	}

	return nil
}

func (um *reflectUnmarshaller) fillStruct(value reflect.Value, record interfaces.UnmarshalObject, schemaDef *parquetschema.SchemaDefinition) error {
	typ := value.Type()

	numFields := typ.NumField()
	for i := 0; i < numFields; i++ {
		fieldValue := value.Field(i)

		fieldName := fieldNameFunc(typ.Field(i))

		fieldSchemaDef := schemaDef.SubSchema(fieldName)

		if fieldSchemaDef == nil {
			continue
		}

		fieldData := record.GetField(fieldName)
		if fieldData.Error() != nil {
			if elem := fieldSchemaDef.SchemaElement(); elem.GetRepetitionType() == parquet.FieldRepetitionType_REQUIRED {
				return fmt.Errorf("field %s is %s but couldn't be found in data", fieldName, elem.GetRepetitionType())
			}
			continue
		}

		if err := um.fillValue(fieldValue, fieldData, fieldSchemaDef); err != nil {
			return err
		}
	}

	return nil
}

func (um *reflectUnmarshaller) fillValue(value reflect.Value, data interfaces.UnmarshalElement, schemaDef *parquetschema.SchemaDefinition) error {
	if value.Kind() == reflect.Ptr {
		value.Set(reflect.New(value.Type().Elem()))
		value = value.Elem()
	}

	if !value.CanSet() {
		return nil
	}

	if value.Type().ConvertibleTo(reflect.TypeOf(Time{})) {
		if elem := schemaDef.SchemaElement(); elem.LogicalType != nil {
			if elem.GetLogicalType().IsSetTIME() {
				i, err := getIntValue(data)
				if err != nil {
					return err
				}

				var t Time
				switch {
				case elem.GetLogicalType().TIME.Unit.IsSetNANOS():
					t = TimeFromNanoseconds(i)
				case elem.GetLogicalType().TIME.Unit.IsSetMICROS():
					t = TimeFromMicroseconds(i)
				case elem.GetLogicalType().TIME.Unit.IsSetMILLIS():
					t = TimeFromMilliseconds(int32(i))
				default:
					return errors.New("invalid TIME unit")
				}

				if elem.GetLogicalType().TIME.GetIsAdjustedToUTC() {
					t = t.UTC()
				}

				value.Set(reflect.ValueOf(t))
				return nil
			}
		}
	}

	if value.Type().ConvertibleTo(reflect.TypeOf(time.Time{})) {
		if elem := schemaDef.SchemaElement(); elem.LogicalType != nil {
			switch {
			case elem.GetLogicalType().IsSetDATE():
				i, err := getIntValue(data)
				if err != nil {
					return err
				}

				date := time.Unix(0, 0).UTC().Add(24 * time.Hour * time.Duration(i))
				value.Set(reflect.ValueOf(date))
				return nil
			case elem.GetLogicalType().IsSetTIMESTAMP():
				i, err := getIntValue(data)
				if err != nil {
					return err
				}

				var ts time.Time
				switch {
				case elem.GetLogicalType().TIMESTAMP.Unit.IsSetNANOS():
					ts = time.Unix(i/1000000000, i%1000000000)
				case elem.GetLogicalType().TIMESTAMP.Unit.IsSetMICROS():
					ts = time.Unix(i/1000000, 1000*(i%1000000))
				case elem.GetLogicalType().TIMESTAMP.Unit.IsSetMILLIS():
					ts = time.Unix(i/1000, 1000000*(i%1000))
				default:
					return errors.New("invalid TIMESTAMP unit")
				}

				if elem.GetLogicalType().TIMESTAMP.GetIsAdjustedToUTC() {
					ts = ts.UTC()
				}

				value.Set(reflect.ValueOf(ts))
				return nil
			}
		}
	}

	switch value.Kind() {
	case reflect.Bool:
		b, err := data.Bool()
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
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return um.fillByteArrayOrSlice(value, data, schemaDef)
		}
		return um.fillArrayOrSlice(value, data, schemaDef)
	case reflect.Map:
		return um.fillMap(value, data, schemaDef)
	case reflect.String:
		s, err := data.ByteArray()
		if err != nil {
			return err
		}
		value.SetString(string(s))
	case reflect.Struct:
		groupData, err := data.Group()
		if err != nil {
			return err
		}
		if err := um.fillStruct(value, groupData, schemaDef); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported type %s", value.Type())
	}

	return nil
}

func (um *reflectUnmarshaller) fillMap(value reflect.Value, data interfaces.UnmarshalElement, schemaDef *parquetschema.SchemaDefinition) error {
	if elem := schemaDef.SchemaElement(); elem.GetConvertedType() != parquet.ConvertedType_MAP {
		return fmt.Errorf("filling map but schema element %s is not annotated as MAP", elem.GetName())
	}

	keyValueList, err := data.Map()
	if err != nil {
		return err
	}

	value.Set(reflect.MakeMap(value.Type()))

	keyValueSchemaDef := schemaDef.SubSchema("key_value")
	keySchemaDef := keyValueSchemaDef.SubSchema("key")
	valueSchemaDef := keyValueSchemaDef.SubSchema("value")

	for keyValueList.Next() {
		key, err := keyValueList.Key()
		if err != nil {
			return err
		}

		valueData, err := keyValueList.Value()
		if err != nil {
			return err
		}

		keyValue := reflect.New(value.Type().Key()).Elem()
		if err := um.fillValue(keyValue, key, keySchemaDef); err != nil {
			return fmt.Errorf("couldn't fill key with key data: %v", err)
		}

		valueValue := reflect.New(value.Type().Elem()).Elem()
		if err := um.fillValue(valueValue, valueData, valueSchemaDef); err != nil {
			return fmt.Errorf("couldn't fill value with value data: %v", err)
		}

		value.SetMapIndex(keyValue, valueValue)
	}

	return nil
}

func (um *reflectUnmarshaller) fillByteArrayOrSlice(value reflect.Value, data interfaces.UnmarshalElement, schemaDef *parquetschema.SchemaDefinition) error {
	byteSlice, err := data.ByteArray()
	if err != nil {
		return err
	}
	if value.Kind() == reflect.Slice {
		value.Set(reflect.MakeSlice(value.Type(), len(byteSlice), len(byteSlice)))
	}

	for i, b := range byteSlice {
		if i < value.Len() {
			value.Index(i).SetUint(uint64(b))
		}
	}
	return nil
}

func (um *reflectUnmarshaller) fillArrayOrSlice(value reflect.Value, data interfaces.UnmarshalElement, schemaDef *parquetschema.SchemaDefinition) error {
	if elem := schemaDef.SchemaElement(); elem.GetConvertedType() != parquet.ConvertedType_LIST {
		return fmt.Errorf("filling slice or array but schema element %s is not annotated as LIST", elem.GetName())
	}

	elemList, err := data.List()
	if err != nil {
		return err
	}

	elementList := []interfaces.UnmarshalElement{}

	for elemList.Next() {
		elemValue, err := elemList.Value()
		if err != nil {
			return err
		}

		elementList = append(elementList, elemValue)
	}

	if value.Kind() == reflect.Slice {
		value.Set(reflect.MakeSlice(value.Type(), len(elementList), len(elementList)))
	}

	listSchemaDef := schemaDef.SubSchema("list")
	elemSchemaDef := listSchemaDef.SubSchema("element")

	for idx, elem := range elementList {
		if idx < value.Len() {
			if err := um.fillValue(value.Index(idx), elem, elemSchemaDef); err != nil {
				return err
			}
		}
	}

	return nil
}

func getIntValue(data interfaces.UnmarshalElement) (int64, error) {
	i32, err := data.Int32()
	if err == nil {
		return int64(i32), nil
	}

	i64, err := data.Int64()
	if err == nil {
		return i64, nil
	}
	return 0, err
}

func getFloatValue(data interfaces.UnmarshalElement) (float64, error) {
	f32, err := data.Float32()
	if err == nil {
		return float64(f32), nil
	}

	f64, err := data.Float64()
	if err == nil {
		return f64, nil
	}

	return 0, err
}

// Err returns an error in case Next returned false due to an error.
// If Next returned false due to EOF, Err returns nil.
func (r *Reader) Err() error {
	return r.err
}

// GetSchemaDefinition returns the schema definition of the parquet
// file.
func (r *Reader) GetSchemaDefinition() *parquetschema.SchemaDefinition {
	return r.r.GetSchemaDefinition()
}
