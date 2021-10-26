package floor

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/pkg/errors"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
)

// NewWriter creates a new high-level writer for parquet.
func NewWriter(w *goparquet.FileWriter) *Writer {
	return &Writer{
		w: w,
	}
}

// NewFileWriter creates a nigh high-level writer for parquet
// that writes to a particular file.
func NewFileWriter(file string, opts ...goparquet.FileWriterOption) (*Writer, error) {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	w := goparquet.NewFileWriter(f, opts...)

	return &Writer{
		w: w,
		f: f,
	}, nil
}

// Writer represents a high-level writer for parquet files.
type Writer struct {
	w *goparquet.FileWriter
	f io.Closer
}

// Write adds a new object to be written to the parquet file. If
// obj implements the floor.Marshaller object, then obj.(Marshaller).Marshal
// will be called to determine the data, otherwise reflection will be
// used.
func (w *Writer) Write(obj interface{}) error {
	m, ok := obj.(interfaces.Marshaller)
	if !ok {
		m = &reflectMarshaller{obj: obj, schemaDef: w.w.GetSchemaDefinition()}
	}

	data := interfaces.NewMarshallObjectWithSchema(nil, w.w.GetSchemaDefinition())
	if err := m.MarshalParquet(data); err != nil {
		return err
	}

	if err := w.w.AddData(data.GetData()); err != nil {
		return err
	}

	return nil
}

type reflectMarshaller struct {
	obj       interface{}
	schemaDef *parquetschema.SchemaDefinition
}

func (m *reflectMarshaller) MarshalParquet(record interfaces.MarshalObject) error {
	return m.marshal(record, reflect.ValueOf(m.obj), m.schemaDef)
}

func (m *reflectMarshaller) marshal(record interfaces.MarshalObject, value reflect.Value, schemaDef *parquetschema.SchemaDefinition) error {
	if value.Type().Kind() == reflect.Ptr {
		if value.IsNil() {
			return errors.New("object is nil")
		}
		value = value.Elem()
	}

	typ := value.Type()

	if typ.Kind() == reflect.Struct {
		return m.decodeStruct(record, value, schemaDef)
	}

	if typ.Kind() != reflect.Map {
		return fmt.Errorf("object needs to be a struct, *struct or map, it's a %v instead", typ)
	}

	iter := value.MapRange()
	for iter.Next() {
		fieldName := iter.Key().String()
		subSchemaDef := schemaDef.SubSchema(fieldName)
		field := record.AddField(fieldName)

		err := m.decodeValue(field, iter.Value(), subSchemaDef)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *reflectMarshaller) decodeStruct(record interfaces.MarshalObject, value reflect.Value, schemaDef *parquetschema.SchemaDefinition) error {
	if value.Type().Kind() == reflect.Ptr {
		if value.IsNil() {
			return errors.New("object is nil")
		}
		value = value.Elem()
	}

	typ := value.Type()

	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("object needs to be a struct or a *struct, it's a %v instead", typ)
	}

	numFields := typ.NumField()
	for i := 0; i < numFields; i++ {
		fieldValue := value.Field(i)

		fieldName := fieldNameFunc(typ.Field(i))

		subSchemaDef := schemaDef.SubSchema(fieldName)

		field := record.AddField(fieldName)

		err := m.decodeValue(field, fieldValue, subSchemaDef)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *reflectMarshaller) decodeTimeValue(elem *parquet.SchemaElement, field interfaces.MarshalElement, value reflect.Value) error {
	switch {
	case elem.GetLogicalType().TIME.Unit.IsSetNANOS():
		field.SetInt64(value.Interface().(Time).Nanoseconds())
	case elem.GetLogicalType().TIME.Unit.IsSetMICROS():
		field.SetInt64(value.Interface().(Time).Microseconds())
	case elem.GetLogicalType().TIME.Unit.IsSetMILLIS():
		field.SetInt32(value.Interface().(Time).Milliseconds())
	default:
		return errors.New("invalid TIME unit")
	}
	return nil
}

func (m *reflectMarshaller) decodeTimestampValue(elem *parquet.SchemaElement, field interfaces.MarshalElement, value reflect.Value) error {
	var factor int64
	switch {
	case elem.GetLogicalType().TIMESTAMP.Unit.IsSetNANOS():
		factor = 1
	case elem.GetLogicalType().TIMESTAMP.Unit.IsSetMICROS():
		factor = 1000
	case elem.GetLogicalType().TIMESTAMP.Unit.IsSetMILLIS():
		factor = 1000000
	default:
		return errors.New("invalid TIMESTAMP unit")
	}
	ts := value.Interface().(time.Time).UnixNano()
	ts /= factor
	field.SetInt64(ts)
	return nil
}

func (m *reflectMarshaller) decodeValue(field interfaces.MarshalElement, value reflect.Value, schemaDef *parquetschema.SchemaDefinition) error {
	elem := schemaDef.SchemaElement()
	if elem == nil {
		return nil
	}

	if value.Kind() == reflect.Ptr || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}

	if value.Type().ConvertibleTo(reflect.TypeOf(Time{})) {
		if elem.LogicalType != nil && elem.GetLogicalType().IsSetTIME() {
			return m.decodeTimeValue(elem, field, value)
		}
	}

	if value.Type().ConvertibleTo(reflect.TypeOf(time.Time{})) {
		if elem.LogicalType != nil {
			switch {
			case elem.GetLogicalType().IsSetDATE():
				days := int32(value.Interface().(time.Time).Sub(time.Unix(0, 0).UTC()).Hours() / 24)
				field.SetInt32(days)
				return nil
			case elem.GetLogicalType().IsSetTIMESTAMP():
				return m.decodeTimestampValue(elem, field, value)
			}
		} else if elem.GetType() == parquet.Type_INT96 {
			field.SetInt96(goparquet.TimeToInt96(value.Interface().(time.Time)))
			return nil
		}
	}

	if !elem.IsSetType() && !elem.IsSetConvertedType() && elem.GetNumChildren() > 0 && value.Kind() == reflect.Map {
		group := field.Group()
		iter := value.MapRange()
		for iter.Next() {
			fieldName := iter.Key().String()
			err := m.decodeValue(group.AddField(fieldName), iter.Value(), schemaDef.SubSchema(fieldName))
			if err != nil {
				return err
			}
		}

		return nil
	}

	switch elem.GetType() {
	case parquet.Type_INT64:
		switch value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field.SetInt64(value.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			field.SetInt64(int64(value.Uint()))
		default:
			return errors.Errorf("unable to decode %s:%s to int64", elem.Name, value.Kind())
		}
		return nil
	case parquet.Type_INT32:
		switch value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field.SetInt32(int32(value.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			field.SetInt32(int32(value.Uint()))
		default:
			return errors.Errorf("unable to decode %s:%s to int32", elem.Name, value.Kind())
		}
		return nil
	}

	switch value.Kind() {
	case reflect.Bool:
		field.SetBool(value.Bool())
		return nil
	case reflect.Float32:
		field.SetFloat32(float32(value.Float()))
		return nil
	case reflect.Float64:
		field.SetFloat64(value.Float())
		return nil
	case reflect.Array, reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return m.decodeByteSliceOrArray(field, value, schemaDef)
		}
		return m.decodeSliceOrArray(field, value, schemaDef)
	case reflect.Map:
		return m.decodeMap(field, value, schemaDef)
	case reflect.String:
		field.SetByteArray([]byte(value.String()))
		return nil
	case reflect.Struct:
		return m.decodeStruct(field.Group(), value, schemaDef)
	default:
		return fmt.Errorf("unsupported type %s", value.Type())
	}
}

func (m *reflectMarshaller) decodeByteSliceOrArray(field interfaces.MarshalElement, value reflect.Value, schemaDef *parquetschema.SchemaDefinition) error {
	elem := schemaDef.SchemaElement()
	if elem == nil {
		return nil
	}

	if value.Kind() == reflect.Slice && value.IsNil() {
		return nil
	}

	if elem.LogicalType != nil && elem.GetLogicalType().IsSetUUID() {
		if value.Len() != 16 {
			return fmt.Errorf("field is annotated as UUID but length is %d", value.Len())
		}
	}

	switch value.Kind() {
	case reflect.Slice:
		if value.IsNil() {
			return nil
		}
		field.SetByteArray(value.Bytes())
	case reflect.Array:
		if elem.GetType() == parquet.Type_INT96 {
			if value.Len() != 12 {
				return fmt.Errorf("field is of type INT96 but length is %d", value.Len())
			}
			data := reflect.New(value.Type()).Elem()
			_ = reflect.Copy(data, value)

			field.SetInt96(data.Interface().([12]byte))
			return nil
		}
		data := reflect.MakeSlice(reflect.TypeOf([]byte{}), value.Len(), value.Len())

		_ = reflect.Copy(data, value)

		field.SetByteArray(data.Bytes())
	}
	return nil
}

func (m *reflectMarshaller) decodeSliceOrArray(field interfaces.MarshalElement, value reflect.Value, schemaDef *parquetschema.SchemaDefinition) error {
	elem := schemaDef.SchemaElement()
	if elem == nil {
		return nil
	}

	if value.Kind() == reflect.Slice && value.IsNil() {
		return nil
	}

	if elem.GetConvertedType() != parquet.ConvertedType_LIST {
		return fmt.Errorf("decoding slice or array but schema element %s is not annotated as LIST", elem.GetName())
	}

	elementSchemaDef := schemaDef.SubSchema("list").SubSchema("element")
	if elementSchemaDef == nil {
		elementSchemaDef = schemaDef.SubSchema("bag").SubSchema("array_element")
		if elementSchemaDef == nil {
			return fmt.Errorf("element %s is annotated as LIST but group structure seems invalid", schemaDef.SchemaElement().GetName())
		}
	}

	list := field.List()

	for i := 0; i < value.Len(); i++ {
		if err := m.decodeValue(list.Add(), value.Index(i), elementSchemaDef); err != nil {
			return err
		}
	}

	return nil
}

func (m *reflectMarshaller) decodeMap(field interfaces.MarshalElement, value reflect.Value, schemaDef *parquetschema.SchemaDefinition) error {
	if value.IsNil() {
		return nil
	}

	if elem := schemaDef.SchemaElement(); elem.GetConvertedType() != parquet.ConvertedType_MAP {
		return fmt.Errorf("decoding map but schema element %s is not annotated as MAP", elem.GetName())
	}

	keyValueSchemaDef := schemaDef.SubSchema("key_value")
	keySchemaDef := keyValueSchemaDef.SubSchema("key")
	valueSchemaDef := keyValueSchemaDef.SubSchema("value")

	mapData := field.Map()

	iter := value.MapRange()

	for iter.Next() {
		kvPair := mapData.Add()

		if err := m.decodeValue(kvPair.Key(), iter.Key(), keySchemaDef); err != nil {
			return err
		}

		if err := m.decodeValue(kvPair.Value(), iter.Value(), valueSchemaDef); err != nil {
			return err
		}
	}

	return nil
}

// Close flushes outstanding data and closes the underlying
// parquet writer.
func (w *Writer) Close() error {
	if w.f != nil {
		defer w.f.Close()
	}

	return w.w.Close()
}
