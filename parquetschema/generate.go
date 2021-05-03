package parquetschema

import (
	"reflect"
	"strings"
	"time"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

func Generate(i interface{}) (*SchemaDefinition, error) {
	c, err := columnForType(i, "root")
	if err != nil {
		return nil, err
	}

	return SchemaDefinitionFromColumnDefinition(c), nil
}

func MustGenerate(i interface{}) *SchemaDefinition {
	s, err := Generate(i)
	if err != nil {
		panic(err)
	}
	return s
}

func columnForType(i interface{}, name string) (c *ColumnDefinition, err error) {
	v := reflect.ValueOf(i)
	t := reflect.TypeOf(i)
	k := v.Kind()

	if k == reflect.Ptr {
		return columnForIndirectType(i, name)
	}

	if name == "" {
		name = "element"
	}

	c = &ColumnDefinition{
		SchemaElement: &parquet.SchemaElement{
			//Type:           nil,
			//TypeLength:     nil,
			RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			Name:           strings.ToLower(name),
			//NumChildren:    nil,
			//ConvertedType:  nil,
			//Scale:          nil,
			//Precision:      nil,
			FieldID: nil,
			//LogicalType:    nil,
		},
	}

	//todo: c.InName, c.ExName = getNames(goName, tags)

	switch k {
	case reflect.Bool:
		c.SchemaElement.Type = parquet.TypePtr(parquet.Type_BOOLEAN)
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return c, annotateInt(i, c)
	case reflect.Float32:
		c.SchemaElement.Type = parquet.TypePtr(parquet.Type_FLOAT)
	case reflect.Float64:
		c.SchemaElement.Type = parquet.TypePtr(parquet.Type_DOUBLE)
	case reflect.String:
		c.SchemaElement.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
		c.SchemaElement.LogicalType = parquet.NewLogicalType()
		c.SchemaElement.LogicalType.STRING = parquet.NewStringType()
	case reflect.Slice:
		// byte slice case
		if t.Elem().Kind() == reflect.Uint8 {
			c.SchemaElement.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
			return
		}

		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_LIST)
		c.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)

		repeatedGroup := &ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Name:           "list",
				Type:           nil, // group
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED),
			},
		}

		child, err := columnForType(reflect.New(t.Elem()).Elem().Interface(), "")
		if err != nil {
			return nil, err
		}
		repeatedGroup.Children = append(repeatedGroup.Children, child)

		c.Children = append(c.Children, repeatedGroup)
	case reflect.Map:
		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_MAP)
		c.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)

		repeatedGroup := &ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Name:           "key_value",
				Type:           nil, // group
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_MAP_KEY_VALUE),
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED),
			},
		}

		keyChild, err := columnForType(reflect.New(t.Key()).Elem().Interface(), "key")
		if err != nil {
			return nil, err
		}
		valueChild, err := columnForType(reflect.New(t.Elem()).Elem().Interface(), "value")
		if err != nil {
			return nil, err
		}
		repeatedGroup.Children = append(repeatedGroup.Children, keyChild, valueChild)

		c.Children = append(c.Children, repeatedGroup)
	case reflect.Struct:
		switch i.(type) {
		case time.Time:
			c.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT64)
			c.SchemaElement.LogicalType = parquet.NewLogicalType()
			c.SchemaElement.LogicalType.TIMESTAMP = parquet.NewTimestampType()
			c.SchemaElement.LogicalType.TIMESTAMP.IsAdjustedToUTC = true
			c.SchemaElement.LogicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
			c.SchemaElement.LogicalType.TIMESTAMP.Unit.NANOS = parquet.NewNanoSeconds()
		default:
			// no type, this is how parquet-go does it, may be unconventional
			for j := 0; j < v.NumField(); j++ {
				fv := v.Field(j)
				ft := t.Field(j)
				//ftags := ParseTags(string(ft.Tag))

				child, err := columnForType(fv.Interface(), ft.Name)
				if err != nil {
					return nil, err
				}

				c.Children = append(c.Children, child)
			}
		}
	case reflect.Array:
		// byte array case
		if t.Elem().Kind() == reflect.Uint8 {
			c.SchemaElement.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
			arraySize := int32(t.Len())
			c.SchemaElement.TypeLength = &arraySize
			return
		}

		fallthrough
	default:
		return nil, errors.Errorf("cannot yet generate schema for kind %s", v.Kind())
	}

	return
}

func columnForIndirectType(i interface{}, name string) (c *ColumnDefinition, err error) {
	v := reflect.ValueOf(i)
	k := v.Kind()

	if !(k == reflect.Ptr || k == reflect.Interface) {
		return nil, errors.Errorf("can only parse interface and pointer kinds, not %s", v.Kind())
	}

	var isPointer bool
	for k == reflect.Ptr || k == reflect.Interface {
		isPointer = isPointer || k == reflect.Ptr
		v = v.Elem()
		k = v.Kind()
	}

	c, err = columnForType(v.Interface(), name)
	if err != nil {
		return nil, err
	}

	if isPointer {
		c.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	}

	return
}

func annotateInt(i interface{}, c *ColumnDefinition) (err error) {
	if c == nil || c.SchemaElement == nil {
		return errors.Errorf("cannot annotate nil schema element")
	}

	v := reflect.ValueOf(i)
	k := v.Kind()

	logicalType := parquet.NewIntType()

	// logical type
	switch k {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		break
	default:
		return errors.Errorf("%s is not an int type", k)
	}

	// signed
	switch k {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		logicalType.IsSigned = true
	}

	// bit width
	switch k {
	case reflect.Int8, reflect.Uint8:
		logicalType.BitWidth = 8
	case reflect.Int16, reflect.Uint16:
		logicalType.BitWidth = 16
	case reflect.Int32, reflect.Uint32:
		logicalType.BitWidth = 32
	case reflect.Int64, reflect.Uint64, reflect.Int, reflect.Uint:
		logicalType.BitWidth = 64
	}

	c.SchemaElement.LogicalType = parquet.NewLogicalType()
	c.SchemaElement.LogicalType.INTEGER = logicalType

	// primitive type
	switch k {
	case reflect.Int64, reflect.Uint64, reflect.Int, reflect.Uint:
		c.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT64)
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		c.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
	}

	// converted type: this is recommended to be included for backwards compatibility
	switch k {
	case reflect.Int8:
		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8)
	case reflect.Int16:
		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16)
	case reflect.Int32:
		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)
	case reflect.Int64:
		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
	case reflect.Uint8:
		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8)
	case reflect.Uint16:
		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16)
	case reflect.Uint32:
		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32)
	case reflect.Uint64:
		c.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64)
	}

	return nil
}
