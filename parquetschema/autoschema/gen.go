package autoschema

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

// GenerateSchema auto-generates a schema definition for a provided object's type
// using reflection. The generated schema is meant to be compatible with
// github.com/fraugster/parquet-go/floor's reflection-based marshalling/unmarshalling.
func GenerateSchema(obj interface{}) (*parquetschema.SchemaDefinition, error) {
	valueObj := reflect.ValueOf(obj)
	columns, err := generateSchema(valueObj.Type())
	if err != nil {
		return nil, fmt.Errorf("can't generate schema: %w", err)
	}

	return &parquetschema.SchemaDefinition{
		RootColumn: &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Name: "autogen_schema",
			},
			Children: columns,
		},
	}, nil
}

func generateSchema(objType reflect.Type) ([]*parquetschema.ColumnDefinition, error) {
	if objType.Kind() == reflect.Ptr {
		objType = objType.Elem()
	}

	if objType.Kind() != reflect.Struct {
		return nil, errors.New("can't generate schema: provided object needs to be of type struct or *struct")
	}

	columns := []*parquetschema.ColumnDefinition{}

	for i := 0; i < objType.NumField(); i++ {
		fieldType := objType.Field(i)
		fieldName := fieldNameToLower(fieldType)

		column, err := generateField(fieldType.Type, fieldName)
		if err != nil {
			return nil, err
		}

		columns = append(columns, column)
	}

	return columns, nil
}

func generateField(fieldType reflect.Type, fieldName string) (*parquetschema.ColumnDefinition, error) {
	switch fieldType.Kind() {
	case reflect.Bool:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_BOOLEAN),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			},
		}, nil
	case reflect.Int:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT64),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 64,
						IsSigned: true,
					},
				},
			},
		}, nil
	case reflect.Int8:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT32),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 8,
						IsSigned: true,
					},
				},
			},
		}, nil
	case reflect.Int16:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT32),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 16,
						IsSigned: true,
					},
				},
			},
		}, nil
	case reflect.Int32:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT32),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 32,
						IsSigned: true,
					},
				},
			},
		}, nil
	case reflect.Int64:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT64),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 64,
						IsSigned: true,
					},
				},
			},
		}, nil
	case reflect.Uint:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT32),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 32,
						IsSigned: false,
					},
				},
			},
		}, nil
	case reflect.Uint8:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT32),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 8,
						IsSigned: false,
					},
				},
			},
		}, nil
	case reflect.Uint16:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT32),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 16,
						IsSigned: false,
					},
				},
			},
		}, nil
	case reflect.Uint32:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT32),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 32,
						IsSigned: false,
					},
				},
			},
		}, nil
	case reflect.Uint64:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT64),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 64,
						IsSigned: false,
					},
				},
			},
		}, nil
	case reflect.Uintptr:
		return nil, errors.New("unsupported type uintptr")
	case reflect.Float32:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_FLOAT),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			},
		}, nil
	case reflect.Float64:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_DOUBLE),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			},
		}, nil
	case reflect.Complex64:
		return nil, errors.New("unsupported type complex64")
	case reflect.Complex128:
		return nil, errors.New("unsupported type complex128")
	case reflect.Chan:
		return nil, errors.New("unsupported type chan")
	case reflect.Func:
		return nil, errors.New("unsupported type func")
	case reflect.Interface:
		return nil, errors.New("unsupported type interface")
	case reflect.Map:
		keyType, err := generateField(fieldType.Key(), "key")
		if err != nil {
			return nil, err
		}
		valueType, err := generateField(fieldType.Elem(), "value")
		if err != nil {
			return nil, err
		}
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
				LogicalType: &parquet.LogicalType{
					MAP: &parquet.MapType{},
				},
			},
			Children: []*parquetschema.ColumnDefinition{
				{
					SchemaElement: &parquet.SchemaElement{
						Name:           "key_value",
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED),
						ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_MAP_KEY_VALUE),
					},
					Children: []*parquetschema.ColumnDefinition{
						keyType,
						valueType,
					},
				},
			},
		}, nil
	case reflect.Ptr:
		colDef, err := generateField(fieldType.Elem(), fieldName)
		if err != nil {
			return nil, err
		}
		colDef.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
		return colDef, nil
	case reflect.Slice, reflect.Array:
		if fieldType.Elem().Kind() == reflect.Uint8 {
			switch fieldType.Kind() {
			case reflect.Slice:
				// handle special case for []byte
				return &parquetschema.ColumnDefinition{
					SchemaElement: &parquet.SchemaElement{
						Type:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
						Name:           fieldName,
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
					},
				}, nil
			case reflect.Array:
				typeLen := int32(fieldType.Len())
				// handle special case for [N]byte
				return &parquetschema.ColumnDefinition{
					SchemaElement: &parquet.SchemaElement{
						Type:           parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
						Name:           fieldName,
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						TypeLength:     &typeLen,
					},
				}, nil
			}
		}
		elementType, err := generateField(fieldType.Elem(), "element")
		if err != nil {
			return nil, err
		}
		repType := elementType.SchemaElement.RepetitionType
		elementType.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Name:           fieldName,
				RepetitionType: repType,
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
				LogicalType: &parquet.LogicalType{
					LIST: &parquet.ListType{},
				},
			},
			Children: []*parquetschema.ColumnDefinition{
				{
					SchemaElement: &parquet.SchemaElement{
						Name:           "list",
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED),
					},
					Children: []*parquetschema.ColumnDefinition{
						elementType,
					},
				},
			},
		}, nil
	case reflect.String:
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				Name:           fieldName,
				RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
				LogicalType: &parquet.LogicalType{
					STRING: &parquet.StringType{},
				},
			},
		}, nil
	case reflect.Struct:
		switch {
		case fieldType.ConvertibleTo(reflect.TypeOf(time.Time{})):
			return &parquetschema.ColumnDefinition{
				SchemaElement: &parquet.SchemaElement{
					Type:           parquet.TypePtr(parquet.Type_INT64),
					Name:           fieldName,
					RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
					LogicalType: &parquet.LogicalType{
						TIMESTAMP: &parquet.TimestampType{
							IsAdjustedToUTC: true,
							Unit: &parquet.TimeUnit{
								NANOS: parquet.NewNanoSeconds(),
							},
						},
					},
				},
			}, nil
		default:
			children, err := generateSchema(fieldType)
			if err != nil {
				return nil, err
			}
			return &parquetschema.ColumnDefinition{
				SchemaElement: &parquet.SchemaElement{
					Name:           fieldName,
					RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
				},
				Children: children,
			}, nil
		}
	case reflect.UnsafePointer:
		return nil, errors.New("unsafe.Pointer is unsupported")
	default:
		return nil, fmt.Errorf("unknown kind %s is unsupported", fieldType.Kind())
	}
}

func fieldNameToLower(field reflect.StructField) string {
	parquetStructTag, ok := field.Tag.Lookup("parquet")
	if !ok {
		return strings.ToLower(field.Name)
	}

	parquetStructTagFields := strings.Split(parquetStructTag, ",")

	return strings.TrimSpace(parquetStructTagFields[0])
}
