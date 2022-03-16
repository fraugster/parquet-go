package interfaces

import (
	"fmt"
	"testing"

	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stretchr/testify/require"
)

func TestObjectMarshalling(t *testing.T) {
	obj := NewMarshallObject(nil)

	obj.AddField("foo").SetInt64(23)
	obj.AddField("bar").SetInt32(42)
	obj.AddField("baz").SetBool(true)
	obj.AddField("name").SetByteArray([]byte("John Doe"))
	group := obj.AddField("my_group").Group()
	group.AddField("foo1").SetFloat32(23.5)
	group.AddField("bar1").SetFloat64(9000.5)

	idList := obj.AddField("id_list").List()
	idList.Add().SetInt64(int64(1))
	idList.Add().SetInt64(int64(2))
	idList.Add().SetInt64(int64(15))
	idList.Add().SetInt64(int64(28))
	idList.Add().SetInt64(int64(32))

	dataMap := obj.AddField("data_map").Map()
	for i := 0; i < 5; i++ {
		elem := dataMap.Add()
		elem.Key().SetByteArray([]byte(fmt.Sprintf("data%d", i)))
		elem.Value().SetInt32(int32(i))
	}

	nestedDataMap := obj.AddField("nested_data_map").Map()
	elem := nestedDataMap.Add()
	elem.Key().SetInt64(23)
	elem.Value().Group().AddField("foo").SetInt32(42)

	groupList := obj.AddField("group_list").List()
	for i := 0; i < 3; i++ {
		group := groupList.Add().Group()
		group.AddField("i").SetInt64(int64(i))
	}

	expectedData := map[string]any{
		"foo":  int64(23),
		"bar":  int32(42),
		"baz":  true,
		"name": []byte("John Doe"),
		"my_group": map[string]any{
			"foo1": float32(23.5),
			"bar1": float64(9000.5),
		},
		"id_list": map[string]any{
			"list": []map[string]any{
				{
					"element": int64(1),
				},
				{
					"element": int64(2),
				},
				{
					"element": int64(15),
				},
				{
					"element": int64(28),
				},
				{
					"element": int64(32),
				},
			},
		},
		"data_map": map[string]any{
			"key_value": []map[string]any{
				{
					"key":   []byte("data0"),
					"value": int32(0),
				},
				{
					"key":   []byte("data1"),
					"value": int32(1),
				},
				{
					"key":   []byte("data2"),
					"value": int32(2),
				},
				{
					"key":   []byte("data3"),
					"value": int32(3),
				},
				{
					"key":   []byte("data4"),
					"value": int32(4),
				},
			},
		},
		"nested_data_map": map[string]any{
			"key_value": []map[string]any{
				{
					"key": int64(23),
					"value": map[string]any{
						"foo": int32(42),
					},
				},
			},
		},
		"group_list": map[string]any{
			"list": []map[string]any{
				{
					"element": map[string]any{
						"i": int64(0),
					},
				},
				{
					"element": map[string]any{
						"i": int64(1),
					},
				},
				{
					"element": map[string]any{
						"i": int64(2),
					},
				},
			},
		},
	}

	require.Equal(t, expectedData, obj.GetData())
}

func TestObjectMarshallingWithSchema(t *testing.T) {
	sd, err := parquetschema.ParseSchemaDefinition(
		`message test {
			required group emails (LIST) {
				repeated group list {
					required binary element (STRING);
				}
			}
		}`)
	require.NoError(t, err)

	obj := NewMarshallObjectWithSchema(nil, sd)

	emailList := obj.AddField("emails").List()
	emailList.Add().SetByteArray([]byte("foo@example.com"))
	emailList.Add().SetByteArray([]byte("bar@example.com"))

	expectedData := map[string]any{
		"emails": map[string]any{
			"list": []map[string]any{
				{
					"element": []byte("foo@example.com"),
				},
				{
					"element": []byte("bar@example.com"),
				},
			},
		},
	}

	require.Equal(t, expectedData, obj.GetData())
}

func TestObjectMarshallingWithAthenaCompatibleSchema(t *testing.T) {
	sd, err := parquetschema.ParseSchemaDefinition(
		`message test {
			required group emails (LIST) {
				repeated group bag {
					required binary array_element (STRING);
				}
			}
		}`)
	require.NoError(t, err)

	obj := NewMarshallObjectWithSchema(nil, sd)

	emailList := obj.AddField("emails").List()
	emailList.Add().SetByteArray([]byte("foo@example.com"))
	emailList.Add().SetByteArray([]byte("bar@example.com"))

	expectedData := map[string]any{
		"emails": map[string]any{
			"bag": []map[string]any{
				{
					"array_element": []byte("foo@example.com"),
				},
				{
					"array_element": []byte("bar@example.com"),
				},
			},
		},
	}

	require.Equal(t, expectedData, obj.GetData())
}
