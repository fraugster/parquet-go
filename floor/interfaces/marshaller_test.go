package interfaces

import (
	"fmt"
	"testing"

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

	expectedData := map[string]interface{}{
		"foo":  int64(23),
		"bar":  int32(42),
		"baz":  true,
		"name": []byte("John Doe"),
		"my_group": map[string]interface{}{
			"foo1": float32(23.5),
			"bar1": float64(9000.5),
		},
		"id_list": map[string]interface{}{
			"list": []map[string]interface{}{
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
		"data_map": map[string]interface{}{
			"key_value": []map[string]interface{}{
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
		"nested_data_map": map[string]interface{}{
			"key_value": []map[string]interface{}{
				{
					"key": int64(23),
					"value": map[string]interface{}{
						"foo": int32(42),
					},
				},
			},
		},
		"group_list": map[string]interface{}{
			"list": []map[string]interface{}{
				map[string]interface{}{
					"element": map[string]interface{}{
						"i": int64(0),
					},
				},
				map[string]interface{}{
					"element": map[string]interface{}{
						"i": int64(1),
					},
				},
				map[string]interface{}{
					"element": map[string]interface{}{
						"i": int64(2),
					},
				},
			},
		},
	}

	require.Equal(t, expectedData, obj.GetData())
}
