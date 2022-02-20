package goparquet

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/fraugster/parquet-go/parquet"
)

func newIntStore() *ColumnStore {
	d := newStore(&int32Store{ColumnParameters: &ColumnParameters{}, stats: newInt32Stats(), pageStats: newInt32Stats()}, parquet.Encoding_PLAIN, false)
	return d
}

func TestOneColumn(t *testing.T) {
	row := schema{}
	require.NoError(t, row.AddColumn("DocID", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REQUIRED)))
	row.resetData()

	data := []map[string]interface{}{
		{"DocID": int32(10)},
		{"DocID": int32(20)},
	}

	for i := range data {
		require.NoError(t, row.AddData(data[i]))
	}
	d, err := row.findDataColumn("DocID")
	require.NoError(t, err)
	assert.Equal(t, uint16(0), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(0), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(10), int32(20)}, d.data.values.getValues())
	assert.Equal(t, []int32{0, 0}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 0}, d.data.rLevels.toArray())

	// Now reading data

	for i := range data {
		read, err := row.getData()
		require.NoError(t, err)
		assert.Equal(t, data[i], read)
	}
}

func TestOneColumnOptional(t *testing.T) {
	row := schema{}
	require.NoError(t, row.AddColumn("DocID", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_OPTIONAL)))
	row.resetData()

	data := []map[string]interface{}{
		{"DocID": int32(10)},
		{},
	}

	for i := range data {
		require.NoError(t, row.AddData(data[i]))
	}
	d, err := row.findDataColumn("DocID")
	require.NoError(t, err)
	assert.Equal(t, uint16(1), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(0), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(10)}, d.data.values.getValues())
	assert.Equal(t, []int32{1, 0}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 0}, d.data.rLevels.toArray())

	for i := range data {
		read, err := row.getData()
		require.NoError(t, err)
		assert.Equal(t, data[i], read)
	}
}

func TestOneColumnRepeated(t *testing.T) {
	row := schema{}
	require.NoError(t, row.AddColumn("DocID", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REPEATED)))
	row.resetData()

	data := []map[string]interface{}{
		{"DocID": []int32{10, 20}},
		{},
	}

	for i := range data {
		require.NoError(t, row.AddData(data[i]))
	}
	d, err := row.findDataColumn("DocID")
	require.NoError(t, err)
	assert.Equal(t, uint16(1), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(10), int32(20)}, d.data.values.getValues())
	assert.Equal(t, []int32{1, 1, 0}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 1, 0}, d.data.rLevels.toArray())

	for i := range data {
		read, err := row.getData()
		require.NoError(t, err)
		assert.Equal(t, data[i], read)
	}
}

func TestComplexPart1(t *testing.T) {
	row := &schema{}
	require.NoError(t, row.AddGroupByPath(ColumnPath{"Name"}, parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddGroupByPath(ColumnPath{"Name", "Language"}, parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddColumnByPath(ColumnPath{"Name", "Language", "Code"}, NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, row.AddColumnByPath(ColumnPath{"Name", "Language", "Country"}, NewDataColumn(newIntStore(), parquet.FieldRepetitionType_OPTIONAL)))
	require.NoError(t, row.AddColumnByPath(ColumnPath{"Name", "URL"}, NewDataColumn(newIntStore(), parquet.FieldRepetitionType_OPTIONAL)))

	row.resetData()

	data := []map[string]interface{}{
		{
			"Name": []map[string]interface{}{
				{
					"Language": []map[string]interface{}{
						{
							"Code":    int32(1),
							"Country": int32(100),
						},
						{
							"Code": int32(2),
						},
					},
					"URL": int32(10),
				},
				{
					"URL": int32(11),
				},
				{
					"Language": []map[string]interface{}{
						{
							"Code":    int32(3),
							"Country": int32(101),
						},
					},
				},
			},
		},
	}

	for i := range data {
		require.NoError(t, row.AddData(data[i]))
	}

	d, err := row.findDataColumn("Name.Language.Code")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(2), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(1), int32(2), int32(3)}, d.data.values.getValues())
	assert.Equal(t, []int32{2, 2, 1, 2}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 2, 1, 1}, d.data.rLevels.toArray())

	d, err = row.findDataColumn("Name.Language.Country")
	require.NoError(t, err)
	assert.Equal(t, uint16(3), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(2), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(100), int32(101)}, d.data.values.getValues())
	assert.Equal(t, []int32{3, 2, 1, 3}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 2, 1, 1}, d.data.rLevels.toArray())

	d, err = row.findDataColumn("Name.URL")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(10), int32(11)}, d.data.values.getValues())
	assert.Equal(t, []int32{2, 2, 1}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 1, 1}, d.data.rLevels.toArray())

	for i := range data {
		read, err := row.getData()
		require.NoError(t, err)
		assert.Equal(t, data[i], read)
	}
}

func TestComplexPart2(t *testing.T) {
	row := &schema{}
	require.NoError(t, row.AddGroupByPath(ColumnPath{"Links"}, parquet.FieldRepetitionType_OPTIONAL))
	require.NoError(t, row.AddColumnByPath(ColumnPath{"Links", "Backward"}, NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REPEATED)))
	require.NoError(t, row.AddColumnByPath(ColumnPath{"Links", "Forward"}, NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REPEATED)))
	row.resetData()

	data := []map[string]interface{}{
		{
			"Links": map[string]interface{}{
				"Forward": []int32{20, 40, 60},
			},
		},
		{
			"Links": map[string]interface{}{
				"Backward": []int32{10, 30},
				"Forward":  []int32{80},
			},
		},
	}

	for i := range data {
		require.NoError(t, row.AddData(data[i]))
	}

	d, err := row.findDataColumn("Links.Forward")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(20), int32(40), int32(60), int32(80)}, d.data.values.getValues())
	assert.Equal(t, []int32{2, 2, 2, 2}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 1, 1, 0}, d.data.rLevels.toArray())

	d, err = row.findDataColumn("Links.Backward")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(10), int32(30)}, d.data.values.getValues())
	assert.Equal(t, []int32{1, 2, 2}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 0, 1}, d.data.rLevels.toArray())

	for i := range data {
		read, err := row.getData()
		require.NoError(t, err)
		assert.Equal(t, data[i], read)
	}
}

func TestComplex(t *testing.T) {
	// Based on this picture https://i.stack.imgur.com/raOFu.png from this doc https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf
	row := &schema{}
	require.NoError(t, row.AddColumn("DocId", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, row.AddGroupByPath(ColumnPath{"Links"}, parquet.FieldRepetitionType_OPTIONAL))
	require.NoError(t, row.AddColumn("Links.Backward", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REPEATED)))
	require.NoError(t, row.AddColumn("Links.Forward", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REPEATED)))
	require.NoError(t, row.AddGroupByPath(ColumnPath{"Name"}, parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddGroupByPath(ColumnPath{"Name", "Language"}, parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddColumn("Name.Language.Code", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, row.AddColumn("Name.Language.Country", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_OPTIONAL)))
	require.NoError(t, row.AddColumn("Name.URL", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_OPTIONAL)))
	row.resetData()

	data := []map[string]interface{}{
		{
			"DocId": int32(10),
			"Links": map[string]interface{}{
				"Forward": []int32{20, 40, 60},
			},
			"Name": []map[string]interface{}{
				{
					"Language": []map[string]interface{}{
						{
							"Code":    int32(1),
							"Country": int32(100),
						},
						{
							"Code": int32(2),
						},
					},
					"URL": int32(10),
				},
				{
					"URL": int32(11),
				},
				{
					"Language": []map[string]interface{}{
						{
							"Code":    int32(3),
							"Country": int32(101),
						},
					},
				},
			},
		},
		{
			"DocId": int32(20),
			"Links": map[string]interface{}{
				"Backward": []int32{10, 30},
				"Forward":  []int32{80},
			},
			"Name": []map[string]interface{}{
				{
					"URL": int32(12),
				},
			},
		},
	}

	for i := range data {
		require.NoError(t, row.AddData(data[i]))
	}

	d, err := row.findDataColumn("DocId")
	require.NoError(t, err)
	assert.Equal(t, uint16(0), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(0), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(10), int32(20)}, d.data.values.getValues())
	assert.Equal(t, []int32{0, 0}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 0}, d.data.rLevels.toArray())

	d, err = row.findDataColumn("Name.URL")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(10), int32(11), int32(12)}, d.data.values.getValues())
	assert.Equal(t, []int32{2, 2, 1, 2}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 1, 1, 0}, d.data.rLevels.toArray())

	d, err = row.findDataColumn("Links.Forward")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(20), int32(40), int32(60), int32(80)}, d.data.values.getValues())
	assert.Equal(t, []int32{2, 2, 2, 2}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 1, 1, 0}, d.data.rLevels.toArray())

	d, err = row.findDataColumn("Links.Backward")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(10), int32(30)}, d.data.values.getValues())
	assert.Equal(t, []int32{1, 2, 2}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 0, 1}, d.data.rLevels.toArray())

	d, err = row.findDataColumn("Name.Language.Country")
	require.NoError(t, err)
	assert.Equal(t, uint16(3), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(2), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(100), int32(101)}, d.data.values.getValues())
	assert.Equal(t, []int32{3, 2, 1, 3, 1}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 2, 1, 1, 0}, d.data.rLevels.toArray())

	d, err = row.findDataColumn("Name.Language.Code")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(2), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(1), int32(2), int32(3)}, d.data.values.getValues())
	assert.Equal(t, []int32{2, 2, 1, 2, 1}, d.data.dLevels.toArray())
	assert.Equal(t, []int32{0, 2, 1, 1, 0}, d.data.rLevels.toArray())

	for i := range data {
		read, err := row.getData()
		require.NoError(t, err)
		assert.Equal(t, data[i], read)
	}
}

func TestTwitterBlog(t *testing.T) {
	// Sample from here https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
	row := &schema{}
	require.NoError(t, row.AddGroupByPath(ColumnPath{"level1"}, parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddColumn("level1.level2", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REPEATED)))
	row.resetData()

	data := []map[string]interface{}{
		{
			"level1": []map[string]interface{}{
				{"level2": []int32{1, 2, 3}},
				{"level2": []int32{4, 5, 6, 7}},
			},
		},
		{
			"level1": []map[string]interface{}{
				{"level2": []int32{8}},
				{"level2": []int32{9, 10}},
			},
		},
	}

	for i := range data {
		require.NoError(t, row.AddData(data[i]))
	}

	d, err := row.findDataColumn("level1.level2")
	require.NoError(t, err)
	var expected []interface{}
	for i := 1; i < 11; i++ {
		expected = append(expected, int32(i))
	}
	assert.Equal(t, expected, d.data.values.getValues())
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(2), d.MaxRepetitionLevel())
	assert.Equal(t, []int32{0, 2, 2, 1, 2, 2, 2, 0, 1, 2}, d.data.rLevels.toArray())
	assert.Equal(t, []int32{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}, d.data.dLevels.toArray())

	for i := range data {
		read, err := row.getData()
		require.NoError(t, err)
		assert.Equal(t, data[i], read)
	}
}

func TestEmptyParent(t *testing.T) {
	elementStore, err := NewInt32Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create elementStore")

	elementCol := NewDataColumn(elementStore, parquet.FieldRepetitionType_REQUIRED)
	list, err := NewListColumn(elementCol, parquet.FieldRepetitionType_OPTIONAL)
	require.NoError(t, err)

	row := &schema{}
	require.NoError(t, row.AddColumn("baz", list))
	row.resetData()
	data := []map[string]interface{}{
		{
			"baz": map[string]interface{}{},
		},
	}

	for i := range data {
		require.NoError(t, row.AddData(data[i]))
	}

	col, err := row.findDataColumn("baz.list.element")
	require.NoError(t, err)

	assert.Equal(t, []interface{}(nil), col.data.values.getValues())

	assert.Equal(t, uint16(2), col.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), col.MaxRepetitionLevel())
	require.Equal(t, []int32{0}, col.data.rLevels.toArray())
	require.Equal(t, []int32{1}, col.data.dLevels.toArray())

	for i := range data {
		read, err := row.getData()
		require.NoError(t, err)
		assert.Equal(t, data[i], read)
	}
}

func TestZeroRL(t *testing.T) {
	row := &schema{}
	//message test_msg {
	//		required group baz (LIST) {
	//			repeated group list {
	//				required group element {
	//					required int64 quux;
	//				}
	//			}
	//		}
	//	}
	require.NoError(t, row.AddGroupByPath(ColumnPath{"baz"}, parquet.FieldRepetitionType_REQUIRED))
	require.NoError(t, row.AddGroupByPath(ColumnPath{"baz", "list"}, parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddGroupByPath(ColumnPath{"baz", "list", "element"}, parquet.FieldRepetitionType_REQUIRED))
	require.NoError(t, row.AddColumn("baz.list.element.quux", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_REQUIRED)))
	row.resetData()

	data := map[string]interface{}{
		"baz": map[string]interface{}{
			"list": []map[string]interface{}{
				{
					"element": map[string]interface{}{
						"quux": int32(23),
					},
				},
				{
					"element": map[string]interface{}{
						"quux": int32(42),
					},
				},
			},
		},
	}

	require.NoError(t, row.AddData(data))

	d, err := row.findDataColumn("baz.list.element.quux")
	require.NoError(t, err)
	var expected = []interface{}{int32(23), int32(42)}
	assert.Equal(t, expected, d.data.values.getValues())
	assert.Equal(t, uint16(1), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []int32{0, 1}, d.data.rLevels.toArray())
	assert.Equal(t, []int32{1, 1}, d.data.dLevels.toArray())

	read, err := row.getData()
	require.NoError(t, err)
	assert.Equal(t, data, read)

	row = &schema{}
	require.NoError(t, row.AddGroupByPath(ColumnPath{"baz"}, parquet.FieldRepetitionType_REQUIRED))
	require.NoError(t, row.AddGroupByPath(ColumnPath{"baz", "list"}, parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddGroupByPath(ColumnPath{"baz", "list", "element"}, parquet.FieldRepetitionType_REQUIRED))
	require.NoError(t, row.AddColumn("baz.list.element.quux", NewDataColumn(newIntStore(), parquet.FieldRepetitionType_OPTIONAL)))
	row.resetData()
	require.NoError(t, row.AddData(data))

	d, err = row.findDataColumn("baz.list.element.quux")
	require.NoError(t, err)
	assert.Equal(t, expected, d.data.values.getValues())
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []int32{0, 1}, d.data.rLevels.toArray())
	assert.Equal(t, []int32{2, 2}, d.data.dLevels.toArray())

	read, err = row.getData()
	require.NoError(t, err)
	assert.Equal(t, data, read)
}
