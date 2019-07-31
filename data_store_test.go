package go_parquet

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/fraugster/parquet-go/parquet"
)

func newIntStore() ColumnStore {
	d := newStore(&int32Store{}, parquet.Encoding_PLAIN, false)
	return d
}

func TestOneColumn(t *testing.T) {
	row := Schema{}
	require.NoError(t, row.AddColumn("DocID", newIntStore(), parquet.FieldRepetitionType_REQUIRED))

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
	assert.Equal(t, []interface{}{int32(10), int32(20)}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{0, 0}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 0}, d.data.repetitionLevels())
}

func TestOneColumnOptional(t *testing.T) {
	row := Schema{}
	require.NoError(t, row.AddColumn("DocID", newIntStore(), parquet.FieldRepetitionType_OPTIONAL))

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
	assert.Equal(t, []interface{}{int32(10), nil}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{1, 0}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 0}, d.data.repetitionLevels())
}

func TestComplexPart1(t *testing.T) {
	row := &Schema{}
	require.NoError(t, row.AddGroup("Name", parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddGroup("Name.Language", parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddColumn("Name.Language.Code", newIntStore(), parquet.FieldRepetitionType_REQUIRED))
	require.NoError(t, row.AddColumn("Name.Language.Country", newIntStore(), parquet.FieldRepetitionType_OPTIONAL))
	require.NoError(t, row.AddColumn("Name.URL", newIntStore(), parquet.FieldRepetitionType_OPTIONAL))

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
	assert.Equal(t, []interface{}{int32(1), int32(2), nil, int32(3)}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{2, 2, 1, 2}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 2, 1, 1}, d.data.repetitionLevels())

	d, err = row.findDataColumn("Name.Language.Country")
	require.NoError(t, err)
	assert.Equal(t, uint16(3), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(2), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(100), nil, nil, int32(101)}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{3, 2, 1, 3}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 2, 1, 1}, d.data.repetitionLevels())

	d, err = row.findDataColumn("Name.URL")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(10), int32(11), nil}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{2, 2, 1}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 1, 1}, d.data.repetitionLevels())

}

func TestComplexPart2(t *testing.T) {
	row := &Schema{}
	require.NoError(t, row.AddGroup("Links", parquet.FieldRepetitionType_OPTIONAL))
	require.NoError(t, row.AddColumn("Links.Backward", newIntStore(), parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddColumn("Links.Forward", newIntStore(), parquet.FieldRepetitionType_REPEATED))

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
	assert.Equal(t, []interface{}{int32(20), int32(40), int32(60), int32(80)}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{2, 2, 2, 2}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 1, 1, 0}, d.data.repetitionLevels())

	d, err = row.findDataColumn("Links.Backward")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{nil, int32(10), int32(30)}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{1, 2, 2}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 0, 1}, d.data.repetitionLevels())
}

func TestComplex(t *testing.T) {
	// Based on this picture https://i.stack.imgur.com/raOFu.png from this doc https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf
	row := &Schema{}
	require.NoError(t, row.AddColumn("DocId", newIntStore(), parquet.FieldRepetitionType_REQUIRED))
	require.NoError(t, row.AddGroup("Links", parquet.FieldRepetitionType_OPTIONAL))
	require.NoError(t, row.AddColumn("Links.Backward", newIntStore(), parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddColumn("Links.Forward", newIntStore(), parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddGroup("Name", parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddGroup("Name.Language", parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddColumn("Name.Language.Code", newIntStore(), parquet.FieldRepetitionType_REQUIRED))
	require.NoError(t, row.AddColumn("Name.Language.Country", newIntStore(), parquet.FieldRepetitionType_OPTIONAL))
	require.NoError(t, row.AddColumn("Name.URL", newIntStore(), parquet.FieldRepetitionType_OPTIONAL))

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
	assert.Equal(t, []interface{}{int32(10), int32(20)}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{0, 0}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 0}, d.data.repetitionLevels())

	d, err = row.findDataColumn("Name.URL")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(10), int32(11), nil, int32(12)}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{2, 2, 1, 2}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 1, 1, 0}, d.data.repetitionLevels())

	d, err = row.findDataColumn("Links.Forward")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(20), int32(40), int32(60), int32(80)}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{2, 2, 2, 2}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 1, 1, 0}, d.data.repetitionLevels())

	d, err = row.findDataColumn("Links.Backward")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(1), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{nil, int32(10), int32(30)}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{1, 2, 2}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 0, 1}, d.data.repetitionLevels())

	d, err = row.findDataColumn("Name.Language.Country")
	require.NoError(t, err)
	assert.Equal(t, uint16(3), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(2), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(100), nil, nil, int32(101), nil}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{3, 2, 1, 3, 1}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 2, 1, 1, 0}, d.data.repetitionLevels())

	d, err = row.findDataColumn("Name.Language.Code")
	require.NoError(t, err)
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(2), d.MaxRepetitionLevel())
	assert.Equal(t, []interface{}{int32(1), int32(2), nil, int32(3), nil}, d.data.dictionary().assemble(true))
	assert.Equal(t, []int32{2, 2, 1, 2, 1}, d.data.definitionLevels())
	assert.Equal(t, []int32{0, 2, 1, 1, 0}, d.data.repetitionLevels())

}

func TestTwitterBlog(t *testing.T) {
	// Sample from here https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
	row := &Schema{}
	require.NoError(t, row.AddGroup("level1", parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, row.AddColumn("level1.level2", newIntStore(), parquet.FieldRepetitionType_REPEATED))

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
	assert.Equal(t, expected, d.data.dictionary().assemble(true))
	assert.Equal(t, uint16(2), d.MaxDefinitionLevel())
	assert.Equal(t, uint16(2), d.MaxRepetitionLevel())
	assert.Equal(t, []int32{0, 2, 2, 1, 2, 2, 2, 0, 1, 2}, d.data.repetitionLevels())
	assert.Equal(t, []int32{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}, d.data.definitionLevels())

}
