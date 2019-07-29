package go_parquet

import "github.com/fraugster/parquet-go/parquet"

type dataPageWriterV1 struct {
	ph *parquet.PageHeader

	col *column
}

func (dp *dataPageWriterV1) init(schema *Schema, path string) error {
	col, err := schema.findDataColumn(path)
	if err != nil {
		return err
	}

	dp.col = col
	return nil
}

func (dp *dataPageWriterV1) getHeader() (*parquet.PageHeader, error) {
	if dp.ph == nil {
		dp.ph = &parquet.PageHeader{
			Type: parquet.PageType_DATA_PAGE_V2,
			DataPageHeader: &parquet.DataPageHeader{
				NumValues: dp.col.data.dictionary().numValues(),
				Encoding:  dp.col.data.encoding(),
				// Only RLE supported for now, not sure if we need support for more encoding
				DefinitionLevelEncoding: parquet.Encoding_RLE,
				RepetitionLevelEncoding: parquet.Encoding_RLE,
				Statistics:              nil,
			},
		}
	}
	return dp.ph, nil
}
