package go_parquet

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

// ColumnChunkReader allows to read data from a single column chunk of a parquet
// file.
type ColumnChunkReader struct {
	col Column

	reader io.ReadSeeker
	meta   *parquet.FileMetaData

	chunkMeta *parquet.ColumnMetaData

	page     *parquet.PageHeader
	dictPage *parquet.PageHeader

	readPageValues int
	pageNumValues  int

	notFirst bool

	valuesDecoder     valuesDecoder
	dictValuesDecoder dictValuesDecoder

	// Sometimes rLevel and dLevel data is constant and not in the page data
	rConst, dConst bool
}

func newColumnChunkReader(r io.ReadSeeker, meta *parquet.FileMetaData, col Column, chunk *parquet.ColumnChunk) (*ColumnChunkReader, error) {
	if chunk.FilePath != nil {
		return nil, fmt.Errorf("nyi: data is in another file: '%s'", *chunk.FilePath)
	}

	c := col.Index()
	// chunk.FileOffset is useless so ChunkMetaData is required here
	// as we cannot read it from r
	// see https://issues.apache.org/jira/browse/PARQUET-291
	if chunk.MetaData == nil {
		return nil, errors.Errorf("missing meta data for column %c", c)
	}

	if typ := *col.Element().Type; chunk.MetaData.Type != typ {
		return nil, errors.Errorf("wrong type in column chunk metadata, expected %s was %s",
			typ, chunk.MetaData.Type)
	}

	offset := chunk.MetaData.DataPageOffset
	if chunk.MetaData.DictionaryPageOffset != nil {
		offset = *chunk.MetaData.DictionaryPageOffset
	}
	_, err := r.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	cr := &ColumnChunkReader{
		col:       col,
		reader:    r,
		meta:      meta,
		chunkMeta: chunk.MetaData,
	}
	nested := strings.IndexByte(col.FlatName(), '.') >= 0
	repType := *col.Element().RepetitionType
	if !nested && repType == parquet.FieldRepetitionType_REQUIRED {
		// TODO: also check that len(Path) = maxD
		// For data that is required, the definition levels are not encoded and
		// always have the value of the max definition level.
		// TODO: document level ranges
		cr.dConst = true
	}
	if !nested && repType != parquet.FieldRepetitionType_REPEATED {
		// TODO: I think we need to check all schemaElements in the path (confirm if above)
		// TODO: clarify the following comment from parquet-format/README:
		// If the column is not nested the repetition levels are not encoded and
		// always have the value of 1
		cr.rConst = true
	}

	return cr, nil
}

func (cr *ColumnChunkReader) createDataReader(compressedSize int32, uncompressedSize int32) (ReaderCounter, error) {
	fmt.Println("Requested to create a reader with size ", compressedSize, " target size of ", uncompressedSize, "codec", cr.chunkMeta.Codec)
	if compressedSize < 0 || uncompressedSize < 0 {
		return nil, errors.New("invalid page data size")
	}

	return NewBlockReader(cr.reader, cr.chunkMeta.Codec, compressedSize, uncompressedSize)
}

func (cr *ColumnChunkReader) readDictionaryPage(compressedSize int32, uncompressedSize int32) error {
	reader, err := cr.createDataReader(compressedSize, uncompressedSize)
	if err != nil {
		return err
	}

	// We need to consume all reader here
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return errors.Wrap(err, "read dictionary page failed")
	}
	fmt.Println("Read the dict page len :", len(b))
	return nil
}

func (cr *ColumnChunkReader) readDataPageV1(ph *parquet.PageHeader, dph *parquet.DataPageHeader) error {
	reader, err := cr.createDataReader(ph.GetCompressedPageSize(), ph.GetUncompressedPageSize())
	if err != nil {
		return err
	}

	// We need to consume all reader here
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return errors.Wrap(err, "read data v1 page failed")
	}
	fmt.Println("Read the data v1 page len :", len(b))
	return nil
}

func (cr *ColumnChunkReader) readDataPageV2(ph *parquet.PageHeader, dph *parquet.DataPageHeaderV2) error {
	if dph.RepetitionLevelsByteLength < 0 {
		return errors.Errorf("invalid RepetitionLevelsByteLength")
	}
	if dph.DefinitionLevelsByteLength < 0 {
		return errors.Errorf("invalid DefinitionLevelsByteLength")
	}

	levelsSize := dph.RepetitionLevelsByteLength + dph.DefinitionLevelsByteLength
	levelsData := make([]byte, levelsSize)
	if _, err := io.ReadFull(cr.reader, levelsData); err != nil {
		return err
	}

	reader, err := cr.createDataReader(ph.GetCompressedPageSize()-levelsSize, ph.GetUncompressedPageSize()-levelsSize)
	if err != nil {
		return err
	}

	// We need to consume all reader here
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return errors.Wrap(err, "read data v1 page failed")
	}
	fmt.Println("Read the data v1 page len :", len(b))
	return nil
}

func (cr *ColumnChunkReader) readPage() error {
	ph := &parquet.PageHeader{}
	if err := readThrift(ph, cr.reader); err != nil {
		return err
	}

	if !cr.notFirst && ph.Type == parquet.PageType_DICTIONARY_PAGE {
		cr.notFirst = true
		cr.dictPage = ph

		dph := ph.DictionaryPageHeader
		if dph == nil {
			return errors.Errorf("null DictionaryPageHeader in %+v", ph)
		}
		if count := dph.NumValues; count < 0 {
			return errors.Errorf("negative NumValues in DICTIONARY_PAGE: %d", count)
		}

		if err := cr.readDictionaryPage(ph.CompressedPageSize, ph.UncompressedPageSize); err != nil {
			return err
		}

		// Read the next data page
		// if we have a DictionaryPageOffset we should return to DataPageOffset
		if cr.chunkMeta.DictionaryPageOffset != nil {
			if _, err := cr.reader.Seek(cr.chunkMeta.DataPageOffset, io.SeekStart); err != nil {
				return err
			}
		}
		ph = &parquet.PageHeader{}
		if err := readThrift(ph, cr.reader); err != nil {
			return err
		}
	}

	var (
		numValues      int
		valuesEncoding parquet.Encoding
		dph            *parquet.DataPageHeader
		dph2           *parquet.DataPageHeaderV2
	)

	switch ph.Type {
	case parquet.PageType_DATA_PAGE:
		dph = ph.DataPageHeader
		if dph == nil {
			return errors.Errorf("missing both DataPageHeader and DataPageHeaderV2 in %+v", ph)
		}
		numValues = int(dph.NumValues)
		valuesEncoding = dph.Encoding

	case parquet.PageType_DATA_PAGE_V2:
		dph2 = ph.DataPageHeaderV2
		if dph2 == nil {
			return errors.Errorf("missing both DataPageHeader and DataPageHeaderV2 in %+v", ph)
		}
		numValues = int(dph2.NumValues)
		valuesEncoding = dph2.Encoding
	default:
		return errors.Errorf("DATA_PAGE or DATA_PAGE_V2 type expected, but was %s", ph.Type)
	}

	if numValues < 0 {
		return errors.Errorf("negative page NumValues")
	}

	fmt.Printf("Read %d value with %s Encoding, From data page %s\n", numValues, valuesEncoding, ph.Type)

	if dph != nil {
		if err := cr.readDataPageV1(ph, dph); err != nil {
			return err
		}
	} else {
		if err := cr.readDataPageV2(ph, dph2); err != nil {
			return err
		}
	}

	return nil
}

func (cr *ColumnChunkReader) Read() error {
	return cr.readPage()
}
