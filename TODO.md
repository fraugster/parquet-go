# Open TODOs

* add test for type store implementations to check whether the min and max values are correctly tracked
* improve design of dictionary encoding, since the best way is to handle the dictionary in the final stage, not in the encoding level
* verify whether blockSize: 128 and miniBlockCount in (\*byteArrayDeltaLengthEncoder).Close() is correct.
* in (\*byteArrayStore).setMinMax() whether the bytes.Compare calls are correct.
* rewrite booleanPlainEncoder implementation using packed array.
* in readPageData, evaluate whether it's possible to reuse data to reduce memory pressure.
* readPageData: having a dictEncoder/decoder is wrong. they should be a plain decoder for header and a int32 hybrid for values. the mix should happen here not in the dict itself
* writeChunk: check whether parquet.Encoding\_RLE is actually required.
* writeChunk: add key/value metadata support in column chunks.
* writeChunk: implement support for statistics.
* rethink decision logic in (\*ColumnStore).useDictionary(), the current one is very simple.
* improve (\*ColumnStore).reset() so that it works without losing schema information in the typed column store.
* check whether (\*FileWriter).FlushRowGroup() should still return an error if the number of records in the row group is 0.
* in (\*FileWriter).FlushRowGroup() add support for sorting columns.
* in (\*FileWriter).Close() add support for column orders.
* check whether it is feasible to implement a block cache in the packed array implementation
* dictPageWriter: add support for CRC.
* dictPageWriter: add support for sorted dictionary.
* dataPageWriterV1: add support for CRC.
* dataPageWriterV1: add statistics support.
* (\*dataPageWriterV1).write(): there is a redundant loop and copy if the value encoder is a dictEncoder.
* (\*dataPageReaderV2).read(): check whether it is correct to subtract the level size from the compressed size
* dataPageWriterV2: add support for CRC.
