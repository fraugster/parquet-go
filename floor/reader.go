package floor

import (
	goparquet "github.com/fraugster/parquet-go"
)

// NewReader returns a new high-level parquet file reader.
func NewReader(r *goparquet.FileReader) *Reader {
	return &Reader{
		r: r,
	}
}

// Reader represents a high-level reader for parquet files.
type Reader struct {
	r *goparquet.FileReader

	initialized  bool
	currentGroup int
	currentRow   int64
	data         map[string]interface{}
	err          error
	eof          bool
}

// Next reads the next object so that it is ready to be scanned.
// Returns true if fetching the next object was successful, false
// otherwise, e.g. in case of an error or when EOF was reached.
func (r *Reader) Next() bool {
	if r.eof {
		return false
	}

	r.init()
	if r.err != nil {
		return false
	}

	r.readNext()
	if r.err != nil || r.eof {
		return false
	}

	return true
}

func (r *Reader) readNext() {
	if r.currentRow == r.r.NumRecords() {

		if r.currentGroup == r.r.RawGroupCount() {
			r.eof = true
			r.err = nil
			return
		}

		r.err = r.r.ReadRowGroup()
		if r.err != nil {
			return
		}
		r.currentGroup++
		r.currentRow = 0
	}

	r.data, r.err = r.r.GetData()
	if r.err != nil {
		return
	}
	r.currentRow++
}

func (r *Reader) init() {
	if r.initialized {
		return
	}
	r.initialized = true

	r.err = r.r.ReadRowGroup()
	if r.err != nil {
		return
	}
	r.currentGroup++
}

// Scan fills obj with the data from the record last fetched.
// Returns an error if there is no data available or if the
// structure of obj doesn't fit the data. obj needs to be
// a pointer to an object.
func (r *Reader) Scan(obj interface{}) error {
	// TODO: implement
	return nil
}

// Err returns an error in case Next returned false due to an error.
// If Next returned false due to EOF, Err returns nil.
func (r *Reader) Err() error {
	return r.err
}

// Close closes the reader including the underlying object.
func (r *Reader) Close() error {
	// TODO: implement
	return nil
}
