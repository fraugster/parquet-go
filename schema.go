package goparquet

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

// TODO: the current design suggest every reader is only on one chunk and its not concurrent support. we can use multiple
// reader but its better to add concurrency support to the file reader itself
// TODO: add validation so every parent at least have one child.

const (
	_ int = iota
	listParent
	mapParent
)

type Column struct {
	index          int
	name, flatName string

	nameArray []string
	// one of the following should be not null. data or children
	data     *ColumnStore
	children []*Column

	rep parquet.FieldRepetitionType

	maxR, maxD uint16

	parent int // one of noParent, listParent, mapParent
	// for the reader we should read this element from the meta, for the writer we need to build this element
	element *parquet.SchemaElement

	params *ColumnParameters
}

func (c *Column) pathArray() []string {
	if c.nameArray == nil {
		c.nameArray = strings.Split(c.flatName, ".")
	}

	return c.nameArray
}

func (c *Column) getSchemaArray() []*parquet.SchemaElement {
	ret := []*parquet.SchemaElement{c.Element()}
	if c.data != nil {
		return ret
	}

	for i := range c.children {
		ret = append(ret, c.children[i].getSchemaArray()...)
	}

	return ret
}

// MaxDefinitionLevel returns the maximum definition level for this column
func (c *Column) MaxDefinitionLevel() uint16 {
	return c.maxD
}

// MaxRepetitionLevel returns the maximum repetition value for this column
func (c *Column) MaxRepetitionLevel() uint16 {
	return c.maxR
}

// FlatName returns the name of the column and its parents separated with dot
func (c *Column) FlatName() string {
	return c.flatName
}

// Name return the column name
func (c *Column) Name() string {
	return c.name
}

// Index return the index of the column in schema, zero based
func (c *Column) Index() int {
	return c.index
}

// Element returns the thrift parquet element
func (c *Column) Element() *parquet.SchemaElement {
	if c.element == nil {
		// If this is a no-element node, we need to re-create element every time to make sure the content is always up-to-date
		// TODO: if its read only, we can build it
		return c.buildElement()
	}
	return c.element
}

// Type returns the parquet type of the value, if this is a group, then it returns nil
// also it returns the basic parquet types, not the logical and converted type
func (c *Column) Type() *parquet.Type {
	if c.data == nil {
		return nil
	}

	return parquet.TypePtr(c.data.parquetType())
}

func (c *Column) getColumnStore() *ColumnStore {
	return c.data
}

func (c *Column) buildElement() *parquet.SchemaElement {
	rep := c.rep
	elem := &parquet.SchemaElement{
		RepetitionType: &rep,
		Name:           c.name,
	}

	if c.params != nil {
		elem.FieldID = c.params.FieldID
		elem.ConvertedType = c.params.ConvertedType
		elem.LogicalType = c.params.LogicalType
	}

	if c.data != nil {
		elem.Type = parquet.TypePtr(c.data.parquetType())
		elem.TypeLength = c.params.TypeLength
		elem.Scale = c.params.Scale
		elem.Precision = c.params.Precision
	} else {
		nc := int32(len(c.children))
		elem.NumChildren = &nc
	}

	return elem
}

func (c *Column) getDataSize() int64 {
	if _, ok := c.data.typedColumnStore.(*booleanStore); ok {
		// Booleans are stored in one bit, so the result is the number of items / 8
		return int64(c.data.values.numValues())/8 + 1
	}
	return c.data.values.size
}

func (c *Column) getNextData() (map[string]interface{}, int32, error) {
	if c.children == nil {
		return nil, 0, errors.New("bug: call getNextData on non group node")
	}
	ret := make(map[string]interface{})
	notNil := 0
	var maxD int32
	for i := range c.children {
		data, dl, err := c.children[i].getData()
		if err != nil {
			return nil, 0, err
		}
		if dl > maxD {
			maxD = dl
		}

		// https://golang.org/doc/faq#nil_error
		if m, ok := data.(map[string]interface{}); ok && m == nil {
			data = nil
		}

		// if the data is not nil, then its ok, but if its nil, we need to know in which definition level is this nil is.
		// if its exactly one below max definition level, then the parent is there
		if data != nil {
			ret[c.children[i].name] = data
			notNil++
		}
		var diff int32
		if c.children[i].rep != parquet.FieldRepetitionType_REQUIRED {
			diff++
		}
		if dl == int32(c.children[i].maxD)-diff {
			notNil++
		}
	}

	if notNil == 0 {
		return nil, maxD, nil
	}

	return ret, int32(c.maxD), nil
}

func (c *Column) getFirstRDLevel() (int32, int32, bool) {
	if c.data != nil {
		return c.data.getRDLevelAt(-1)
	}

	// there should be at lease 1 child, // TODO : add validation
	for i := range c.children {
		rl, dl, last := c.children[i].getFirstRDLevel()
		if last {
			return rl, dl, last
		}

		// if this value is not nil, dLevel less than this level is not interesting
		if dl == int32(c.children[i].maxD) {
			return rl, dl, last
		}
	}

	return -1, -1, false
}

func (c *Column) getData() (interface{}, int32, error) {
	if c.children != nil {
		data, maxD, err := c.getNextData()
		if err != nil {
			return nil, 0, err
		}

		if c.rep != parquet.FieldRepetitionType_REPEATED || data == nil {
			return data, maxD, nil
		}

		ret := []map[string]interface{}{data}
		for {
			rl, _, last := c.getFirstRDLevel()
			if last || rl < int32(c.maxR) || rl == 0 {
				// end of this object
				return ret, maxD, nil
			}

			data, _, err := c.getNextData()
			if err != nil {
				return nil, maxD, err
			}

			ret = append(ret, data)
		}
	}

	return c.data.get(int32(c.maxD), int32(c.maxR))
}

type schema struct {
	root       *Column
	numRecords int64
	readOnly   int
}

// TODO(f0rud): a hacky way to make sure the root is not nil (because of my wrong assumption of the root element) at the last minute. fix it
func (r *schema) ensureRoot() {
	if r.root == nil {
		r.root = &Column{
			index:    0,
			name:     "msg", // TODO: provide way of overriding this.
			flatName: "",    // the flat name for root element is empty
			data:     nil,
			children: []*Column{},
			rep:      0,
			maxR:     0,
			maxD:     0,
			element:  nil,
		}
	}
}

func (r *schema) getSchemaArray() []*parquet.SchemaElement {
	r.ensureRoot()
	elem := r.root.getSchemaArray()
	// the root doesn't have repetition type
	elem[0].RepetitionType = nil
	return elem
}

func (r *schema) Columns() []*Column {
	var ret []*Column
	var fn func([]*Column)

	fn = func(columns []*Column) {
		for i := range columns {
			if columns[i].data != nil {
				ret = append(ret, columns[i])
			} else {
				fn(columns[i].children)
			}
		}
	}
	r.ensureRoot()
	fn(r.root.children)
	return ret
}

func (r *schema) GetColumnByName(path string) *Column {
	data := r.Columns()
	for i := range data {
		if data[i].flatName == path {
			return data[i]
		}
	}

	return nil
}

// resetData is useful for resetting data after writing a chunk, to collect data for the next chunk
func (r *schema) resetData() {
	data := r.Columns()
	for i := range data {
		data[i].data.reset(data[i].rep, data[i].maxR, data[i].maxD)
	}

	r.numRecords = 0
}

func (r *schema) setNumRecords(n int64) {
	r.numRecords = n
}

func (r *schema) sortIndex() {
	var (
		idx int
		fn  func(c *[]*Column)
	)

	fn = func(c *[]*Column) {
		if c == nil {
			return
		}
		for data := range *c {
			if (*c)[data].data != nil {
				(*c)[data].index = idx
				idx++
			} else {
				fn(&(*c)[data].children)
			}
		}
	}
	r.ensureRoot()
	fn(&r.root.children)
}

func (r *schema) SetSchemaDefinition(sd *SchemaDefinition) {
	r.root = sd.col

	for _, c := range r.root.children {
		recursiveFix(c, "", 0, 0)
	}
}

// ColumnParameters contains common parameters related to a column.
type ColumnParameters struct {
	LogicalType   *parquet.LogicalType
	ConvertedType *parquet.ConvertedType
	TypeLength    *int32
	FieldID       *int32
	Scale         *int32
	Precision     *int32
}

// NewDataColumn create new column, not a group
func NewDataColumn(store *ColumnStore, rep parquet.FieldRepetitionType) *Column {
	return &Column{
		data:     store,
		children: nil,
		rep:      rep,
		params:   store.typedColumnStore.params(),
	}
}

// NewListColumn return a new LIST in parquet file
func NewListColumn(element *Column, rep parquet.FieldRepetitionType) (*Column, error) {
	// the higher level element doesn't need name, but all lower level does.
	element.name = "element"
	return &Column{
		data:   nil,
		rep:    rep,
		parent: listParent,
		children: []*Column{
			{
				name:     "list",
				data:     nil,
				rep:      parquet.FieldRepetitionType_REPEATED,
				children: []*Column{element},
			},
		},
		params: &ColumnParameters{
			LogicalType: &parquet.LogicalType{
				LIST: parquet.NewListType(),
			},
			ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
		},
	}, nil
}

// NewMapColumn return a new MAP in parquet file
func NewMapColumn(key, value *Column, rep parquet.FieldRepetitionType) (*Column, error) {
	// the higher level element doesn't need name, but all lower level does.
	if key.rep != parquet.FieldRepetitionType_REQUIRED {
		return nil, errors.New("the key repetition type should be REQUIRED")
	}

	key.name = "key"
	value.name = "value"
	return &Column{
		data:   nil,
		rep:    rep,
		parent: mapParent,
		children: []*Column{
			{
				name: "key_value",
				data: nil,
				rep:  parquet.FieldRepetitionType_REPEATED,
				children: []*Column{
					key,
					value,
				},
				params: &ColumnParameters{
					ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_MAP_KEY_VALUE),
				},
			},
		},
		params: &ColumnParameters{
			LogicalType: &parquet.LogicalType{
				MAP: parquet.NewMapType(),
			},
			ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
		},
	}, nil
}

// AddGroup add a group to the parquet schema, path is the dot separated path of the group,
// the parent group should be there or it will return an error
func (r *schema) AddGroup(path string, rep parquet.FieldRepetitionType) error {
	return r.addColumnOrGroup(path, &Column{
		children: []*Column{},
		data:     nil,
		rep:      rep,
		params:   &ColumnParameters{},
	})
}

// AddColumn is for adding a column to the parquet schema, it resets the store
// path is the dot separated path of the group, the parent group should be there or it will return an error
func (r *schema) AddColumn(path string, col *Column) error {
	return r.addColumnOrGroup(path, col)
}

func recursiveFix(col *Column, path string, maxR, maxD uint16) {
	if col.rep != parquet.FieldRepetitionType_REQUIRED {
		maxD++
	}
	if col.rep == parquet.FieldRepetitionType_REPEATED {
		maxR++
	}

	col.maxR = maxR
	col.maxD = maxD
	col.flatName = path + "." + col.name
	if path == "" {
		col.flatName = col.name
	}
	if col.data != nil {
		col.data.reset(col.rep, col.maxR, col.maxD)
		return
	}

	for i := range col.children {
		recursiveFix(col.children[i], col.flatName, maxR, maxD)
	}
}

// do not call this function externally
func (r *schema) addColumnOrGroup(path string, col *Column) error {
	if r.readOnly != 0 {
		return errors.New("the schema is read only")
	}

	r.ensureRoot()
	pa := strings.Split(path, ".")
	name := strings.Trim(pa[len(pa)-1], " \n\r\t")
	if name == "" {
		return errors.Errorf("the name of the Column is required")
	}

	col.name = name
	c := r.root
	for i := 0; i < len(pa)-1; i++ {
		found := false
		if c.children == nil {
			break
		}
		for j := range c.children {
			if c.children[j].name == pa[i] {
				found = true
				c = c.children[j]
				break
			}
		}

		if !found {
			return errors.Errorf("path %s failed on %q", path, pa[i])
		}

		if c.parent != 0 {
			return errors.New("can not add a new Column to a list or map logical type")
		}

		if c.children == nil && i < len(pa)-1 {
			return errors.Errorf("path %s is not parent at %q", path, pa[i])
		}
	}

	if c.children == nil {
		return errors.New("the children are nil")
	}

	recursiveFix(col, c.flatName, c.maxR, c.maxD)

	c.children = append(c.children, col)
	r.sortIndex()

	return nil
}

func (r *schema) findDataColumn(path string) (*Column, error) {
	pa := strings.Split(path, ".")
	r.ensureRoot()
	c := r.root.children
	var ret *Column
	for i := 0; i < len(pa); i++ {
		found := false
		for j := range c {
			if c[j].name == pa[i] {
				found = true
				ret = c[j]
				c = c[j].children
				break
			}
		}
		if !found {
			return nil, errors.Errorf("path %s failed on %q", path, pa[i])
		}
		if c == nil && i < len(pa)-1 {
			return nil, errors.Errorf("path %s is not parent at %q", path, pa[i])
		}
	}

	if ret == nil || ret.data == nil {
		return nil, errors.Errorf("path %s doesnt end on data", path)
	}

	return ret, nil
}

func (r *schema) AddData(m map[string]interface{}) error {
	r.readOnly = 1
	r.ensureRoot()
	_, err := recursiveAddColumnData(r.root.children, m, 0, 0, 0)
	if err == nil {
		r.numRecords++
	}
	return err
}

func (r *schema) getData() (map[string]interface{}, error) {
	// TODO: keep track of read row count
	d, _, err := r.root.getData()
	if err != nil {
		return nil, err
	}
	if d.(map[string]interface{}) == nil {
		d = make(map[string]interface{}) // just non nil root doc
	}

	return d.(map[string]interface{}), nil
}

func recursiveAddColumnNil(c []*Column, defLvl, maxRepLvl uint16, repLvl uint16) error {
	for i := range c {
		if c[i].data != nil {
			if c[i].rep == parquet.FieldRepetitionType_REQUIRED && defLvl == c[i].maxD {
				return errors.Errorf("the value %q is required", c[i].flatName)
			}
			_, err := c[i].data.add(nil, defLvl, maxRepLvl, repLvl)
			if err != nil {
				return err
			}
		}
		if c[i].children != nil {
			if err := recursiveAddColumnNil(c[i].children, defLvl, maxRepLvl, repLvl); err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO: maxRepLvl is available in the *Column at definition time, we can remove it here
func recursiveAddColumnData(c []*Column, m interface{}, defLvl uint16, maxRepLvl uint16, repLvl uint16) (bool, error) {
	var data = m.(map[string]interface{})
	var advance bool
	for i := range c {
		d := data[c[i].name]
		if c[i].data != nil {
			inc, err := c[i].data.add(d, defLvl, maxRepLvl, repLvl)
			if err != nil {
				return false, err
			}

			if inc {
				advance = true //
			}
		}
		if c[i].children != nil {
			l := defLvl
			// In case of required value, there is no need to add a definition value, since it should be there always,
			// also for nil value, it means we should skip from this level to the lowest level
			if c[i].rep != parquet.FieldRepetitionType_REQUIRED && d != nil {
				l++
			}

			switch v := d.(type) {
			case nil:
				if err := recursiveAddColumnNil(c[i].children, l, maxRepLvl, repLvl); err != nil {
					return false, err
				}
			case map[string]interface{}: // Not repeated
				if c[i].rep == parquet.FieldRepetitionType_REPEATED {
					return false, errors.Errorf("repeated group should be array")
				}
				_, err := recursiveAddColumnData(c[i].children, v, l, maxRepLvl, repLvl)
				if err != nil {
					return false, err
				}
			case []map[string]interface{}:
				m := maxRepLvl
				if c[i].rep == parquet.FieldRepetitionType_REPEATED {
					m++
				}
				if c[i].rep != parquet.FieldRepetitionType_REPEATED {
					return false, errors.Errorf("no repeated group should not be array")
				}
				rL := repLvl
				if len(v) == 0 {
					return false, recursiveAddColumnNil(c[i].children, l, m, rL)
				}
				for vi := range v {
					inc, err := recursiveAddColumnData(c[i].children, v[vi], l, m, rL)
					if err != nil {
						return false, err
					}

					if inc {
						advance = true
						rL = m
					}
				}

			default:
				return false, errors.Errorf("data is not a map or array of map, its a %T", v)
			}
		}
	}

	return advance, nil
}

func (c *Column) readColumnSchema(schema []*parquet.SchemaElement, name string, idx int, dLevel, rLevel uint16) (int, error) {
	s := schema[idx]

	// TODO: validate Name is not empty
	if s.RepetitionType == nil {
		return 0, errors.Errorf("field RepetitionType is nil in index %d", idx)
	}

	if *s.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
		dLevel++
	}

	if *s.RepetitionType == parquet.FieldRepetitionType_REPEATED {
		rLevel++
	}

	c.element = s
	c.maxR = rLevel
	c.maxD = dLevel
	data, err := getValuesStore(s)
	if err != nil {
		return 0, err
	}
	c.rep = *s.RepetitionType
	c.data = data
	c.flatName = name + "." + s.Name
	c.name = s.Name
	if name == "" {
		c.flatName = s.Name
	}
	return idx + 1, nil
}

func (c *Column) readGroupSchema(schema []*parquet.SchemaElement, name string, idx int, dLevel, rLevel uint16) (int, error) {
	if len(schema) <= idx {
		return 0, errors.New("schema index out of bound")
	}

	s := schema[idx]
	if s.Type != nil {
		return 0, errors.Errorf("field Type is not nil in index %d", idx)
	}
	if s.NumChildren == nil {
		return 0, errors.Errorf("the field NumChildren is invalid in index %d", idx)
	}

	if *s.NumChildren <= 0 {
		return 0, errors.Errorf("the field NumChildren is zero in index %d", idx)
	}
	l := int(*s.NumChildren)

	if len(schema) <= idx+l {
		return 0, errors.Errorf("not enough element in the schema list in index %d", idx)
	}

	if s.RepetitionType != nil && *s.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
		dLevel++
	}

	if s.RepetitionType != nil && *s.RepetitionType == parquet.FieldRepetitionType_REPEATED {
		rLevel++
	}

	if name == "" {
		name = s.Name
	} else {
		name += "." + s.Name
	}
	c.flatName = name
	c.name = s.Name
	// TODO : Do more validation here
	c.element = s
	c.children = make([]*Column, 0, l)
	c.rep = *s.RepetitionType

	var err error
	idx++ // move idx from this group to next
	for i := 0; i < l; i++ {
		if schema[idx].Type == nil {
			// another group
			child := &Column{}
			idx, err = child.readGroupSchema(schema, name, idx, dLevel, rLevel)
			if err != nil {
				return 0, err
			}
			c.children = append(c.children, child)
		} else {
			child := &Column{}
			idx, err = child.readColumnSchema(schema, name, idx, dLevel, rLevel)
			if err != nil {
				return 0, err
			}
			c.children = append(c.children, child)
		}
	}

	return idx, nil
}

func (r *schema) readSchema(schema []*parquet.SchemaElement) error {
	r.readOnly = 1
	var err error
	for idx := 0; idx < len(schema); {
		if schema[idx].Type == nil {
			c := &Column{}
			idx, err = c.readGroupSchema(schema, "", idx, 0, 0)
			if err != nil {
				return err
			}
			r.root.children = append(r.root.children, c)
		} else {
			c := &Column{}
			idx, err = c.readColumnSchema(schema, "", idx, 0, 0)
			if err != nil {
				return err
			}
			r.root.children = append(r.root.children, c)
		}
	}
	r.sortIndex()
	return nil
}

func (r *schema) GetSchemaDefinition() *SchemaDefinition {
	return &SchemaDefinition{
		col: r.root,
	}
}

// DataSize return the size of data stored in the schema right now
func (r *schema) DataSize() int64 {
	cols := r.Columns()
	var size int64
	for i := range cols {
		size += cols[i].getDataSize()
	}

	return size
}

func (r *schema) rowGroupNumRecords() int64 {
	return r.numRecords
}

type schemaCommon interface {
	// Columns return only data columns, not all columns
	Columns() []*Column
	// Return a column by its name
	GetColumnByName(path string) *Column

	// GetSchemaDefinition returns the schema definition.
	GetSchemaDefinition() *SchemaDefinition
	SetSchemaDefinition(*SchemaDefinition)

	// Internal functions
	rowGroupNumRecords() int64
	resetData()
	getSchemaArray() []*parquet.SchemaElement
}

// schemaReader is a reader for the schema in file
type schemaReader interface {
	schemaCommon
	setNumRecords(int64)
	getData() (map[string]interface{}, error)
}

// SchemaWriter is a writer and generator for the schema
type SchemaWriter interface {
	schemaCommon

	AddData(m map[string]interface{}) error
	AddGroup(path string, rep parquet.FieldRepetitionType) error
	AddColumn(path string, col *Column) error
	DataSize() int64
}

func makeSchema(meta *parquet.FileMetaData) (schemaReader, error) {
	if len(meta.Schema) < 1 {
		return nil, errors.New("no schema element found")
	}
	s := &schema{
		root: &Column{
			index:    0,
			name:     meta.Schema[0].Name,
			flatName: "",
			data:     nil,
			children: make([]*Column, 0, len(meta.Schema)-1),
			rep:      0,
			maxR:     0,
			maxD:     0,
			element:  meta.Schema[0],
			params: &ColumnParameters{
				LogicalType:   meta.Schema[0].LogicalType,
				ConvertedType: meta.Schema[0].ConvertedType,
				TypeLength:    meta.Schema[0].TypeLength,
				FieldID:       meta.Schema[0].FieldID,
			},
		},
	}
	err := s.readSchema(meta.Schema[1:])
	if err != nil {
		return nil, err
	}

	return s, nil
}
