package go_parquet

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

// TODO: Add MAP and LIST support

// TODO: the current design suggest every reader is only on one chunk and its not concurrent support. we can use multiple
// reader but its better to add concurrency support to the file reader itself
// TODO: add validation so every parent at least have one child.

const (
	_ int = iota
	listParent
	mapParent
)

type column struct {
	index          int
	name, flatName string
	// one of the following should be not null. data or children
	data     *ColumnStore
	children []*column

	rep parquet.FieldRepetitionType

	maxR, maxD uint16

	parent int // one of noParent, listParent, mapParent
	// for the reader we should read this element from the meta, for the writer we need to build this element
	element *parquet.SchemaElement
}

func (c *column) getSchemaArray() []*parquet.SchemaElement {
	ret := []*parquet.SchemaElement{c.Element()}
	if c.data != nil {
		return ret
	}

	for i := range c.children {
		ret = append(ret, c.children[i].getSchemaArray()...)
	}

	return ret
}

func (c *column) MaxDefinitionLevel() uint16 {
	return c.maxD
}

func (c *column) MaxRepetitionLevel() uint16 {
	return c.maxR
}

func (c *column) FlatName() string {
	return c.flatName
}

func (c *column) Name() string {
	return c.name
}

func (c *column) Index() int {
	return c.index
}

func (c *column) Element() *parquet.SchemaElement {
	if c.element == nil {
		// If this is a no-element node, we need to re-create element every time to make sure the content is always up-to-date
		// TODO: if its read only, we can build it
		return c.buildElement()
	}
	return c.element
}

func (c *column) getColumnStore() *ColumnStore {
	return c.data
}

func (c *column) buildElement() *parquet.SchemaElement {
	rep := c.rep
	elem := &parquet.SchemaElement{
		RepetitionType: &rep,
		Name:           c.name,
		FieldID:        nil, // Not sure about this field
	}

	if c.data != nil {
		t := c.data.parquetType()
		elem.Type = &t
		elem.TypeLength = c.data.typeLen()
		elem.ConvertedType = c.data.convertedType()
		elem.Scale = c.data.scale()
		elem.Precision = c.data.precision()
		elem.LogicalType = c.data.logicalType()
	} else {
		nc := int32(len(c.children))
		switch c.parent {
		case listParent:
			ct := parquet.ConvertedType_LIST
			elem.ConvertedType = &ct
			elem.LogicalType = &parquet.LogicalType{
				LIST: &parquet.ListType{},
			}
		case mapParent:
			ct := parquet.ConvertedType_MAP
			elem.ConvertedType = &ct
			elem.LogicalType = &parquet.LogicalType{
				MAP: &parquet.MapType{},
			}
		}

		elem.NumChildren = &nc
	}

	return elem
}

func (c *column) getDataSize() int64 {
	if _, ok := c.data.typedColumnStore.(*booleanStore); ok {
		// Booleans are stored in one bit, so the result is the number of items / 8
		return int64(c.data.values.numValues())/8 + 1
	}
	return c.data.values.size
}

func (c *column) getNextData() (map[string]interface{}, error) {
	if c.children == nil {
		return nil, errors.New("bug: call getNextData on non group node")
	}
	ret := make(map[string]interface{})
	notNil := 0
	for i := range c.children {
		data, err := c.children[i].getData()
		if err != nil {
			return nil, err
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
	}

	if notNil == 0 {
		return nil, nil
	}

	return ret, nil
}

func (c *column) getFirstDRLevel() (int32, int32, bool) {
	if c.data != nil {
		return c.data.getDRLevelAt(-1)
	}

	// there should be at lease 1 child, // TODO : add validation
	for i := range c.children {
		dl, rl, last := c.children[i].getFirstDRLevel()
		if last {
			return dl, rl, last
		}

		// if this value is not nil, dLevel less than this level is not interesting
		if dl >= int32(c.maxD) {
			return dl, rl, last
		}
	}

	return c.children[0].getFirstDRLevel()
}

func (c *column) getData() (interface{}, error) {
	if c.children != nil {
		data, err := c.getNextData()
		if err != nil {
			return nil, err
		}

		if c.rep != parquet.FieldRepetitionType_REPEATED || data == nil {
			return data, nil
		}

		ret := []map[string]interface{}{data}
		for {
			_, rl, last := c.getFirstDRLevel()
			if last || rl < int32(c.maxR) {
				// end of this object
				return ret, nil
			}

			data, err := c.getNextData()
			if err != nil {
				return nil, err
			}

			ret = append(ret, data)
		}
	}

	return c.data.get(int32(c.maxD), int32(c.maxR))
}

type schema struct {
	root       *column
	numRecords int64
	readOnly   int
}

// TODO(f0rud): a hacky way to make sure the root is not nil (because of my wrong assumption of the root element) at the last minute. fix it
func (r *schema) ensureRoot() {
	if r.root == nil {
		r.root = &column{
			index:    0,
			name:     "",
			flatName: "",
			data:     nil,
			children: []*column{},
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

func (r *schema) Columns() Columns {
	var ret []Column
	var fn func([]*column)

	fn = func(columns []*column) {
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

func (r *schema) GetColumnByName(path string) Column {
	var fn func([]*column) *column

	fn = func(columns []*column) *column {
		for i := range columns {
			if columns[i].data != nil {
				if columns[i].flatName == path {
					return columns[i]
				}
			} else {
				if c := fn(columns[i].children); c != nil {
					return c
				}
			}
		}

		return nil
	}
	r.ensureRoot()
	return fn(r.root.children)
}

// resetData is useful for resetting data after writing a chunk, to collect data for the next chunk
func (r *schema) resetData() {
	var fn func(c []*column)

	fn = func(c []*column) {
		for i := range c {
			if c[i].children != nil {
				fn(c[i].children)
			} else {
				c[i].data.reset(c[i].data.repetitionType())
			}
		}
	}
	r.ensureRoot()
	fn(r.root.children)
	r.numRecords = 0
}

func (r *schema) setNumRecords(n int64) {
	r.numRecords = n
}

func (r *schema) sortIndex() {
	var (
		idx int
		fn  func(c *[]*column)
	)

	fn = func(c *[]*column) {
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

// NewDataColumn create new column, not a group
func NewDataColumn(store *ColumnStore, rep parquet.FieldRepetitionType) Column {
	store.reset(rep)
	return &column{
		data:     store,
		children: nil,
		rep:      rep,
	}
}

// NewListColumn return a new LIST in parquet file
func NewListColumn(element Column, rep parquet.FieldRepetitionType) (Column, error) {
	// the higher level element doesn't need name, but all lower level does.
	c, ok := element.(*column)
	if !ok {
		return nil, errors.Errorf("type %T is not supported, use the NewDataColumn or NewListColumn to create the column", element)
	}

	c.name = "element"
	return &column{
		data:   nil,
		rep:    rep,
		parent: listParent,
		children: []*column{
			{
				name:     "list",
				data:     nil,
				rep:      parquet.FieldRepetitionType_REPEATED,
				children: []*column{c},
			},
		},
	}, nil
}

// NewListColumn return a new LIST in parquet file
func NewMapColumn(key, value Column, rep parquet.FieldRepetitionType) (Column, error) {
	// the higher level element doesn't need name, but all lower level does.
	k, ok := key.(*column)
	if !ok {
		return nil, errors.Errorf("type %T is not supported, use the NewDataColumn or NewListColumn to create the column", key)
	}

	v, ok := value.(*column)
	if !ok {
		return nil, errors.Errorf("type %T is not supported, use the NewDataColumn or NewListColumn to create the column", value)
	}

	if k.rep != parquet.FieldRepetitionType_REQUIRED {
		return nil, errors.New("the key repetition type should be REQUIRED")
	}

	k.name = "key"
	v.name = "value"
	return &column{
		data:   nil,
		rep:    rep,
		parent: mapParent,
		children: []*column{
			{
				name: "key_value",
				data: nil,
				rep:  parquet.FieldRepetitionType_REPEATED,
				children: []*column{
					k,
					v,
				},
			},
		},
	}, nil
}

// AddGroup add a group to the parquet schema, path is the dot separated path of the group,
// the parent group should be there or it will return an error
func (r *schema) AddGroup(path string, rep parquet.FieldRepetitionType) error {
	return r.addColumnOrGroup(path, &column{
		children: []*column{},
		data:     nil,
		rep:      rep,
	})
}

// AddColumn is for adding a column to the parquet schema, it resets the store
// path is the dot separated path of the group, the parent group should be there or it will return an error
func (r *schema) AddColumn(path string, col Column) error {
	c, ok := col.(*column)
	if !ok {
		return errors.Errorf("type %T is not supported, use the NewDataColumn or NewListColumn to create the column", col)
	}

	return r.addColumnOrGroup(path, c)
}

func recursiveFix(col *column, path string, maxR, maxD uint16) {
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
		return
	}

	for i := range col.children {
		recursiveFix(col.children[i], col.flatName, maxR, maxD)
	}
}

// do not call this function externally
func (r *schema) addColumnOrGroup(path string, col *column) error {
	if r.readOnly != 0 {
		return errors.New("the schema is read only")
	}

	r.ensureRoot()
	pa := strings.Split(path, ".")
	name := strings.Trim(pa[len(pa)-1], " \n\r\t")
	if name == "" {
		return errors.Errorf("the name of the column is required")
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
			return errors.New("can not add a new column to a list or map logical type")
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

func (r *schema) findDataColumn(path string) (*column, error) {
	pa := strings.Split(path, ".")
	r.ensureRoot()
	c := r.root.children
	var ret *column
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

func (r *schema) GetData() (map[string]interface{}, error) {
	// TODO: keep track of read row count
	d, err := r.root.getData()
	if err != nil {
		return nil, err
	}
	if d.(map[string]interface{}) == nil {
		d = make(map[string]interface{}) // just non nil root doc
	}

	return d.(map[string]interface{}), nil
}

func recursiveAddColumnNil(c []*column, defLvl, maxRepLvl uint16, repLvl uint16) error {
	for i := range c {
		if c[i].data != nil {
			_, err := c[i].data.add(nil, defLvl, maxRepLvl, repLvl)
			if err != nil {
				return err
			}
		}
		if c[i].children != nil {
			if err := recursiveAddColumnNil(c, defLvl, maxRepLvl, repLvl); err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO: maxRepLvl is available in the *column at definition time, we can remove it here
func recursiveAddColumnData(c []*column, m interface{}, defLvl uint16, maxRepLvl uint16, repLvl uint16) (bool, error) {
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

			// TODO: Transform data  into acceptable type if this is a LIST or MAP

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

func (c *column) readColumnSchema(schema []*parquet.SchemaElement, name string, idx int, dLevel, rLevel uint16) (int, error) {
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
	data.repTyp = *s.RepetitionType
	c.data = data
	c.flatName = name + "." + s.Name
	c.name = s.Name
	if name == "" {
		c.flatName = s.Name
	}
	return idx + 1, nil
}

func (c *column) readGroupSchema(schema []*parquet.SchemaElement, name string, idx int, dLevel, rLevel uint16) (int, error) {
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
	c.name = name
	// TODO : Do more validation here
	c.element = s
	c.children = make([]*column, 0, l)
	c.rep = *s.RepetitionType

	var err error
	idx++ // move idx from this group to next
	for i := 0; i < l; i++ {
		if schema[idx].Type == nil {
			// another group
			child := &column{}
			idx, err = child.readGroupSchema(schema, name, idx, dLevel, rLevel)
			if err != nil {
				return 0, err
			}
			c.children = append(c.children, child)
		} else {
			child := &column{}
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
			c := &column{}
			idx, err = c.readGroupSchema(schema, "", idx, 0, 0)
			if err != nil {
				return err
			}
			r.root.children = append(r.root.children, c)
		} else {
			c := &column{}
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

func printIndent(w io.Writer, indent int) {
	for i := 0; i < indent; i++ {
		fmt.Fprintf(w, " ")
	}
}

func printCols(w io.Writer, cols []*column, indent int) {
	for _, col := range cols {
		printIndent(w, indent)

		switch col.element.GetRepetitionType() {
		case parquet.FieldRepetitionType_REPEATED:
			fmt.Fprintf(w, "repeated")
		case parquet.FieldRepetitionType_OPTIONAL:
			fmt.Fprintf(w, "optional")
		case parquet.FieldRepetitionType_REQUIRED:
			fmt.Fprintf(w, "required")
		}
		fmt.Fprintf(w, " ")

		if col.element.Type == nil {
			fmt.Fprintf(w, "group %s", col.element.GetName())
			if col.element.ConvertedType != nil {
				fmt.Fprintf(w, " (%s)", getSchemaConvertedType(col.element.GetConvertedType()))
			}
			fmt.Fprintf(w, " {\n")
			printCols(w, col.children, indent+2)

			printIndent(w, indent)
			fmt.Fprintf(w, "}\n")
		} else {
			typ := getSchemaType(col.element.GetType())
			fmt.Fprintf(w, "%s %s", typ, col.element.GetName())
			if col.element.ConvertedType != nil {
				fmt.Fprintf(w, " (%s)", getSchemaConvertedType(col.element.GetConvertedType()))
			}
			if col.element.FieldID != nil {
				fmt.Fprintf(w, " = %d", col.element.GetFieldID())
			}
			fmt.Fprintf(w, ";\n")
		}
	}
}

func getSchemaType(t parquet.Type) string {
	switch t {

	case parquet.Type_BYTE_ARRAY:
		return "binary"
	case parquet.Type_FLOAT:
		return "float"
	case parquet.Type_DOUBLE:
		return "double"
	case parquet.Type_BOOLEAN:
		return "boolean"
	case parquet.Type_INT32:
		return "int32"
	case parquet.Type_INT64:
		return "int64"
	case parquet.Type_INT96:
		return "int96"
	}
	return fmt.Sprintf("UT:%s", t)
}

func getSchemaConvertedType(t parquet.ConvertedType) string {
	switch t {
	case parquet.ConvertedType_UTF8:
		return "STRING"
	case parquet.ConvertedType_LIST:
		return "LIST"
	case parquet.ConvertedType_MAP:
		return "MAP"
	case parquet.ConvertedType_MAP_KEY_VALUE:
		return "MAP_KEY_VALUE"
	}
	return fmt.Sprintf("UC:%s", t)
}

func (r *schema) String() string {
	if r.root == nil {
		return "<nil>"
	}

	buf := new(bytes.Buffer)

	fmt.Fprintf(buf, "message %s {\n", r.root.Name())

	printCols(buf, r.root.children, 2)

	fmt.Fprintf(buf, "}\n")

	return buf.String()
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

func (r *schema) NumRecords() int64 {
	return r.numRecords
}

type schemaCommon interface {
	// Columns return only data columns, not all columns
	Columns() Columns
	// Return a column by its name
	GetColumnByName(path string) Column

	// String returns the string representation of the schema
	String() string

	NumRecords() int64
	// Internal functions
	resetData()
	getSchemaArray() []*parquet.SchemaElement
}

// SchemaReader is a reader for the schema in file
type SchemaReader interface {
	schemaCommon
	setNumRecords(int64)
	GetData() (map[string]interface{}, error)
}

// SchemaWriter is a writer and generator for the schema
type SchemaWriter interface {
	schemaCommon

	AddData(m map[string]interface{}) error
	AddGroup(path string, rep parquet.FieldRepetitionType) error
	AddColumn(path string, col Column) error
	DataSize() int64
}

func makeSchema(meta *parquet.FileMetaData) (SchemaReader, error) {
	if len(meta.Schema) < 1 {
		return nil, errors.New("no schema element found")
	}
	s := &schema{
		root: &column{
			index:    0,
			name:     meta.Schema[0].Name,
			flatName: "",
			data:     nil,
			children: make([]*column, 0, len(meta.Schema)-1),
			rep:      0,
			maxR:     0,
			maxD:     0,
			element:  meta.Schema[0],
		},
	}
	err := s.readSchema(meta.Schema[1:])
	if err != nil {
		return nil, err
	}

	return s, nil
}
