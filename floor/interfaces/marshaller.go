package interfaces

import "github.com/fraugster/parquet-go/parquetschema"

// Marshaller is the interface necessary for objects to be
// marshalled when passed to the (*Writer).WriteRecord method.
type Marshaller interface {
	MarshalParquet(obj MarshalObject) error
}

// MarshalObject is the interface a Marshaller needs to marshall its data
// to.
type MarshalObject interface {
	AddField(field string) MarshalElement

	GetData() map[string]any
}

// MarshalElement describes the interface to set the value of an element in a Marshaller
// implementation.
type MarshalElement interface {
	Group() MarshalObject
	SetInt32(i int32)
	SetInt64(i int64)
	SetInt96(i [12]byte)
	SetFloat32(f float32)
	SetFloat64(f float64)
	SetBool(b bool)
	SetByteArray(data []byte)
	List() MarshalList
	Map() MarshalMap
}

// MarshalList describes the interface to add a list of values in a Marshaller
// implementation.
type MarshalList interface {
	Add() MarshalElement
}

// MarshalMap describes the interface to add a map of keys and values in a Marshaller
// implementation.
type MarshalMap interface {
	Add() MarshalMapElement
}

// MarshalMapElement describes the interfaces to set an individual pair of keys and
// values in a Marshaller implementation.
type MarshalMapElement interface {
	Key() MarshalElement
	Value() MarshalElement
}

type object struct {
	data   map[string]any
	schema *parquetschema.SchemaDefinition
}

func (o *object) GetData() map[string]any {
	return o.data
}

func (o *object) AddField(field string) MarshalElement {
	return &element{data: o.data, f: field, schema: o.schema.SubSchema(field)}
}

type element struct {
	data   map[string]any
	f      string
	schema *parquetschema.SchemaDefinition
}

func (e *element) SetInt32(i int32) {
	e.data[e.f] = i
}

func (e *element) SetInt64(i int64) {
	e.data[e.f] = i
}

func (e *element) SetInt96(i [12]byte) {
	e.data[e.f] = i
}

func (e *element) SetFloat32(f float32) {
	e.data[e.f] = f
}

func (e *element) SetFloat64(f float64) {
	e.data[e.f] = f
}

func (e *element) SetBool(b bool) {
	e.data[e.f] = b
}

func (e *element) SetByteArray(data []byte) {
	e.data[e.f] = data
}

func (e *element) List() MarshalList {
	listName := "list"
	elemName := "element"
	bagSchema := e.schema.SubSchema("bag")
	if bagSchema != nil {
		listName = "bag"
		elemName = "array_element"
	}
	return &list{parentData: e.data, parentField: e.f, listName: listName, elemName: elemName, schema: e.schema.SubSchema(listName).SubSchema(elemName)}
}

func (e *element) Map() MarshalMap {
	data := map[string]any{"key_value": []map[string]any{}}
	e.data[e.f] = data
	return &marshMap{data: data, schema: e.schema}
}

func (e *element) Group() MarshalObject {
	obj := map[string]any{}
	e.data[e.f] = obj
	return &object{data: obj}
}

type list struct {
	parentData  map[string]any
	parentField string
	data        map[string]any
	schema      *parquetschema.SchemaDefinition
	listName    string
	elemName    string
}

func (l *list) Add() MarshalElement {
	if l.data == nil {
		l.data = map[string]any{l.listName: []map[string]any{}}
		// we need to delay adding map to parent data field until Add() is called first time, otherwise
		// this code will fail on an empty m.Foobar:
		//
		// 	list := obj.AddField("foobar").List()
		// 	for _, elem := range m.Foobar {
		// 		list.Add().SetByteArray([]byte(elem))
		// 	}
		l.parentData[l.parentField] = l.data
	}
	listData := l.data[l.listName].([]map[string]any)
	elemData := map[string]any{}
	l.data[l.listName] = append(listData, elemData)
	e := &element{data: elemData, f: l.elemName, schema: l.schema}
	return e
}

type marshMap struct {
	data   map[string]any
	schema *parquetschema.SchemaDefinition
}

func (l *marshMap) Add() MarshalMapElement {
	kvData := l.data["key_value"].([]map[string]any)
	elemData := map[string]any{}
	l.data["key_value"] = append(kvData, elemData)
	me := &mapElement{data: elemData, schema: l.schema.SubSchema("key_value")}
	return me
}

type mapElement struct {
	data   map[string]any
	schema *parquetschema.SchemaDefinition
}

func (m *mapElement) Key() MarshalElement {
	return &element{data: m.data, f: "key", schema: m.schema.SubSchema("key")}
}

func (m *mapElement) Value() MarshalElement {
	return &element{data: m.data, f: "value", schema: m.schema.SubSchema("value")}
}

// NewMarshallObject creates a new marshaller object
func NewMarshallObject(data map[string]any) MarshalObject {
	if data == nil {
		data = make(map[string]any)
	}
	return &object{
		data: data,
	}
}

// NewMarshallObjectWithSchema creates a new marshaller object with a particular schema.
func NewMarshallObjectWithSchema(data map[string]any, schemaDef *parquetschema.SchemaDefinition) MarshalObject {
	if data == nil {
		data = make(map[string]any)
	}
	return &object{
		data:   data,
		schema: schemaDef,
	}
}

// NewMarshalElement creates new marshall element object
func NewMarshalElement(data map[string]any, name string) MarshalElement {
	if data == nil {
		data = make(map[string]any)
	}
	return &element{
		data: data,
		f:    name,
	}
}
