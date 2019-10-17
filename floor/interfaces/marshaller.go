package interfaces

// Marshaller is the interface necessary for objects to be
// marshalled when passed to the (*Writer).WriteRecord method.
type Marshaller interface {
	MarshalParquet(obj MarshalObject) error
}

// MarshalObject is the interface a Marshaller needs to marshall its data
// to.
type MarshalObject interface {
	AddField(field string) MarshalElement

	GetData() map[string]interface{}
}

// MarshalElement describes the interface to set the value of an element in a Marshaller
// implementation.
type MarshalElement interface {
	Group() MarshalObject
	SetInt32(i int32)
	SetInt64(i int64)
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
	data map[string]interface{}
}

func (o *object) GetData() map[string]interface{} {
	return o.data
}

func (o *object) AddField(field string) MarshalElement {
	return &element{data: o.data, f: field}
}

type element struct {
	data map[string]interface{}
	f    string
}

func (e *element) SetInt32(i int32) {
	e.data[e.f] = i
}

func (e *element) SetInt64(i int64) {
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
	data := map[string]interface{}{"list": []map[string]interface{}{}}
	e.data[e.f] = data
	return &list{data: data}
}

func (e *element) Map() MarshalMap {
	data := map[string]interface{}{"key_value": []map[string]interface{}{}}
	e.data[e.f] = data
	return &marshMap{data: data}
}

func (e *element) Group() MarshalObject {
	obj := map[string]interface{}{}
	e.data[e.f] = obj
	return &object{data: obj}
}

type list struct {
	data map[string]interface{}
}

func (l *list) Add() MarshalElement {
	listData := l.data["list"].([]map[string]interface{})
	elemData := map[string]interface{}{}
	l.data["list"] = append(listData, elemData)
	e := &element{data: elemData, f: "element"}
	return e
}

type marshMap struct {
	data map[string]interface{}
}

func (l *marshMap) Add() MarshalMapElement {
	kvData := l.data["key_value"].([]map[string]interface{})
	elemData := map[string]interface{}{}
	l.data["key_value"] = append(kvData, elemData)
	me := &mapElement{data: elemData}
	return me
}

type mapElement struct {
	data map[string]interface{}
}

func (m *mapElement) Key() MarshalElement {
	return &element{data: m.data, f: "key"}
}

func (m *mapElement) Value() MarshalElement {
	return &element{data: m.data, f: "value"}
}

// NewMarshallObject creates a new marshaller object
func NewMarshallObject(data map[string]interface{}) MarshalObject {
	if data == nil {
		data = make(map[string]interface{})
	}
	return &object{
		data: data,
	}
}

// NewMarshalElement creates new marshall element object
func NewMarshalElement(data map[string]interface{}, name string) MarshalElement {
	if data == nil {
		data = make(map[string]interface{})
	}
	return &element{
		data: data,
		f:    name,
	}
}
