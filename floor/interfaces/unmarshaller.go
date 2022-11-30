package interfaces

import (
	"errors"
	"fmt"
)

var (
	// ErrFieldNotPresent indicate the field is not available in the result
	ErrFieldNotPresent = errors.New("field is not present")
)

// Unmarshaller is the interface necessary for objects to
// be unmarshalled.
type Unmarshaller interface {
	UnmarshalParquet(obj UnmarshalObject) error
}

// UnmarshalObject is the interface an Unmarshaller needs to unmarshal its data
// from.
type UnmarshalObject interface {
	GetField(field string) UnmarshalElement

	GetData() map[string]interface{}
}

// UnmarshalElement describes the interface to get the value of an element in an Unmarshaller
// implementation.
type UnmarshalElement interface {
	Group() (UnmarshalObject, error)
	Int32() (int32, error)
	Int64() (int64, error)
	Int96() ([12]byte, error)
	Float32() (float32, error)
	Float64() (float64, error)
	Bool() (bool, error)
	ByteArray() ([]byte, error)
	List() (UnmarshalList, error)
	Map() (UnmarshalMap, error)
	Error() error
}

// UnmarshalList describes the interface to get the values of a list in an Unmarshaller
// implementation.
type UnmarshalList interface {
	Next() bool
	Value() (UnmarshalElement, error)
}

// UnmarshalMap describes the interface to get key-value pairs of a map in an Unmarshaller
// implementation.
type UnmarshalMap interface {
	Next() bool
	Key() (UnmarshalElement, error)
	Value() (UnmarshalElement, error)
}

type unmarshalErr struct {
}

func (u unmarshalErr) Error() error {
	return ErrFieldNotPresent
}

func (u unmarshalErr) Group() (UnmarshalObject, error) {
	return nil, ErrFieldNotPresent
}

func (u unmarshalErr) Int32() (int32, error) {
	return 0, ErrFieldNotPresent
}

func (u unmarshalErr) Int64() (int64, error) {
	return 0, ErrFieldNotPresent
}

func (u unmarshalErr) Int96() ([12]byte, error) {
	return [12]byte{}, ErrFieldNotPresent
}

func (u unmarshalErr) Float32() (float32, error) {
	return 0, ErrFieldNotPresent
}

func (u unmarshalErr) Float64() (float64, error) {
	return 0, ErrFieldNotPresent
}

func (u unmarshalErr) Bool() (bool, error) {
	return false, ErrFieldNotPresent
}

func (u unmarshalErr) ByteArray() ([]byte, error) {
	return nil, ErrFieldNotPresent
}

func (u unmarshalErr) List() (UnmarshalList, error) {
	return nil, ErrFieldNotPresent
}

func (u unmarshalErr) Map() (UnmarshalMap, error) {
	return nil, ErrFieldNotPresent
}

func (o *object) GetField(field string) UnmarshalElement {
	fieldData, ok := o.data[field]
	if !ok {
		return &unmarshalErr{}
	}

	return &unmarshElem{data: fieldData}
}

type unmarshElem struct {
	data interface{}
}

func (e *unmarshElem) Error() error {
	return nil
}

func (e *unmarshElem) Group() (UnmarshalObject, error) {
	data, ok := e.data.(map[string]interface{})
	if !ok {
		return nil, errors.New("field is not a group")
	}

	return &object{data: data}, nil
}

func (e *unmarshElem) Int32() (int32, error) {
	i, ok := e.data.(int32)
	if !ok {
		return 0, fmt.Errorf("expected int32, found %T instead", e.data)
	}
	return i, nil
}

func (e *unmarshElem) Int64() (int64, error) {
	i, ok := e.data.(int64)
	if !ok {
		return 0, fmt.Errorf("expected int64, found %T instead", e.data)
	}
	return i, nil
}

func (e *unmarshElem) Int96() ([12]byte, error) {
	i, ok := e.data.([12]byte)
	if !ok {
		return [12]byte{}, fmt.Errorf("expected [12]byte, found %T instead", e.data)
	}
	return i, nil
}

func (e *unmarshElem) Float32() (float32, error) {
	f, ok := e.data.(float32)
	if !ok {
		return 0, fmt.Errorf("expected float32, found %T instead", e.data)
	}
	return f, nil
}

func (e *unmarshElem) Float64() (float64, error) {
	f, ok := e.data.(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64, found %T instead", e.data)
	}
	return f, nil
}

func (e *unmarshElem) Bool() (bool, error) {
	f, ok := e.data.(bool)
	if !ok {
		return false, fmt.Errorf("expected bool, found %T instead", e.data)
	}
	return f, nil
}

func (e *unmarshElem) ByteArray() ([]byte, error) {
	f, ok := e.data.([]byte)
	if !ok {
		return nil, fmt.Errorf("expected []byte, found %T instead", e.data)
	}
	return f, nil
}

func (e *unmarshElem) List() (UnmarshalList, error) {
	data, ok := e.data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data is not a list, found %T instead", e.data)
	}

	isAthenaList := false
	listData, ok := data["list"]
	if !ok {
		listData, ok = data["bag"]
		if !ok {
			return nil, errors.New("sub-group list or bag not found")
		}
		isAthenaList = true
	}

	elemList, ok := listData.([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected sub-group list to be []map[string]interface{}, got %T instead", listData)
	}

	return &unmarshList{list: elemList, idx: -1, isAthenaList: isAthenaList}, nil
}

func (e *unmarshElem) Map() (UnmarshalMap, error) {
	data, ok := e.data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data is not a map, found %T instead", e.data)
	}

	if len(data) == 0 {
		return &unmarshMap{data: []map[string]interface{}{}, idx: -1}, ErrFieldNotPresent
	}

	kvData, ok := data["key_value"]
	if !ok {
		kvData, ok = data["map"]
		if !ok {
			return nil, errors.New("sub-group key_value not found")
		}
	}

	kvList, ok := kvData.([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected sub-group key_value to be []map[string]interface{}, got %T instead", kvData)
	}

	return &unmarshMap{data: kvList, idx: -1}, nil
}

type unmarshList struct {
	list         []map[string]interface{}
	idx          int
	isAthenaList bool
}

func (l *unmarshList) Next() bool {
	l.idx++
	return l.idx < len(l.list)
}

func (l *unmarshList) Value() (UnmarshalElement, error) {
	if l.idx >= len(l.list) {
		return nil, errors.New("iterator has reached end of list")
	}

	elemName := "element"
	if l.isAthenaList {
		elemName = "array_element"
	}

	elem, ok := l.list[l.idx][elemName]
	if !ok {
		return nil, fmt.Errorf("%s not found in current list element", elemName)
	}

	return &unmarshElem{data: elem}, nil
}

type unmarshMap struct {
	data []map[string]interface{}
	idx  int
}

func (m *unmarshMap) Next() bool {
	m.idx++
	return m.idx < len(m.data)
}

func (m *unmarshMap) Key() (UnmarshalElement, error) {
	if m.idx >= len(m.data) {
		return nil, errors.New("iterator has reached end of map")
	}

	elem, ok := m.data[m.idx]["key"]
	if !ok {
		return nil, errors.New("key not found in current map element")
	}

	return &unmarshElem{data: elem}, nil
}

func (m *unmarshMap) Value() (UnmarshalElement, error) {
	if m.idx >= len(m.data) {
		return nil, errors.New("iterator has reached end of map")
	}

	elem, ok := m.data[m.idx]["value"]
	if !ok {
		return nil, errors.New("value not found in current map element")
	}

	return &unmarshElem{data: elem}, nil
}

// NewUnmarshallObject creates a new unmarshaller object
func NewUnmarshallObject(data map[string]interface{}) UnmarshalObject {
	if data == nil {
		data = make(map[string]interface{})
	}
	return &object{
		data: data,
	}
}

// NewUnmarshallElement creates new unmarshall element object
func NewUnmarshallElement(data interface{}) UnmarshalElement {
	return &unmarshElem{
		data: data,
	}
}
