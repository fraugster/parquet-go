package go_parquet

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

// In memory (or maybe other type) of column store to buffer the column value before writing into a page
// TODO: tune the functions, maybe we need more information, maybe less.
type columnStore interface {
	// TODO: pass maxR and maxD
	reset(repetitionType parquet.FieldRepetitionType)
	// Min and Max in parquet byte
	maxValue() []byte
	minValue() []byte
	// Add One row, if the value is null, call Add() , if the value is repeated, call all value in array
	// the second argument s the definition level
	// if there is a data the the result should be true, if there is only null (or empty array), the the result should be false
	add(data interface{}, defLvl uint16, maxRepLvl uint16, repLvl uint16) (bool, error)
	// Get all values
	dictionary() *dictStore
	// TODO: uint16? since we write it in the parquet using int32 encoder
	definitionLevels() []int32

	repetitionLevels() []int32

	//// we can use the array to get this two, but its better to skip the loop
	// TODO: uncomment this after fixing the todo on the init
	//maxDefinitionLevel() uint16
	//maxRepetitionLevel() uint16
}

type typedColumnStore interface {
	reset(repetitionType parquet.FieldRepetitionType)
	// Min and Max in parquet byte
	maxValue() []byte
	minValue() []byte

	// Should extract the value, turn it into an array and check for min and max on all values in this
	getValues(v interface{}) ([]interface{}, error)
}

// genericStore is a hack to less duplicate code and logic on each type. there is a place that we can actually benefit from
// generics :/
type genericStore struct {
	repTyp parquet.FieldRepetitionType

	values *dictStore

	dLevels []int32
	rLevels []int32
	rep     []int

	typedColumnStore
}

func (is *genericStore) reset(rep parquet.FieldRepetitionType) {
	if is.typedColumnStore == nil {
		panic("generic should be used with typed column store")
	}
	is.repTyp = rep
	if is.values == nil {
		is.values = &dictStore{}
	}
	is.values.init()
	is.dLevels = is.dLevels[:0]
	is.rLevels = is.rLevels[:0]
	is.rep = is.rep[:0]

	is.typedColumnStore.reset(rep)
}

func (is *genericStore) add(v interface{}, dL uint16, maxRL, rL uint16) (bool, error) {
	// if the current column is repeated, we should increase the maxRL here
	if is.repTyp == parquet.FieldRepetitionType_REPEATED {
		maxRL++
	}
	if rL > maxRL {
		rL = maxRL
	}
	// the dL is a little tricky. there is some case if the REQUIRED field here are nil (since there is something above
	// them is nil) they can not be the first level, but if they are in the next levels, is actually ok, but the
	// level is one less
	if v == nil {
		is.dLevels = append(is.dLevels, int32(dL))
		is.rLevels = append(is.rLevels, int32(rL))
		// TODO: the next line is the problem. how I can ignore the nil value here? I need the count to be exact, but nil
		// should I save it in the dictionary?
		is.values.addValue(nil)
		return false, nil
	}
	vals, err := is.getValues(v)
	if err != nil {
		return false, err
	}
	if len(vals) == 0 {
		// the MaxRl might be increased in the beginning and increased again in the next call but for nil its not important
		return is.add(nil, dL, maxRL, rL)
	}

	is.rep = append(is.rep, len(vals))
	for i, j := range vals {
		is.values.addValue(j)
		tmp := dL
		if is.repTyp != parquet.FieldRepetitionType_REQUIRED {
			tmp++
		}
		is.dLevels = append(is.dLevels, int32(tmp))
		if i == 0 {
			is.rLevels = append(is.rLevels, int32(rL))
		} else {
			is.rLevels = append(is.rLevels, int32(maxRL))
		}
	}

	return true, nil
}

func (is *genericStore) dictionary() *dictStore {
	return is.values
}

func (is *genericStore) definitionLevels() []int32 {
	return is.dLevels
}

func (is *genericStore) repetitionLevels() []int32 {
	return is.rLevels
}

type column struct {
	index          int
	name, flatName string
	// one of the following could be not null. data or children
	data     columnStore
	children []column

	rep parquet.FieldRepetitionType

	maxR, maxD uint16

	// for the reader we should read this element from the meta, for the writer we need to build this element
	element *parquet.SchemaElement
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
	return c.element
}

// TODO: merge this type with the Schema type
type rowStore struct {
	children []column
}

func (r *rowStore) sortIndex() {
	var (
		idx int
		fn  func(c *[]column)
	)

	fn = func(c *[]column) {
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

	fn(&r.children)

}

// path is the dot separated path of the group, the parent group should be there or it will return an error
func (r *rowStore) addGroup(path string, rep parquet.FieldRepetitionType) error {
	return r.addColumnOrGroup(path, nil, rep)
}

// path is the dot separated path of the group, the parent group should be there or it will return an error
func (r *rowStore) addColumn(path string, store columnStore, rep parquet.FieldRepetitionType) error {
	if store == nil {
		return errors.New("column should have column store")
	}
	return r.addColumnOrGroup(path, store, rep)
}

// do not call this function externally
func (r *rowStore) addColumnOrGroup(path string, store columnStore, rep parquet.FieldRepetitionType) error {
	var (
		maxR, maxD uint16
	)
	if r.children == nil {
		r.children = []column{}
	}
	pa := strings.Split(path, ".")
	name := strings.Trim(pa[len(pa)-1], " \n\r\t")
	if name == "" {
		return errors.Errorf("the name of the column is required")
	}

	c := &r.children
	for i := 0; i < len(pa)-1; i++ {
		found := false
		if c == nil {
			break
		}
		for j := range *c {
			if (*c)[j].name == pa[i] {
				found = true
				maxR = (*c)[j].maxR
				maxD = (*c)[j].maxD
				c = &(*c)[j].children
				break
			}
		}
		if !found {
			return errors.Errorf("path %s failed on %q", path, pa[i])
		}
		if c == nil && i < len(pa)-1 {
			return errors.Errorf("path %s is not parent at %q", path, pa[i])
		}
	}

	if c == nil {
		return errors.New("the children are nil")
	}
	if rep != parquet.FieldRepetitionType_REQUIRED {
		maxD++
	}
	if rep == parquet.FieldRepetitionType_REPEATED {
		maxR++
	}

	col := column{
		name:     name,
		flatName: path,
		data:     nil,
		children: nil,
		rep:      rep,
		maxR:     maxR,
		maxD:     maxD,
	}
	if store != nil {
		store.reset(rep)
		col.data = store
	} else {
		col.children = []column{}
	}

	*c = append(*c, col)
	r.sortIndex()

	return nil
}

func (r *rowStore) findDataColumn(path string) (*column, error) {
	pa := strings.Split(path, ".")
	c := r.children
	var ret *column
	for i := 0; i < len(pa); i++ {
		found := false
		for j := range c {
			if c[j].name == pa[i] {
				found = true
				ret = &c[j]
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

func (r *rowStore) addData(m map[string]interface{}) error {
	_, err := recursiveAdd(r.children, m, 0, 0, 0)
	return err
}

func recursiveNil(c []column, defLvl, maxRepLvl uint16, repLvl uint16) error {
	for i := range c {
		if c[i].data != nil {
			_, err := c[i].data.add(nil, defLvl, maxRepLvl, repLvl)
			if err != nil {
				return err
			}
		}
		if c[i].children != nil {
			if err := recursiveNil(c, defLvl, maxRepLvl, repLvl); err != nil {
				return err
			}
		}
	}
	return nil
}

func recursiveAdd(c []column, m interface{}, defLvl uint16, maxRepLvl uint16, repLvl uint16) (bool, error) {
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
				if err := recursiveNil(c[i].children, l, maxRepLvl, repLvl); err != nil {
					return false, err
				}
			case map[string]interface{}: // Not repeated
				if c[i].rep == parquet.FieldRepetitionType_REPEATED {
					return false, errors.Errorf("repeated group should be array")
				}
				_, err := recursiveAdd(c[i].children, v, l, maxRepLvl, repLvl)
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
					inc, err := recursiveAdd(c[i].children, v[vi], l, m, rL)
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

func newStore(typed typedColumnStore) columnStore {
	return &genericStore{typedColumnStore: typed}
}
