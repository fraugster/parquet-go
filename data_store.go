package go_parquet

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

// In memory (or maybe other type) of column store to buffer the column value before writing into a page
// TODO: tune the functions, maybe we need more information, maybe less.
type columnStore interface {
	Reset(repetitionType parquet.FieldRepetitionType)
	// Min and Max in parquet byte
	Max() []byte
	Min() []byte
	// Add One row, if the value is null, call Add() , if the value is repeated, call all value in array
	// the second argument s the definition level
	// if there is a data the the result should be true, if there is only null (or empty array), the the result should be false
	Add(data interface{}, defLvl int16, maxRepLvl int16, repLvl int16) (bool, error)
	// Len of all values, including repetition and nulls
	Len() int
	// Len of not nulls value
	NotNulls() int
	// len of all values, not repetition
	Column() int
	// Get all values
	Values() []interface{}
	// TODO: int16? since we write it in the parquet using int32 encoder
	DefinitionLevels() []int32

	RepetitionLevels() []int32
}

type column struct {
	name string
	// one of the following could be not null. data or children
	data     columnStore
	children []column

	rep parquet.FieldRepetitionType
}

func (c *column) print(prefix string) {
	fmt.Println(prefix + c.name)
	if c.children != nil {
		for i := range c.children {
			c.children[i].print(prefix + "->")
		}
	}
	if c.data != nil {
		v := c.data.Values()
		rep := strings.Repeat(" ", len(prefix))
		for i := range v {
			fmt.Printf("%s%+v\n", rep, v[i])
		}
	}
}

type rowStore struct {
	children []column
}

func (r *rowStore) findDataColumn(path string) (columnStore, error) {
	pa := strings.Split(path, ".")
	c := r.children
	var d columnStore
	for i := 0; i < len(pa); i++ {
		found := false
		for j := range c {
			if c[j].name == pa[i] {
				found = true
				d = c[j].data
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

	if d == nil {
		return nil, errors.Errorf("path %s doesnt end on data", path)
	}

	return d, nil
}

func (r *rowStore) print() {
	for i := range r.children {
		r.children[i].print("")
	}
}

func (r *rowStore) add(m map[string]interface{}) error {
	_, err := recursiveAdd(r.children, m, 0, 0, 0)
	return err
}

func recursiveNil(c []column, defLvl, maxRepLvl int16, repLvl int16) error {
	for i := range c {
		if c[i].data != nil {
			_, err := c[i].data.Add(nil, defLvl, maxRepLvl, repLvl)
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

func recursiveAdd(c []column, m interface{}, defLvl int16, maxRepLvl int16, repLvl int16) (bool, error) {
	var data = m.(map[string]interface{})
	var advance bool
	for i := range c {
		d := data[c[i].name]
		if c[i].data != nil {
			inc, err := c[i].data.Add(d, defLvl, maxRepLvl, repLvl)
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
