package go_parquet

import (
	"github.com/fraugster/parquet-go/parquet"
)

// In memory (or maybe other type) of column store to buffer the column value before writing into a page
// TODO: tune the functions, maybe we need more information, maybe less.
type columnStore interface {
	// TODO: pass maxR and maxD
	reset(repetitionType parquet.FieldRepetitionType)
	// TODO: sort of redundant function to handle reset without losing the schema. maybe remove te `reset` argument and add a new function?
	repetitionType() parquet.FieldRepetitionType
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

func (g *genericStore) repetitionType() parquet.FieldRepetitionType {
	return g.repTyp
}

func (g *genericStore) reset(rep parquet.FieldRepetitionType) {
	if g.typedColumnStore == nil {
		panic("generic should be used with typed column store")
	}
	g.repTyp = rep
	if g.values == nil {
		g.values = &dictStore{}
	}
	g.values.init()
	g.dLevels = g.dLevels[:0]
	g.rLevels = g.rLevels[:0]
	g.rep = g.rep[:0]

	g.typedColumnStore.reset(rep)
}

func (g *genericStore) add(v interface{}, dL uint16, maxRL, rL uint16) (bool, error) {
	// if the current column is repeated, we should increase the maxRL here
	if g.repTyp == parquet.FieldRepetitionType_REPEATED {
		maxRL++
	}
	if rL > maxRL {
		rL = maxRL
	}
	// the dL is a little tricky. there is some case if the REQUIRED field here are nil (since there is something above
	// them is nil) they can not be the first level, but if they are in the next levels, is actually ok, but the
	// level is one less
	if v == nil {
		g.dLevels = append(g.dLevels, int32(dL))
		g.rLevels = append(g.rLevels, int32(rL))
		// TODO: the next line is the problem. how I can ignore the nil value here? I need the count to be exact, but nil
		// should I save it in the dictionary?
		g.values.addValue(nil)
		return false, nil
	}
	vals, err := g.getValues(v)
	if err != nil {
		return false, err
	}
	if len(vals) == 0 {
		// the MaxRl might be increased in the beginning and increased again in the next call but for nil its not important
		return g.add(nil, dL, maxRL, rL)
	}

	g.rep = append(g.rep, len(vals))
	for i, j := range vals {
		g.values.addValue(j)
		tmp := dL
		if g.repTyp != parquet.FieldRepetitionType_REQUIRED {
			tmp++
		}
		g.dLevels = append(g.dLevels, int32(tmp))
		if i == 0 {
			g.rLevels = append(g.rLevels, int32(rL))
		} else {
			g.rLevels = append(g.rLevels, int32(maxRL))
		}
	}

	return true, nil
}

func (g *genericStore) dictionary() *dictStore {
	return g.values
}

func (g *genericStore) definitionLevels() []int32 {
	return g.dLevels
}

func (g *genericStore) repetitionLevels() []int32 {
	return g.rLevels
}

func newStore(typed typedColumnStore) columnStore {
	return &genericStore{typedColumnStore: typed}
}
