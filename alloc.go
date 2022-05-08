package goparquet

import (
	"fmt"
	"runtime"
)

type allocTracker struct {
	allocs    map[interface{}]uint64
	totalSize uint64
	maxSize   uint64
}

func newAllocTracker(maxSize uint64) *allocTracker {
	return &allocTracker{
		allocs:  make(map[interface{}]uint64),
		maxSize: maxSize,
	}
}

func (t *allocTracker) register(obj interface{}, size uint64) {
	if t == nil {
		return
	}

	if _, ok := obj.([]byte); ok {
		obj = &obj
	}

	if _, ok := t.allocs[obj]; ok { // object has already been tracked, no need to add it.
		return
	}

	t.allocs[obj] = size
	t.totalSize += size

	runtime.SetFinalizer(obj, t.finalize)

	if t.maxSize > 0 && t.totalSize > t.maxSize {
		t.doPanic(t.totalSize)
	}
}

func (t *allocTracker) test(size uint64) {
	if t == nil {
		return
	}
	if t.maxSize > 0 && t.totalSize+size > t.maxSize {
		t.doPanic(t.totalSize + size)
	}
}

func (t *allocTracker) doPanic(totalSize uint64) {
	if t == nil {
		return
	}
	panic(fmt.Errorf("memory usage of %d bytes is greater than configured maximum of %d bytes", totalSize, t.maxSize))
}

func (t *allocTracker) finalize(obj interface{}) {
	if t == nil {
		return
	}

	if _, ok := obj.([]byte); ok {
		obj = &obj
	}

	size, ok := t.allocs[obj]
	if !ok { // if object hasn't been tracked, do nothing.
		return
	}

	// remove size from total size, and unregister from tracker.
	t.totalSize -= size
	delete(t.allocs, obj)
}
