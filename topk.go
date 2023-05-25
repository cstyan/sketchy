package main

import (
	"hash/fnv"
)

// Topk is a structure that uses a Count Min Sketch, Min-Heap, and slice of int64,
// the latter two of which have len(k), to track the top k events by frequency.
// The reason the last element is a slice is because we're likely getting the TopK
// where k is a small amount, say 10 or 100. In that case, traversing the slice to check
// for existence of a particular hash isn't that much more costly than doing so via a map
// and we avoid the extra memory overhead of using a map.
type Topk struct {
	max int
	// slice of the hashes for things we've seen, tracks
	currentTop []string
	heap       *MinHeap
	sketch     *CountMinSketch
}

func NewTopk(k int) (Topk, error) {
	s, err := NewSketch(0.001, 0.999)
	if err != nil {
		return Topk{}, err
	}
	return Topk{
		max:        k,
		currentTop: make([]string, 0, k),
		heap:       NewMinHeap(k), //make heap,
		sketch:     s,
	}, nil
}

func (t *Topk) Observe(event string) {
	t.sketch.Add(event, 1)
	estimate := t.sketch.Min(event)
	// check if the event is already in the topk, if it is we should update it's count
	if t.InTopk(event) {
		t.heap.UpdateValue(event)
		return
	}
	if estimate > t.heap.min().count {
		a := fnv.New64()
		a.Write([]byte(event))
		// remove the min event from the heap
		if len(t.currentTop) == t.max {
			min := t.heap.Pop()
			removeIndex := -1
			for i, e := range t.currentTop {
				if e == min.event {
					removeIndex = i
				}
			}
			// just to be safe, but this should never happen
			if removeIndex > -1 {
				t.currentTop[removeIndex] = t.currentTop[len(t.currentTop)-1]
				t.currentTop = t.currentTop[:len(t.currentTop)-1]
			}
		}

		insertEstimate := t.sketch.Min(event)
		// insert the new event onto the heap
		t.heap.Push(event, insertEstimate)
		t.currentTop = append(t.currentTop, event)
	}
}

// InTopk checks to see if an event is already in the topk for this query
func (t *Topk) InTopk(event string) bool {
	// check for the thing
	for _, e := range t.currentTop {
		if e == event {
			return true
		}
	}
	return false
}

type TopKResult struct {
	Event string
	Count int64
}

func (t *Topk) Topk() []TopKResult {
	res := make([]TopKResult, len(t.currentTop))
	for i, e := range t.currentTop {
		res[i] = TopKResult{
			Event: e,
			Count: t.sketch.Min(e),
		}
	}
	return res
}

type BadTopk struct {
	max      int
	heap     *MinHeap
	counters map[string]int64
	// slice of the hashes for things we've seen, tracks
	currentTop []string
}

func NewBadTopk(k int) (BadTopk, error) {
	return BadTopk{
		max:        k,
		currentTop: make([]string, 0, k),
		counters:   make(map[string]int64),
		heap:       NewMinHeap(k), //make heap,
	}, nil
}

func (bt *BadTopk) Observe(event string) {
	bt.counters[event] += 1
	if bt.InTopk(event) {
		bt.heap.UpdateValue(event)
		return
	}
	if bt.counters[event] > bt.heap.min().count {
		// remove the min event from the heap
		if len(bt.currentTop) == bt.max {
			min := bt.heap.Pop()
			removeIndex := -1
			for i, e := range bt.currentTop {
				if e == min.event {
					removeIndex = i
				}
			}
			// just to be safe, but this should never happen
			if removeIndex > -1 {
				bt.currentTop[removeIndex] = bt.currentTop[len(bt.currentTop)-1]
				bt.currentTop = bt.currentTop[:len(bt.currentTop)-1]
			}
		}
	}
	bt.heap.Push(event, bt.counters[event])
	bt.currentTop = append(bt.currentTop, event)
}

func (bt *BadTopk) InTopk(event string) bool {
	_, ok := bt.counters[event]
	return ok
}
