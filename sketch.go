package main

import (
	"errors"
	"hash"
	"math"
)

// CountMinSketch struct.
// d is the number of hashing functions,
// w is the size of every hash table.
// counters, a matrix, is used to store the count.
// uint is used to store count, the maximum count is 1<<32-1 in
// 32 bit OS, and 1<<64-1 in 64 bit OS.
type CountMinSketch struct {
	//numHashes int64
	//w         int64
	depth, length int64
	counters      [][]int64
	hashFuncs     []hash.Hash32
}

// NewSketch creates a new Count-Min Sketch with given error rate and confidence.
// Accuracy guarantees will be made in terms of a pair of user specified parameters,
// epsilon and delta, meaning that the error in answering a query is within a factor of epsilon with
// probability delta.
// see https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch
func NewSketch(epsilon, delta float64) (*CountMinSketch, error) {
	if epsilon <= 0 || epsilon >= 1 {
		return nil, errors.New("countminsketch: value of epsilon should be in range of (0, 1)")
	}
	if delta <= 0 || delta >= 1 {
		return nil, errors.New("countminsketch: value of delta should be in range of (0, 1)")
	}
	rowLen := math.Ceil(math.E / epsilon)
	numHashFuncs := uint(math.Ceil(math.Log(1 / delta)))
	//
	return &CountMinSketch{
		depth:    int64(numHashFuncs),
		length:   int64(rowLen),
		counters: make2dslice(int(numHashFuncs), int(rowLen)),
	}, nil
}

func make2dslice(row, col int) [][]int64 {
	ret := make([][]int64, row)
	for i := range ret {
		ret[i] = make([]int64, col)
	}
	return ret
}

// Add 'count' occurences of the given input
func (s *CountMinSketch) Add(h string, count int64) int64 {
	val := int64(math.MaxInt64)
	h1, h2 := hashn(h)
	for i := int64(0); i < s.depth; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(s.length)
		v := s.counters[i][pos] + count
		s.counters[i][pos] = v
		if v < val {
			val = v
		}
	}
	return val
}

// Min returns the approximate min count for the given input.
func (s *CountMinSketch) Min(h string) int64 {
	min := int64(math.MaxInt64)

	h1, h2 := hashn(h)
	for i := int64(0); i < s.depth; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(s.length)

		v := s.counters[i][pos]
		if v < min {
			min = v
		}
	}
	return min
}
