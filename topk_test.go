package main

import (
	"github.com/DmitriyVTitov/size"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	gen := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[gen.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestTopK(t *testing.T) {
	topk, err := NewTopk(3)
	assert.NoError(t, err)
	// lets insert 0-9 some amount of times between 1-5 times
	for i := 0; i < 10; i++ {
		n := rand.Intn(5) + 1
		for j := 0; j <= n; j++ {
			topk.Observe(strconv.Itoa(i))
		}
	}
	for i := 10; i < 13; i++ {
		n := rand.Intn(6)
		for j := 0; j < 10+n; j++ {
			topk.Observe(strconv.Itoa(i))
		}
	}

	// todo, check for the expected top k
	//fmt.Println(topk.Topk())
}

func BenchmarkTopK(b *testing.B) {
	names := make([]string, 100000)
	for i := 0; i < len(names); i++ {
		names[i] = RandStringRunes(100)
	}
	for i := 0; i < b.N; i++ {
		topk, err := NewTopk(3)
		sketch, _ := NewSketch(0.001, 0.01)
		topk.sketch = sketch

		assert.NoError(b, err)
		for i := 0; i < len(names)-3; i++ {
			for j := 0; j <= 10; j++ {
				topk.Observe(strconv.Itoa(i))
			}
		}
		for i := len(names) - 3; i < len(names); i++ {
			for j := 0; j < 100; j++ {
				topk.Observe(strconv.Itoa(i))
			}
		}
		b.ReportMetric(float64(size.Of(topk)), "struct_size")
	}
}

func BenchmarkTopKBad(b *testing.B) {
	names := make([]string, 100000)
	for i := 0; i < len(names); i++ {
		names[i] = RandStringRunes(100)
	}
	for i := 0; i < b.N; i++ {
		topk, err := NewBadTopk(3)
		assert.NoError(b, err)
		for i := 0; i < len(names)-3; i++ {
			for j := 0; j <= 10; j++ {
				topk.Observe(strconv.Itoa(i))
			}
		}
		for i := len(names) - 3; i < len(names); i++ {
			for j := 0; j < 100; j++ {
				topk.Observe(strconv.Itoa(i))
			}
		}
		b.ReportMetric(float64(size.Of(topk)), "struct_size")
	}
}
