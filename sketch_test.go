package main

import (
	"fmt"
	"github.com/DmitriyVTitov/size"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"strconv"
	"testing"
)

type event struct {
	name  string
	count int
}

func TestAccuracy(t *testing.T) {
	type event struct {
		name  string
		count int64
	}
	nStreamNames := 1000
	diff := 5
	maxInserts := 100
	// 99.9% chance of being within 1 diff range
	accuracy := 0.001
	names := make([]event, nStreamNames)
	for i := 0; i < len(names); i++ {
		n := rand.Intn(maxInserts) + 1
		names[i].name = RandStringRunes(100)
		names[i].count = int64(n)
	}
	for i := 0; i < 100; i++ {
		eps := float64(diff) / float64(nStreamNames*(maxInserts/2))
		s, err := NewSketch(eps, accuracy)
		if err != nil {
			t.Error(err)
		}

		for i := 0; i < len(names); i++ {
			s.Add(names[i].name, names[i].count)
		}

		var miss int
		for i := 0; i < len(names); i++ {
			vv := names[i].count

			v := s.Min(names[i].name)
			assert.Equal(t, v >= v, true)
			// we're okay with overcounting by 1, undercounting is not allowed at all
			if v > vv+int64(diff) || v < vv {
				log.Printf("real: %d, estimate: %d\n", vv, v)
				miss++
			}
		}
		// we wanted a 99.9% chance of being in range, for 1000 streams that should mean miss shouldn't be > 1
		assert.LessOrEqual(t, miss, 1, "had more misses than we expected: %d", miss)
	}
}

func TestSketch(t *testing.T) {
	events := make([]event, 0)
	// lets insert 0-999 some amount of times between 6-10 times
	for i := 0; i < 1000; i++ {
		n := rand.Intn(5) + 6
		for j := 5; j <= n; j++ {
			events = append(events, event{name: strconv.Itoa(i), count: 1})
			//cms.Add(strconv.Itoa(i), 1),
		}
	}
	// then add 1000 between 1 and 5 times
	n := rand.Intn(5)
	for i := 0; i < n; i++ {
		events = append(events, event{name: "1000", count: 1})
	}
	// we're okay with being 1 element away from the real count
	eps := 1 / float64(len(events))
	cms, _ := NewSketch(eps, 0.001)

	for _, e := range events {
		cms.Add(e.name, int64(e.count))
	}

	assert.Less(t, cms.Min("1000"), cms.Min("0"), "min for 1000 should be lower than all others")
	fmt.Println("size: ", size.Of(cms))
}

func BenchmarkSketch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		cms, _ := NewSketch(0.001, 0.999)
		// lets insert 0-999 some amount of times between 10-14 times
		for i := 0; i < 1000; i++ {
			n := rand.Intn(5) + 10
			for j := 5; j <= n; j++ {
				cms.Add(strconv.Itoa(i), 1)
			}
		}
		// then add 1000 between 1 and 5 times
		n := rand.Intn(5)
		for i := 0; i < n; i++ {
			cms.Add("1000", 1)
		}

		assert.Less(b, cms.Min("1000"), cms.Min("0"), "min for 1000 should be lower than all others")
	}

}
