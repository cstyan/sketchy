package main

import (
	"fmt"
	"testing"
)

func TestHeap(t *testing.T) {
	h := MinHeap{
		10, make([]*node, 0),
	}
	h.Push("1", 10)
	h.Push("2", 30)
	h.Push("3", 20)
	//h.Push(4, 60)
	//h.Push(5, 80)
	//h.Push(6, 50)
	//h.Push(7, 70)

	fmt.Println(h.heap)
	c, _ := h.leftChild(0)
	fmt.Println("left child", c)

	c, _ = h.rightChild(0)
	fmt.Println("right child", c)

	c, _ = h.leftChild(1)
	fmt.Println("left child of 1", c)

}
