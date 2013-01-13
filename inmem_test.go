package kv

import (
	"testing"
)

func TestTableDatabase(t *testing.T) {

	root := NewMemoryTable()
	db := FromTable(root)
	fruit, _ := db.Open("fruit")
	animals, _ := db.Open("animals")

	StrStore(fruit, "apple", "red or green")
	StrStore(fruit, "banana", "yellow")
	StrStore(fruit, "dog", "dogs aren't fruit!")

	StrStore(animals, "dog", "WOOF!")
	StrStore(animals, "apple", "not really an animal")

	doTest := func(table Table, tests map[string]string) {
		for key, expect := range tests {
			if val, _ := StrGet(table, key); val != expect {
				t.Errorf("incorrect result for '%s': %s (expected %s)", key, val, expect)
			}
		}
	}

	doTest(fruit, map[string]string{
		"apple":  "red or green",
		"banana": "yellow",
		"dog":    "dogs aren't fruit!",
	})

	doTest(animals, map[string]string{
		"apple": "not really an animal",
		"dog":   "WOOF!",
	})

}
