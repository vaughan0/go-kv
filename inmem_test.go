package kv

import (
	"testing"
)

func TestTableDatabase(t *testing.T) {

	root := NewMemoryTable()
	db := FromTable(root)

	data := map[string]map[string]string{
		"fruit": {
			"apple":  "red or green",
			"banana": "yellow",
			"dog":    "dogs aren't fruit!",
		},
		"animals": {
			"apple": "not really an animal",
			"dog":   "WOOF!",
		},
	}

	for group, entries := range data {
		table, err := db.Open(group)
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range entries {
			if err := StrStore(table, key, value); err != nil {
				t.Fatal(err)
			}
		}
		table.Close()
	}

	for group, entries := range data {
		table, err := db.Open(group)
		if err != nil {
			t.Fatal(err)
		}
		for key, expect := range entries {
			value, err := StrGet(table, key)
			if err != nil {
				t.Fatal(err)
			}
			if value != expect {
				t.Errorf("incorrect result for %s.%s: %s (expected %s)", group, key, value, expect)
			}
		}
	}

}
