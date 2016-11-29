package models

import (
	"sort"
)

//go:generate msgp -io=true -marshal=false -o=encode.go

// Row represents a single row returned from the execution of a statement.
type Row struct {
	Name    string            `json:"name,omitempty" msg:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty" msg:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty" msg:"columns,omitempty"`
	Values  [][]interface{}   `json:"values,omitempty" msg:"values,omitempty"`
	Partial bool              `json:"partial,omitempty" msg:"values,omitempty"`
}

// SameSeries returns true if r contains values for the same series as o.
func (z *Row) SameSeries(o *Row) bool {
	return z.tagsHash() == o.tagsHash() && z.Name == o.Name
}

// tagsHash returns a hash of tag key/value pairs.
func (z *Row) tagsHash() uint64 {
	h := NewInlineFNV64a()
	keys := z.tagsKeys()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte(z.Tags[k]))
	}
	return h.Sum64()
}

// tagKeys returns a sorted list of tag keys.
func (z *Row) tagsKeys() []string {
	a := make([]string, 0, len(z.Tags))
	for k := range z.Tags {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// Rows represents a collection of rows. Rows implements sort.Interface.
type Rows []*Row

func (z Rows) Len() int { return len(z) }

func (z Rows) Less(i, j int) bool {
	// Sort by name first.
	if z[i].Name != z[j].Name {
		return z[i].Name < z[j].Name
	}

	// Sort by tag set hash. Tags don't have a meaningful sort order so we
	// just compute a hash and sort by that instead. This allows the tests
	// to receive rows in a predictable order every time.
	return z[i].tagsHash() < z[j].tagsHash()
}

func (z Rows) Swap(i, j int) { z[i], z[j] = z[j], z[i] }
