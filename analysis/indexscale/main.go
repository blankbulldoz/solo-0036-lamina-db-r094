package main

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/index"
	"lamina/internal/versions"
	"time"
)

func mk(n, revs int) (*catalog.Catalog, *versions.Store) {
	rel := catalog.Relation{Name: "spectra", Primary: "sample", Created: 1, Columns: []catalog.Column{
		{Name: "sample", Type: "int"}, {Name: "band", Type: "text"}, {Name: "amplitude", Type: "int", Nullable: true}, {Name: "valid", Type: "bool"}}}
	s := versions.New()
	s.Ensure("spectra")
	seq := uint64(1)
	for r := 0; r < revs; r++ {
		seq++
		for i := 0; i < n; i++ {
			row := datum.Row{"sample": datum.Integer(int64(i)), "band": datum.Value{Kind: "text", Text: fmt.Sprintf("band-%06d-r%02d", i, r)}, "amplitude": datum.Integer(int64(i)), "valid": datum.Value{Kind: "bool", Bool: true}}
			s.Tables["spectra"][rel.Key(row)] = append(s.Tables["spectra"][rel.Key(row)], versions.Version{Sequence: seq, Row: row})
		}
	}
	c := catalog.New()
	c.Relations["spectra"] = rel
	c.Indexes["by_band"] = catalog.Index{Name: "by_band", Relation: "spectra", Column: "band", Created: 2}
	c.Indexes["by_amp"] = catalog.Index{Name: "by_amp", Relation: "spectra", Column: "amplitude", Created: 2}
	return c, s
}

func main() {
	for _, tc := range []struct{ n, revs int }{{1000, 1}, {2000, 1}, {4000, 1}, {8000, 1}, {16000, 1}, {2000, 5}, {4000, 3}} {
		c, s := mk(tc.n, tc.revs)
		t0 := time.Now()
		ir := index.Build(c, s)
		dt := time.Since(t0)
		fmt.Printf("INDEX keys=%d revs=%d versions=%d entries_by_band=%d build_ms=%.0f\n", tc.n, tc.revs, tc.n*tc.revs, ir.Trees["by_band"].Size(), float64(dt.Microseconds())/1000)
	}
}
