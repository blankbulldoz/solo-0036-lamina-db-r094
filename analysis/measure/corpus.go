package main

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/versions"
)

func corpus(keys, revs, textLen int) (*versions.Store, *catalog.Catalog) {
	rel := catalog.Relation{Name: "spectra", Primary: "sample", Created: 1, Columns: []catalog.Column{
		{Name: "sample", Type: "int"}, {Name: "band", Type: "text"},
		{Name: "amplitude", Type: "int", Nullable: true}, {Name: "valid", Type: "bool"}}}
	s := versions.New()
	s.Ensure("spectra")
	seq := uint64(1)
	for r := 0; r < revs; r++ {
		seq++
		for i := 0; i < keys; i++ {
			band := fmt.Sprintf("band-%06d-r%02d", i, r)
			for len(band) < textLen {
				band += "x"
			}
			row := datum.Row{"sample": datum.Integer(int64(i)), "band": datum.Value{Kind: "text", Text: band[:textLen]},
				"amplitude": datum.Integer(int64(i)), "valid": datum.Value{Kind: "bool", Bool: true}}
			s.Tables["spectra"][rel.Key(row)] = append(s.Tables["spectra"][rel.Key(row)], versions.Version{Sequence: seq, Row: row})
		}
	}
	c := catalog.New()
	c.Relations["spectra"] = rel
	c.Indexes["by_band"] = catalog.Index{Name: "by_band", Relation: "spectra", Column: "band", Created: 2}
	return s, c
}
