package main

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: genjournal <dir> <keys> <revs> [text]")
		os.Exit(2)
	}
	dir := os.Args[1]
	keys, _ := strconv.Atoi(os.Args[2])
	revs, _ := strconv.Atoi(os.Args[3])
	text := 64
	if len(os.Args) > 4 {
		text, _ = strconv.Atoi(os.Args[4])
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		panic(err)
	}
	w, err := journal.Open(dir, 0)
	if err != nil {
		panic(err)
	}
	r := catalog.Relation{Name: "spectra", Primary: "sample", Columns: []catalog.Column{
		{Name: "sample", Type: "int"}, {Name: "band", Type: "text"},
		{Name: "amplitude", Type: "int", Nullable: true}, {Name: "valid", Type: "bool"},
	}, Created: 1}
	if err := w.Append(journal.Record{Sequence: 1, Kind: "relation", Relation: &r}); err != nil {
		panic(err)
	}
	seq := uint64(1)
	per := 1000
	for rev := 1; rev <= revs; rev++ {
		seq++
		writes := make([]versions.Mutation, 0, keys)
		for i := 0; i < keys; i++ {
			band := fmt.Sprintf("band-%06d-r%02d", i, rev)
			for len(band) < text {
				band += "x"
			}
			row := datum.Row{"sample": datum.Integer(int64(i + 1)), "band": datum.Value{Kind: "text", Text: band[:text]},
				"amplitude": datum.Integer(int64((i*7 + rev*13) % 1000)), "valid": datum.Value{Kind: "bool", Bool: (i+rev)%2 == 0}}
			writes = append(writes, versions.Mutation{Relation: "spectra", Key: r.Key(row), Row: row})
		}
		// split one revision into key-bounded commits to respect 1000-key transaction limit
		for start := 0; start < len(writes); start += per {
			end := start + per
			if end > len(writes) {
				end = len(writes)
			}
			if start > 0 {
				seq++
			}
			if err := w.Append(journal.Record{Sequence: seq, Kind: "commit", Writes: writes[start:end]}); err != nil {
				panic(err)
			}
		}
	}
	w.Close()
	fmt.Printf("generated dir=%s keys=%d revs=%d versions=%d last_seq=%d\n", dir, keys, revs, keys*revs, seq)
}
