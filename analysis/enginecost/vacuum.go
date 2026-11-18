package main

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/engine"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"os"
	"time"
)

func buildJournal(dir string, keys, revs, textLen int) {
	r := catalog.Relation{Name: "spectra", Primary: "sample", Created: 1, Columns: []catalog.Column{
		{Name: "sample", Type: "int"}, {Name: "band", Type: "text"}, {Name: "amplitude", Type: "int", Nullable: true}}}
	w, err := journal.Open(dir, 0)
	if err != nil {
		panic(err)
	}
	if err := w.Append(journal.Record{Sequence: 1, Kind: "relation", Relation: &r}); err != nil {
		panic(err)
	}
	seq := uint64(1)
	for rev := 1; rev <= revs; rev++ {
		seq++
		writes := make([]versions.Mutation, 0, keys)
		for i := 0; i < keys; i++ {
			band := fmt.Sprintf("band-%06d-r%02d", i, rev)
			for len(band) < textLen {
				band += "x"
			}
			row := datum.Row{"sample": datum.Integer(int64(i + 1)), "band": datum.Value{Kind: "text", Text: band[:textLen]},
				"amplitude": datum.Integer(int64((i*7 + rev*13) % 1000))}
			writes = append(writes, versions.Mutation{Relation: "spectra", Key: r.Key(row), Row: row})
		}
		if err := w.Append(journal.Record{Sequence: seq, Kind: "commit", Writes: writes}); err != nil {
			panic(err)
		}
	}
	w.Close()
}

func vacuumMain() {
	// checkpoint size boundary: same live rows, different retention
	for _, tc := range []struct{ keys, revs, text int }{{100000, 10, 64}, {100000, 10, 32}, {100000, 9, 64}} {
		dir, _ := os.MkdirTemp("", "lamina-cp-")
		buildJournal(dir, tc.keys, tc.revs, tc.text)
		d, err := engine.Open(dir)
		if err != nil {
			panic(err)
		}
		t0 := time.Now()
		_, cpErr := d.Checkpoint()
		elapsed := time.Since(t0)
		fmt.Printf("CHECKPOINT versions=%d text=%d cp_err=%v cp_ms=%.0f\n", tc.keys*tc.revs, tc.text, cpErr, float64(elapsed.Microseconds())/1000)
		d.Close()
		os.RemoveAll(dir)
	}
	// vacuum with an index: index rebuild happens synchronously under the DB write lock
	for _, tc := range []struct{ keys, revs int }{{5000, 10}, {10000, 10}, {20000, 10}} {
		dir, _ := os.MkdirTemp("", "lamina-vac-")
		buildJournal(dir, tc.keys, tc.revs, 32)
		d, err := engine.Open(dir)
		if err != nil {
			panic(err)
		}
		idx, err := d.CreateIndex(catalog.Index{Name: "by_band", Relation: "spectra", Column: "band"})
		if err != nil {
			panic(err)
		}
		head := uint64(0)
		st, _ := d.Stats()
		head = st.Sequence
		t0 := time.Now()
		res, err := d.Vacuum(head)
		elapsed := time.Since(t0)
		fmt.Printf("VACUUM_INDEX versions=%d index_created=%d removed=%d retained=%d err=%v vacuum_ms=%.0f\n", tc.keys*tc.revs, idx.Created, res.Removed, res.Retained, err, float64(elapsed.Microseconds())/1000)
		d.Close()
		os.RemoveAll(dir)
	}
}
