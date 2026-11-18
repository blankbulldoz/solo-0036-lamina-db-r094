package main

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/engine"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"runtime"
	"time"
)

func heap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
func rel() catalog.Relation {
	return catalog.Relation{Name: "spectra", Primary: "sample", Columns: []catalog.Column{{Name: "sample", Type: "int"}, {Name: "band", Type: "text"}, {Name: "amplitude", Type: "int", Nullable: true}, {Name: "valid", Type: "bool"}}, Created: 1}
}
func row(i int64) datum.Row {
	return datum.Row{"sample": datum.Integer(i), "band": datum.Value{Kind: "text", Text: fmt.Sprintf("band-%08d", i)}, "amplitude": datum.Integer(i % 1000), "valid": datum.Value{Kind: "bool", Bool: i%2 == 0}}
}
func mkdir() string {
	d, err := os.MkdirTemp("", "lamina-cost-")
	if err != nil {
		panic(err)
	}
	return d
}

// installCheckpoint writes an image with n current rows and an empty journal at base n.
func installCheckpoint(dir string, n int, idx bool) {
	r := rel()
	c := catalog.New()
	c.Relations["spectra"] = r
	if idx {
		c.Indexes["by_band"] = catalog.Index{Name: "by_band", Relation: "spectra", Column: "band", Unique: true, Created: 1}
	}
	s := versions.New()
	s.Ensure("spectra")
	for i := 0; i < n; i++ {
		rw := row(int64(i + 1))
		s.Tables["spectra"][r.Key(rw)] = []versions.Version{{Sequence: 2, Row: rw}}
	}
	img := snapshot.Image{Format: 1, Sequence: 2, Horizon: 0, Catalog: c, Store: s}
	if err := snapshot.Write(dir+"/checkpoint.lmi", img); err != nil {
		panic(err)
	}
	w, err := journal.Open(dir, 2)
	if err != nil {
		panic(err)
	}
	w.Close()
}

func putCost(n int, idx bool) {
	dir := mkdir()
	defer os.RemoveAll(dir)
	installCheckpoint(dir, n, idx)
	d, err := engine.Open(dir)
	if err != nil {
		panic(err)
	}
	defer d.Close()
	// one warm-up
	tx, _ := d.Begin(false)
	_ = d.Put(tx.ID, "spectra", row(int64(n+100)), false)
	_, err = d.Commit(tx.ID)
	if err != nil {
		panic(err)
	}
	t0 := time.Now()
	tx, _ = d.Begin(false)
	_ = d.Put(tx.ID, "spectra", row(int64(n+101)), false)
	t1 := time.Now()
	_, err = d.Commit(tx.ID)
	if err != nil {
		panic(err)
	}
	t2 := time.Now()
	fmt.Printf("PUTCOST table_keys=%d unique_index=%v put_ms=%.2f commit_ms=%.2f rows_scanned_per_put~%d\n", n, idx, float64(t1.Sub(t0).Microseconds())/1000, float64(t2.Sub(t1).Microseconds())/1000, n)
}

func makeJournal(dir string, from, to int, per int) {
	w, err := journal.Open(dir, 0)
	if err != nil {
		panic(err)
	}
	seq := uint64(0)
	for start := from; start < to; start += per {
		seq++
		writes := make([]versions.Mutation, 0, per)
		for i := start; i < start+per && i < to; i++ {
			rw := row(int64(i%100000 + 1))
			k := rel().Key(rw)
			writes = append(writes, versions.Mutation{Relation: "spectra", Key: k, Row: rw})
		}
		if err := w.Append(journal.Record{Sequence: seq, Kind: "commit", Writes: writes}); err != nil {
			panic(err)
		}
	}
	w.Close()
}

func restartCost(label string, versionsN int, checkpointAt int, idx bool) {
	dir := mkdir()
	defer os.RemoveAll(dir)
	// relation record first: create journal with a relation record then commits
	// simplest: write checkpoint with catalog but no rows at seq=0? Use image at seq=0 with catalog requires Created<=seq => impossible.
	// Instead, write a journal containing a relation record at seq 1 then commits, and optional checkpoint at checkpointAt.
	r := rel()
	// build journal from scratch
	w, err := journal.Open(dir, 0)
	if err != nil {
		panic(err)
	}
	rr := r
	rr.Created = 1
	if err := w.Append(journal.Record{Sequence: 1, Kind: "relation", Relation: &rr}); err != nil {
		panic(err)
	}
	seq := uint64(1)
	per := 1000
	for start := 0; start < versionsN; start += per {
		seq++
		writes := make([]versions.Mutation, 0, per)
		for i := start; i < start+per && i < versionsN; i++ {
			rw := row(int64(i + 1))
			k := rr.Key(rw)
			writes = append(writes, versions.Mutation{Relation: "spectra", Key: k, Row: rw})
		}
		if err := w.Append(journal.Record{Sequence: seq, Kind: "commit", Writes: writes}); err != nil {
			panic(err)
		}
	}
	w.Close()
	if checkpointAt > 0 {
		// Build the same state by opening, replaying, then checkpoint via engine.
		d, err := engine.Open(dir)
		if err != nil {
			panic(err)
		}
		// can't checkpoint at arbitrary seq; checkpoint only at head. So for suffix test, write checkpoint at head then add more records.
		_ = checkpointAt
		if _, err = d.Checkpoint(); err != nil {
			panic(err)
		}
		d.Close()
	}
	base := heap()
	t0 := time.Now()
	d, err := engine.Open(dir)
	dt := time.Since(t0)
	if err != nil {
		panic(err)
	}
	top := heap()
	st, _ := d.Stats()
	fmt.Printf("RESTART %s versions=%d checkpoint=%v open_ms=%.1f heap_mb=%.1f journal_bytes=%d journal_records_in_mem=%d index_entries=%v\n", label, versionsN, checkpointAt > 0, float64(dt.Microseconds())/1000, float64(top-base)/(1<<20), fileSize(dir+"/journal.lmw"), st.JournalRecords, st.IndexEntries)
	d.Close()
}

func fileSize(p string) int64 {
	i, err := os.Stat(p)
	if err != nil {
		return -1
	}
	return i.Size()
}

func restartSuffix(versionsN, checkpointAfter int) {
	dir := mkdir()
	defer os.RemoveAll(dir)
	rr := rel()
	rr.Created = 1
	w, _ := journal.Open(dir, 0)
	if err := w.Append(journal.Record{Sequence: 1, Kind: "relation", Relation: &rr}); err != nil {
		panic(err)
	}
	seq := uint64(1)
	per := 1000
	for start := 0; start < versionsN; start += per {
		seq++
		writes := make([]versions.Mutation, 0, per)
		for i := start; i < start+per && i < versionsN; i++ {
			rw := row(int64(i + 1))
			writes = append(writes, versions.Mutation{Relation: "spectra", Key: rr.Key(rw), Row: rw})
		}
		if err := w.Append(journal.Record{Sequence: seq, Kind: "commit", Writes: writes}); err != nil {
			panic(err)
		}
	}
	w.Close()
	// checkpoint at head after half the versions is simulated by checkpointing now, then appending more records directly
	d, err := engine.Open(dir)
	if err != nil {
		panic(err)
	}
	if _, err = d.Checkpoint(); err != nil {
		panic(err)
	}
	d.Close()
	w2, _ := journal.Open(dir, seq)
	start := versionsN
	end := checkpointAfter
	for start < end {
		seq++
		writes := make([]versions.Mutation, 0, per)
		for i := start; i < start+per && i < end; i++ {
			rw := row(int64(i + 1))
			writes = append(writes, versions.Mutation{Relation: "spectra", Key: rr.Key(rw), Row: rw})
		}
		if err := w2.Append(journal.Record{Sequence: seq, Kind: "commit", Writes: writes}); err != nil {
			panic(err)
		}
		start += per
	}
	w2.Close()
	base := heap()
	t0 := time.Now()
	dd, err := engine.Open(dir)
	dt := time.Since(t0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("RESTART checkpoint_at=%d then_commits_to=%d open_ms=%.1f heap_mb=%.1f journal_bytes=%d records_in_mem=%d\n", versionsN, end, float64(dt.Microseconds())/1000, float64(heap()-base)/(1<<20), fileSize(dir+"/journal.lmw"), func() int { s, _ := dd.Stats(); return s.JournalRecords }())
	dd.Close()
}

func main() {
	if os.Getenv("MEM") == "1" {
		memMain()
		return
	}
	if os.Getenv("VAC") == "1" {
		vacuumMain()
		return
	}
	for _, n := range []int{1000, 10000, 100000} {
		putCost(n, false)
	}
	for _, n := range []int{1000, 10000, 100000} {
		putCost(n, true)
	}
	restartCost("journal-only", 0, 0, false)
	for _, n := range []int{10000, 100000} {
		restartCost("journal-only", n, 0, false)
	}
	for _, n := range []int{10000, 100000} {
		restartCost("checkpoint-at-head", n, 1, false)
	}
	restartSuffix(10000, 20000)
	restartSuffix(50000, 100000)
}
