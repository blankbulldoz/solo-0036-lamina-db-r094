package main

import (
	"encoding/json"
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/index"
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

func schema() catalog.Relation {
	return catalog.Relation{Name: "spectra", Primary: "sample", Columns: []catalog.Column{
		{Name: "sample", Type: "int"}, {Name: "band", Type: "text"},
		{Name: "amplitude", Type: "int", Nullable: true}, {Name: "valid", Type: "bool"},
	}, Created: 1}
}
func mkrow(i, rev, textLen int, changing bool) datum.Row {
	band := fmt.Sprintf("band-%06d", i)
	if changing {
		band = fmt.Sprintf("band-%06d-r%03d", i, rev)
	}
	for len(band) < textLen {
		band += "x"
	}
	return datum.Row{"sample": datum.Integer(int64(i)), "band": datum.Value{Kind: "text", Text: band},
		"amplitude": datum.Integer(int64((i*7 + rev*13) % 1000)), "valid": datum.Value{Kind: "bool", Bool: (i+rev)%2 == 0}}
}

func build(keys, revs, textLen int, changing bool) (*versions.Store, *catalog.Catalog, uint64) {
	rel := schema()
	s := versions.New()
	s.Ensure("spectra")
	seq := uint64(0)
	for r := 0; r < revs; r++ {
		seq++
		for i := 0; i < keys; i++ {
			k := rel.Key(mkrow(i, r, textLen, changing))
			s.Tables["spectra"][k] = append(s.Tables["spectra"][k], versions.Version{Sequence: seq, Row: datum.Clone(mkrow(i, r, textLen, changing))})
		}
	}
	cat := catalog.New()
	cat.Relations["spectra"] = rel
	cat.Indexes["by_band"] = catalog.Index{Name: "by_band", Relation: "spectra", Column: "band", Created: 1}
	return s, cat, seq
}

func main() {
	if os.Getenv("LIMITS") == "1" {
		limits()
		return
	}
	keys, revs := 100000, 10
	var t0 time.Time
	var dt time.Duration
	var enc []byte
	for _, tc := range []struct {
		name     string
		textLen  int
		changing bool
	}{{"static-text64", 64, false}, {"changing-text32", 32, true}} {
		base := heap()
		s, cat, seq := build(keys, revs, tc.textLen, tc.changing)
		after := heap()
		ib := heap()
		ir := index.Build(cat, s)
		ia := heap()
		img := snapshot.Image{Format: 1, Sequence: seq, Horizon: 0, Catalog: cat.Clone(), Store: s}
		t0 = time.Now()
		var err error
		enc, err = snapshot.Encode(img)
		dt = time.Since(t0)
		if err != nil {
			panic(err)
		}
		// live-only image
		lv := versions.New()
		lv.Ensure("spectra")
		for i := 0; i < keys; i++ {
			k := cat.Relations["spectra"].Key(mkrow(i, revs-1, tc.textLen, tc.changing))
			lv.Tables["spectra"][k] = []versions.Version{{Sequence: seq, Row: datum.Clone(mkrow(i, revs-1, tc.textLen, tc.changing))}}
		}
		live, _ := snapshot.Encode(snapshot.Image{Format: 1, Sequence: seq, Horizon: 0, Catalog: cat.Clone(), Store: lv})
		fmt.Printf("%s keys=%d revs=%d versions=%d store_heap=%d B/ver=%.1f index_entries=%d index_heap=%d B/entry=%.1f image_all=%d image_live=%d ratio=%.2f encode_ms=%.0f\n",
			tc.name, keys, revs, keys*revs, after-base, float64(after-base)/float64(keys*revs), ir.Trees["by_band"].Size(), ia-ib, float64(ia-ib)/float64(max(1, ir.Trees["by_band"].Size())), len(enc), len(live), float64(len(enc))/float64(len(live)), float64(dt.Microseconds())/1000)
		_ = ib
	}

	// WAL in-memory duplicate + on-disk bytes, and replay load behavior
	dir, _ := os.MkdirTemp("", "lamina-wal-")
	defer os.RemoveAll(dir)
	w, err := journal.Open(dir, 0)
	if err != nil {
		panic(err)
	}
	base := heap()
	const wkeys, wrevs = 20000, 5
	for r := 0; r < wrevs; r++ {
		writes := make([]versions.Mutation, 0, wkeys)
		for i := 0; i < wkeys; i++ {
			row := mkrow(i, r, 64, true)
			k := schema().Key(row)
			writes = append(writes, versions.Mutation{Relation: "spectra", Key: k, Row: row})
		}
		if err := w.Append(journal.Record{Sequence: uint64(r + 1), Kind: "commit", Writes: writes}); err != nil {
			panic(err)
		}
	}
	walHeap := heap() - base
	info, _ := os.Stat(dir + "/journal.lmw")
	fmt.Printf("WAL keys=%d revs=%d versions=%d records_in_memory_heap=%d heap_per_version=%.1f file_bytes=%d bytes_per_version=%.1f\n", wkeys, wrevs, wkeys*wrevs, walHeap, float64(walHeap)/float64(wkeys*wrevs), info.Size(), float64(info.Size())/float64(wkeys*wrevs))
	w.Close()

	// query.Scan allocation/time on large table (single narrow point)
	s, cat, seq := build(keys, 1, 64, false)
	t0 = time.Now()
	out := s.Scan("spectra", seq)
	dt = time.Since(t0)
	t1 := time.Now()
	v, ok := s.Read("spectra", cat.Relations["spectra"].Key(mkrow(42, 0, 64, false)), seq)
	dt2 := time.Since(t1)
	fmt.Printf("SCAN keys=%d rows=%d elapsed_ms=%.1f read_one_ok=%v read_one_ms=%.4f\n", keys, len(out), float64(dt.Microseconds())/1000, ok, float64(dt2.Microseconds())/1000)
	_ = v

	// vacuum timing on all-history
	s2, _, _ := build(keys, revs, 32, true)
	t0 = time.Now()
	res := s2.Vacuum(uint64(keys))
	dt = time.Since(t0)
	fmt.Printf("VACUUM versions=%d removed=%d retained=%d elapsed_ms=%.1f\n", keys*revs, res.Removed, res.Retained, float64(dt.Microseconds())/1000)

	// decode/encode checkpoint peak-ish time for 1M versions already above
	json.Unmarshal(enc, &struct{}{})
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
