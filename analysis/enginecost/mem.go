package main

// Additional in-process measurement of the real engine at scale: heap per live
// version and per historical version, plus index replication.
import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/index"
	"lamina/internal/versions"
	"runtime"
)

func memMain() {
	for _, revs := range []int{1, 4, 10} {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		r := rel()
		s := versions.New()
		s.Ensure("spectra")
		for rev := 1; rev <= revs; rev++ {
			for i := 0; i < 100000; i++ {
				rw := datum.Row{"sample": datum.Integer(int64(i)), "band": datum.Value{Kind: "text", Text: fmt.Sprintf("band-%06d-r%02d", i, rev)}, "amplitude": datum.Integer(int64(i)), "valid": datum.Value{Kind: "bool", Bool: true}}
				s.Tables["spectra"][r.Key(rw)] = append(s.Tables["spectra"][r.Key(rw)], versions.Version{Sequence: uint64(rev + 1), Row: rw})
			}
		}
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		c := catalog.New()
		c.Relations["spectra"] = r
		c.Indexes["by_amplitude"] = catalog.Index{Name: "by_amplitude", Relation: "spectra", Column: "amplitude", Created: 2}
		t0 := indexBuildStart()
		ir := index.Build(c, s)
		elapsed := indexBuildElapsed(t0)
		var withIndex runtime.MemStats
		runtime.ReadMemStats(&withIndex)
		fmt.Printf("ENGMEM revs=%d versions=%d store_heap_delta_mb=%.1f bytes_per_version_real=%.0f index_entries=%d index_heap_delta_mb=%.1f index_build_ms=%.0f\n",
			revs, revs*100000, float64(after.HeapAlloc-before.HeapAlloc)/(1<<20), float64(after.HeapAlloc-before.HeapAlloc)/float64(revs*100000), ir.Trees["by_amplitude"].Size(), float64(withIndex.HeapAlloc-after.HeapAlloc)/(1<<20), elapsed)
		runtime.KeepAlive(ir)
	}
}
