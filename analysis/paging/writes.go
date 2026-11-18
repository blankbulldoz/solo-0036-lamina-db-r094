package main

// Write-path and retention-pressure comparison for the same four layouts.

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"time"
)

type writePlan struct {
	Commits   int
	PerCommit int
	NewText   int
}

func runwrite(keys, revs, hot, text, pageKiB, budget int) {
	recs := genCorpus(keys, revs, hot, text)
	maxSeq := uint32(0)
	for _, r := range recs {
		if r.Seq > maxSeq {
			maxSeq = r.Seq
		}
	}
	plan := writePlan{Commits: 200, PerCommit: 200, NewText: text}
	fmt.Printf("WRITECORPUS keys=%d versions=%d max_seq=%d text=%d commits=%d per_commit=%d\n",
		keys, len(recs), maxSeq, text, plan.Commits, plan.PerCommit)

	// payload bytes that a WAL-style append-only representation must record
	var newBytes int64
	for c := 0; c < plan.Commits; c++ {
		for j := 0; j < plan.PerCommit; j++ {
			key := (c*plan.PerCommit + j*7919) % keys
			r := Rec{Key: fmt.Sprintf("int:%d", key), Seq: maxSeq + 1 + uint32(c),
				Band: bandText(key, 100+int(rune(c))+j, plan.NewText), Amp: int64(c + j), HasAmp: true, Valid: true}
			newBytes += int64(recSize(r))
		}
	}
	fmt.Printf("WRITEPAYLOAD logical_new_bytes=%d bytes_per_commit=%.0f\n", newBytes, float64(newBytes)/float64(plan.Commits))

	dir, _ := os.MkdirTemp("", "lamina-write-")
	defer os.RemoveAll(dir)

	// baseline: append in memory; measure heap growth and time
	{
		b := newBaseline(recs)
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		t0 := time.Now()
		for c := 0; c < plan.Commits; c++ {
			for j := 0; j < plan.PerCommit; j++ {
				key := (c*plan.PerCommit + j*7919) % keys
				k := fmt.Sprintf("int:%d", key)
				r := Rec{Key: k, Seq: maxSeq + 1 + uint32(c), Band: bandText(key, 100+c+j, plan.NewText), Amp: int64(c + j), HasAmp: true, Valid: true}
				b.dir[k] = append(b.dir[k], struct {
					seq uint32
					rec Rec
				}{r.Seq, r})
			}
		}
		dt := time.Since(t0)
		runtime.ReadMemStats(&m1)
		fmt.Printf("WRITE layout=all-resident elapsed_ms=%.0f heap_growth_mb=%.1f bytes_written_est=0 (in-memory append)\n",
			float64(dt.Microseconds())/1000, float64(m1.HeapAlloc-m0.HeapAlloc)/(1<<20))
	}

	// locator + append history: append-only, one pread per new version not needed
	{
		h, err := newHistory(dir+"/hist-w.bin", recs, budget<<20)
		if err != nil {
			panic(err)
		}
		t0 := time.Now()
		var written int64
		for c := 0; c < plan.Commits; c++ {
			for j := 0; j < plan.PerCommit; j++ {
				key := (c*plan.PerCommit + j*7919) % keys
				k := fmt.Sprintf("int:%d", key)
				r := Rec{Key: k, Seq: maxSeq + 1 + uint32(c), Band: bandText(key, 100+c+j, plan.NewText), Amp: int64(c + j), HasAmp: true, Valid: true}
				buf := encRec(nil, r)
				off := h.size
				if _, err := h.f.Write(buf); err != nil {
					panic(err)
				}
				h.dir[k] = append(h.dir[k], loc{Seq: r.Seq, Off: uint64(off), Len: uint16(len(buf)), Flags: 2 | 4})
				h.size += int64(len(buf))
				h.latest[k] = r
				written += int64(len(buf))
			}
		}
		if err := h.f.Sync(); err != nil {
			panic(err)
		}
		dt := time.Since(t0)
		fmt.Printf("WRITE layout=locator+history elapsed_ms=%.0f bytes_written=%d amplification=%.2f locator_versions=%d\n",
			float64(dt.Microseconds())/1000, written, float64(written)/float64(newBytes), len(recs)+plan.Commits*plan.PerCommit)
		h.f.Close()
	}

	// paged, strategy A: rewrite the affected page for each version (random keys)
	{
		pg, err := newPaged(dir+"/pages-w.bin", recs, pageKiB<<10, budget<<20)
		if err != nil {
			panic(err)
		}
		// simple mutable page store: load, insert, rewrite at end of file, update meta
		t0 := time.Now()
		var written int64
		rewrites := 0
		for c := 0; c < plan.Commits; c++ {
			for j := 0; j < plan.PerCommit; j++ {
				key := (c*plan.PerCommit + j*7919) % keys
				k := fmt.Sprintf("int:%d", key)
				r := Rec{Key: k, Seq: maxSeq + 1 + uint32(c), Band: bandText(key, 100+c+j, plan.NewText), Amp: int64(c + j), HasAmp: true, Valid: true}
				pi := pg.pageForKey(k)
				if pi < 0 {
					continue
				}
				recsOnPage, _ := pg.loadPage(pi)
				out := make([]Rec, 0, len(recsOnPage)+1)
				inserted := false
				for _, x := range recsOnPage {
					if !inserted && (x.Key > r.Key || (x.Key == r.Key && x.Seq > r.Seq)) {
						out = append(out, r)
						inserted = true
					}
					out = append(out, x)
				}
				if !inserted {
					out = append(out, r)
				}
				buf := make([]byte, 0, len(out)*recSize(r))
				for _, x := range out {
					buf = encRec(buf, x)
				}
				off, _ := pg.f.Seek(0, 2)
				if _, err := pg.f.Write(buf); err != nil {
					panic(err)
				}
				pg.metas[pi].Off = off
				pg.metas[pi].Size = int32(len(buf))
				pg.cache.put(fmt.Sprint(pi), out, len(buf))
				written += int64(len(buf))
				rewrites++
			}
		}
		if err := pg.f.Sync(); err != nil {
			panic(err)
		}
		dt := time.Since(t0)
		fmt.Printf("WRITE layout=pages-inplace elapsed_ms=%.0f bytes_written=%d amplification=%.2f page_rewrites=%d reread_pages=%d\n",
			float64(dt.Microseconds())/1000, written, float64(written)/float64(newBytes), rewrites, pg.preads)
		pg.f.Close()
	}

	// paged, strategy B: append-only delta chain, pages untouched
	{
		pg, err := newPaged(dir+"/pages-d.bin", recs, pageKiB<<10, budget<<20)
		if err != nil {
			panic(err)
		}
		type dl struct {
			Seq uint32
			Off uint64
			Len uint16
		}
		delta := map[string][]dl{}
		var deltaFile *os.File
		deltaFile, err = os.Create(dir + "/delta.bin")
		if err != nil {
			panic(err)
		}
		t0 := time.Now()
		var written int64
		for c := 0; c < plan.Commits; c++ {
			for j := 0; j < plan.PerCommit; j++ {
				key := (c*plan.PerCommit + j*7919) % keys
				k := fmt.Sprintf("int:%d", key)
				r := Rec{Key: k, Seq: maxSeq + 1 + uint32(c), Band: bandText(key, 100+c+j, plan.NewText), Amp: int64(c + j), HasAmp: true, Valid: true}
				buf := encRec(nil, r)
				off, _ := deltaFile.Seek(0, 2)
				if _, err := deltaFile.Write(buf); err != nil {
					panic(err)
				}
				delta[k] = append(delta[k], dl{r.Seq, uint64(off), uint16(len(buf))})
				written += int64(len(buf))
			}
		}
		if err := deltaFile.Sync(); err != nil {
			panic(err)
		}
		dt := time.Since(t0)
		fmt.Printf("WRITE layout=pages-delta elapsed_ms=%.0f bytes_written=%d amplification=%.2f delta_versions=%d delta_index_bytes=%d\n",
			float64(dt.Microseconds())/1000, written, float64(written)/float64(newBytes), plan.Commits*plan.PerCommit, len(delta)*(48+8))
		deltaFile.Close()
		pg.f.Close()
	}

	// retention pressure: writes churn while one long reader pins an old snapshot.
	// Compare retained history bytes (logical) at various pin positions.
	fmt.Printf("RETENTION\n")
	for _, pinPct := range []int{0, 50, 90, 100} {
		pin := uint32(float64(maxSeq) * float64(pinPct) / 100)
		retained, bytes := retainedAt(recs, pin)
		live := 0
		var liveBytes int64
		for _, r := range recs {
			if r.Seq == maxSeq {
				live++
				liveBytes += int64(recSize(r))
			}
		}
		fmt.Printf("  pin_pct=%d pin=%d retained_versions=%d retained_bytes=%d history_only_bytes=%d live_rows=%d live_bytes=%d\n",
			pinPct, pin, retained, bytes, bytes-liveBytes, live, liveBytes)
	}
	_ = sort.Strings
}
