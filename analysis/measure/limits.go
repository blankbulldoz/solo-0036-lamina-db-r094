package main

// Image-size boundary and index-build scaling measurements used in the report.
import (
	"fmt"
	"lamina/internal/index"
	"lamina/internal/snapshot"
	"time"
)

func limits() {
	for _, cfg := range []struct{ keys, revs, text int }{{100000, 10, 64}, {100000, 10, 32}, {200000, 5, 32}, {100000, 5, 64}} {
		s, c := corpus(cfg.keys, cfg.revs, cfg.text)
		img := snapshot.Image{Format: 1, Sequence: uint64(cfg.revs), Horizon: 0, Catalog: c.Clone(), Store: s}
		t0 := time.Now()
		data, err := snapshot.Encode(img)
		dt := time.Since(t0)
		fmt.Printf("CONFIG keys=%d revs=%d text=%d versions=%d image_bytes=%d over_128MiB=%v encode_ms=%.0f err=%v\n",
			cfg.keys, cfg.revs, cfg.text, cfg.keys*cfg.revs, len(data), len(data) > snapshot.MaxImage, float64(dt.Microseconds())/1000, err)
		if err != nil {
			continue
		}
		dir, _ := mkdtemp()
		werr := snapshot.Write(dir+"/checkpoint.lmi", img)
		fmt.Printf("  snapshot.Write_ok=%v err=%v\n", werr == nil, werr)
		removeall(dir)
	}
	s, c := corpus(100000, 10, 32)
	t0 := time.Now()
	ir := index.Build(c, s)
	dt := time.Since(t0)
	fmt.Printf("INDEX_BUILD versions=%d entries=%d elapsed_ms=%.0f\n", 1000000, ir.Trees["by_band"].Size(), float64(dt.Microseconds())/1000)
}
