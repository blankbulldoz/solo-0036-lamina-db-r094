package main

// Bounded prototype: compares all-resident version chains (the current engine),
// a locator+history-file layout with a row cache, and a key-ordered paged layout
// with a byte-budgeted page cache. Same operation trace, same visibility rule.

import (
	"bufio"
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"runtime"
	"sort"
	"time"
)

// ---------- record model ----------
type Rec struct {
	Key    string
	Seq    uint32
	Del    bool
	Band   string
	Amp    int64
	HasAmp bool
	Valid  bool
}

func putUvarint(b []byte, x uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], x)
	return append(b, tmp[:n]...)
}
func zigzag(x int64) uint64   { return uint64(x<<1) ^ uint64(x>>63) }
func unzigzag(x uint64) int64 { return int64(x>>1) ^ -int64(x&1) }

func encRec(b []byte, r Rec) []byte {
	b = putUvarint(b, uint64(len(r.Key)))
	b = append(b, r.Key...)
	b = putUvarint(b, uint64(r.Seq))
	var fl byte
	if r.Del {
		fl |= 1
	}
	if r.HasAmp {
		fl |= 2
	}
	if r.Valid {
		fl |= 4
	}
	b = append(b, fl)
	b = putUvarint(b, uint64(len(r.Band)))
	b = append(b, r.Band...)
	if r.HasAmp {
		b = putUvarint(b, zigzag(r.Amp))
	}
	return b
}
func decRec(b []byte) (Rec, int, bool) {
	var r Rec
	start := len(b)
	k, n := binary.Uvarint(b)
	if n <= 0 {
		return r, 0, false
	}
	if int(k) > len(b)-n {
		return r, 0, false
	}
	r.Key = string(b[n : n+int(k)])
	b = b[n+int(k):]
	sv, n := binary.Uvarint(b)
	if n <= 0 {
		return r, 0, false
	}
	r.Seq = uint32(sv)
	b = b[n:]
	if len(b) < 1 {
		return r, 0, false
	}
	fl := b[0]
	b = b[1:]
	r.Del = fl&1 != 0
	r.HasAmp = fl&2 != 0
	r.Valid = fl&4 != 0
	l, n := binary.Uvarint(b)
	if n <= 0 {
		return r, 0, false
	}
	if int(l) > len(b)-n {
		return r, 0, false
	}
	r.Band = string(b[n : n+int(l)])
	b = b[n+int(l):]
	if r.HasAmp {
		a, n := binary.Uvarint(b)
		if n <= 0 {
			return r, 0, false
		}
		r.Amp = unzigzag(a)
		b = b[n:]
	}
	return r, start - len(b), true
}
func recSize(r Rec) int { return len(encRec(nil, r)) }

// ---------- corpus ----------
func bandText(i, rev, n int) string {
	b := make([]byte, 0, n)
	b = append(b, fmt.Sprintf("band-%06d-r%02d", i, rev)...)
	for len(b) < n {
		b = append(b, 'x')
	}
	return string(b[:n])
}
func genCorpus(keys, revs, hotExtra, textLen int) []Rec {
	out := make([]Rec, 0, keys*(revs+hotExtra/10+2))
	seq := uint32(0)
	for r := 1; r <= revs; r++ {
		seq++
		for i := 0; i < keys; i++ {
			out = append(out, Rec{Key: fmt.Sprintf("int:%d", i), Seq: seq, Band: bandText(i, r, textLen),
				Amp: int64((i*7 + r*13) % 1000), HasAmp: true, Valid: (i+r)%2 == 0})
		}
	}
	for e := 0; e < hotExtra; e++ {
		seq++
		for i := 0; i < keys/10; i++ {
			out = append(out, Rec{Key: fmt.Sprintf("int:%d", i), Seq: seq, Band: bandText(i, revs+e+1, textLen),
				Amp: int64((i*11 + e) % 1000), HasAmp: true, Valid: true})
		}
	}
	return out
}

// ---------- LRU ----------
type lru struct {
	budget                  int
	used                    int
	ll                      *list.List
	m                       map[string]*list.Element
	hits, misses, evictions int
	readBytes               int64
}
type lruEnt struct {
	k  string
	v  any
	sz int
}

func newLRU(budget int) *lru {
	return &lru{budget: budget, ll: list.New(), m: map[string]*list.Element{}}
}
func (c *lru) get(k string) (any, bool) {
	e, ok := c.m[k]
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
	c.ll.MoveToFront(e)
	return e.Value.(*lruEnt).v, true
}
func (c *lru) put(k string, v any, sz int) {
	if c.budget <= 0 {
		return
	}
	if e, ok := c.m[k]; ok {
		c.ll.MoveToFront(e)
		return
	}
	for c.used+sz > c.budget && c.ll.Len() > 0 {
		back := c.ll.Back()
		ent := back.Value.(*lruEnt)
		c.used -= ent.sz
		delete(c.m, ent.k)
		c.ll.Remove(back)
		c.evictions++
	}
	e := c.ll.PushFront(&lruEnt{k, v, sz})
	c.m[k] = e
	c.used += sz
}

// ---------- baseline: all version rows resident (real engine type) ----------
type baseline struct {
	dir map[string][]struct {
		seq uint32
		rec Rec
	}
	keys []string
}

func newBaseline(recs []Rec) *baseline {
	b := &baseline{dir: map[string][]struct {
		seq uint32
		rec Rec
	}{}}
	for _, r := range recs {
		b.dir[r.Key] = append(b.dir[r.Key], struct {
			seq uint32
			rec Rec
		}{r.Seq, r})
	}
	for k := range b.dir {
		b.keys = append(b.keys, k)
	}
	sort.Strings(b.keys)
	return b
}
func (b *baseline) read(key string, at uint32) (Rec, bool) {
	c := b.dir[key]
	i := sort.Search(len(c), func(i int) bool { return c[i].seq > at }) - 1
	if i < 0 {
		return Rec{}, false
	}
	r := c[i].rec
	if r.Del {
		return Rec{}, false
	}
	return r, true
}
func (b *baseline) scan(at uint32) []Rec {
	out := make([]Rec, 0, len(b.keys))
	for _, k := range b.keys {
		if r, ok := b.read(k, at); ok {
			out = append(out, r)
		}
	}
	return out
}

// ---------- H: append history file + locator directory + row cache ----------
type loc struct {
	Seq   uint32
	Off   uint64
	Len   uint16
	Flags uint8
}
type history struct {
	f                  *os.File
	dir                map[string][]loc
	keys               []string
	cache              *lru
	preads, preadBytes int64
	size               int64
	latest             map[string]Rec
}

func newHistory(path string, recs []Rec, budget int) (*history, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	h := &history{f: f, dir: map[string][]loc{}, cache: newLRU(budget), latest: map[string]Rec{}}
	buf := make([]byte, 0, 256)
	for _, r := range recs {
		buf = encRec(buf[:0], r)
		off := h.size
		if _, err := f.Write(buf); err != nil {
			return nil, err
		}
		var fl byte
		if r.Del {
			fl |= 1
		}
		if r.HasAmp {
			fl |= 2
		}
		if r.Valid {
			fl |= 4
		}
		h.dir[r.Key] = append(h.dir[r.Key], loc{Seq: r.Seq, Off: uint64(off), Len: uint16(len(buf)), Flags: fl})
		h.size += int64(len(buf))
		if r.Seq >= h.latest[r.Key].Seq {
			h.latest[r.Key] = r
		}
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	for k := range h.dir {
		h.keys = append(h.keys, k)
	}
	sort.Strings(h.keys)
	return h, nil
}
func (h *history) locatorBytes() int64 {
	var n int64
	for _, locs := range h.dir {
		n += int64(len(locs)) * 24 // Seq+Off+Len+Flags padding
	}
	n += int64(len(h.dir)) * 48 // key string header + map overhead estimate
	for k := range h.dir {
		n += int64(len(k))
	}
	return n
}
func (h *history) read(key string, at uint32) (Rec, bool) {
	c := h.dir[key]
	i := sort.Search(len(c), func(i int) bool { return c[i].Seq > at }) - 1
	if i < 0 {
		return Rec{}, false
	}
	l := c[i]
	if l.Flags&1 != 0 {
		return Rec{}, false
	}
	if r, ok := h.latest[key]; ok && r.Seq == l.Seq {
		return r, true
	}
	ck := key + "\x00" + fmt.Sprint(l.Seq)
	if v, ok := h.cache.get(ck); ok {
		return v.(Rec), true
	}
	buf := make([]byte, l.Len)
	n, _ := h.f.ReadAt(buf, int64(l.Off))
	h.preads++
	h.preadBytes += int64(n)
	if n != int(l.Len) {
		return Rec{}, false
	}
	r, _, ok := decRec(buf)
	if !ok {
		return Rec{}, false
	}
	h.cache.put(ck, r, int(l.Len)+32)
	return r, true
}

// readResident serves any read that the resident current row can answer and
// only touches the history file when the requested position predates it.
func (h *history) readResident(key string, at uint32) (Rec, bool) {
	if r, ok := h.latest[key]; ok && r.Seq <= at {
		if r.Del {
			return Rec{}, false
		}
		return r, true
	}
	return h.read(key, at)
}

func (h *history) scanSequential(at uint32) []Rec {
	best := make(map[string]Rec, len(h.keys))
	f, err := os.Open(h.f.Name())
	if err != nil {
		return nil
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	for {
		rec, err := readRec(br)
		if err != nil {
			break
		}
		if rec.Seq <= at && (best[rec.Key].Seq == 0 || rec.Seq >= best[rec.Key].Seq) {
			best[rec.Key] = rec
		}
	}
	h.preads++
	h.preadBytes += h.size
	out := make([]Rec, 0, len(best))
	for _, r := range best {
		if !r.Del {
			out = append(out, r)
		}
	}
	sortRecs(out)
	return out
}

func sortRecs(recs []Rec) {
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].Key != recs[j].Key {
			return recs[i].Key < recs[j].Key
		}
		return recs[i].Seq < recs[j].Seq
	})
}

func readRec(br *bufio.Reader) (Rec, error) {
	var r Rec
	k, err := binary.ReadUvarint(br)
	if err != nil {
		return r, err
	}
	kb := make([]byte, k)
	if _, err = io.ReadFull(br, kb); err != nil {
		return r, err
	}
	r.Key = string(kb)
	sv, err := binary.ReadUvarint(br)
	if err != nil {
		return r, err
	}
	r.Seq = uint32(sv)
	fl, err := br.ReadByte()
	if err != nil {
		return r, err
	}
	r.Del = fl&1 != 0
	r.HasAmp = fl&2 != 0
	r.Valid = fl&4 != 0
	l, err := binary.ReadUvarint(br)
	if err != nil {
		return r, err
	}
	bb := make([]byte, l)
	if _, err = io.ReadFull(br, bb); err != nil {
		return r, err
	}
	r.Band = string(bb)
	if r.HasAmp {
		a, err := binary.ReadUvarint(br)
		if err != nil {
			return r, err
		}
		r.Amp = unzigzag(a)
	}
	return r, nil
}

// ---------- P: key-ordered immutable pages + sparse index + page cache ----------
type pageMeta struct {
	Off      int64
	Size     int32
	FirstKey string
}
type paged struct {
	f                  *os.File
	metas              []pageMeta
	keys               []string
	cache              *lru
	preads, preadBytes int64
	oversized          int
}

func newPaged(path string, recs []Rec, pageSize, budget int) (*paged, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	sorted := append([]Rec(nil), recs...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Key != sorted[j].Key {
			return sorted[i].Key < sorted[j].Key
		}
		return sorted[i].Seq < sorted[j].Seq
	})
	p := &paged{f: f, cache: newLRU(budget)}
	var off int64
	block := make([]byte, 0, 256)
	page := make([]byte, 0, pageSize)
	first := ""
	flushPage := func() error {
		if len(page) == 0 {
			return nil
		}
		if _, err := f.Write(page); err != nil {
			return err
		}
		p.metas = append(p.metas, pageMeta{Off: off, Size: int32(len(page)), FirstKey: first})
		off += int64(len(page))
		page = page[:0]
		first = ""
		return nil
	}
	i := 0
	for i < len(sorted) {
		j := i
		block = block[:0]
		for j < len(sorted) && sorted[j].Key == sorted[i].Key {
			block = encRec(block, sorted[j])
			j++
		}
		if len(block) > pageSize {
			// A single key's chain does not fit; it owns a page and is never split.
			if err := flushPage(); err != nil {
				return nil, err
			}
			page = append(page, block...)
			first = sorted[i].Key
			p.oversized++
			if err := flushPage(); err != nil {
				return nil, err
			}
			i = j
			continue
		}
		if len(page) > 0 && len(page)+len(block) > pageSize {
			if err := flushPage(); err != nil {
				return nil, err
			}
		}
		if first == "" {
			first = sorted[i].Key
		}
		page = append(page, block...)
		i = j
	}
	if err := flushPage(); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	for i := 0; i < len(sorted); {
		p.keys = append(p.keys, sorted[i].Key)
		k := sorted[i].Key
		for i < len(sorted) && sorted[i].Key == k {
			i++
		}
	}
	sort.Strings(p.keys)
	return p, nil
}
func (p *paged) loadPage(i int) ([]Rec, error) {
	if v, ok := p.cache.get(fmt.Sprint(i)); ok {
		return v.([]Rec), nil
	}
	m := p.metas[i]
	buf := make([]byte, m.Size)
	n, _ := p.f.ReadAt(buf, m.Off)
	p.preads++
	p.preadBytes += int64(n)
	recs := make([]Rec, 0, 16)
	b := buf[:n]
	for len(b) > 0 {
		r, used, ok := decRec(b)
		if !ok || used == 0 {
			break
		}
		recs = append(recs, r)
		b = b[used:]
	}
	p.cache.put(fmt.Sprint(i), recs, int(m.Size))
	return recs, nil
}
func (p *paged) pageForKey(key string) int {
	i := sort.Search(len(p.metas), func(i int) bool { return p.metas[i].FirstKey > key }) - 1
	return i
}
func (p *paged) debugKey(key string) {
	i := p.pageForKey(key)
	fmt.Printf("DEBUG key=%s page=%d metas=%d", key, i, len(p.metas))
	if i >= 0 && i < len(p.metas) {
		fmt.Printf(" first=%s size=%d", p.metas[i].FirstKey, p.metas[i].Size)
		recs, _ := p.loadPage(i)
		fmt.Printf(" recs=%d recfirst=%s reclast=%s", len(recs), recs[0].Key, recs[len(recs)-1].Key)
		if i+1 < len(p.metas) {
			fmt.Printf(" next_first=%s", p.metas[i+1].FirstKey)
		}
	}
	fmt.Println()
}
func (p *paged) read(key string, at uint32) (Rec, bool) {
	if len(p.metas) == 0 {
		return Rec{}, false
	}
	pi := p.pageForKey(key)
	if pi < 0 || pi >= len(p.metas) {
		return Rec{}, false
	}
	for pi < len(p.metas) {
		recs, err := p.loadPage(pi)
		if err != nil {
			return Rec{}, false
		}
		// last record for key with seq<=at
		i := sort.Search(len(recs), func(i int) bool {
			if recs[i].Key != key {
				return recs[i].Key > key
			}
			return recs[i].Seq > at
		}) - 1
		if i >= 0 && recs[i].Key == key {
			if recs[i].Del {
				return Rec{}, false
			}
			return recs[i], true
		}
		if len(recs) == 0 || recs[len(recs)-1].Key > key {
			break
		}
		pi++
	}
	return Rec{}, false
}

func (p *paged) scanSequential(at uint32) []Rec {
	best := make(map[string]Rec, len(p.keys))
	for i := range p.metas {
		recs, err := p.loadPage(i)
		if err != nil {
			break
		}
		for _, r := range recs {
			if r.Seq <= at && (best[r.Key].Seq == 0 || r.Seq >= best[r.Key].Seq) {
				best[r.Key] = r
			}
		}
	}
	out := make([]Rec, 0, len(best))
	for _, r := range best {
		if !r.Del {
			out = append(out, r)
		}
	}
	sortRecs(out)
	return out
}

// hybrid: current rows resident (latest per key), historical versions in
// key-ordered pages whose sparse index is the only always-resident metadata.
type hybrid struct {
	latest map[string]Rec
	pages  *paged
}

func newHybrid(path string, recs []Rec, pageSize, budget int) (*hybrid, error) {
	p, err := newPaged(path, recs, pageSize, budget)
	if err != nil {
		return nil, err
	}
	h := &hybrid{latest: map[string]Rec{}, pages: p}
	for _, r := range recs {
		if cur, ok := h.latest[r.Key]; !ok || r.Seq >= cur.Seq {
			h.latest[r.Key] = r
		}
	}
	return h, nil
}
func (h *hybrid) read(key string, at uint32) (Rec, bool) {
	if r, ok := h.latest[key]; ok && r.Seq <= at {
		if r.Del {
			return Rec{}, false
		}
		return r, true
	}
	return h.pages.read(key, at)
}
func (h *hybrid) scanSequential(at uint32) []Rec {
	return h.pages.scanSequential(at)
}

// ---------- workloads ----------
const (
	opPoint = iota
	opRange
	opScan
)

type Op struct {
	Kind        int
	Key, Lo, Hi string
	At          uint32
}
type Trace struct {
	Ops     []Op
	Current uint32
}

func percentiles(d []time.Duration) (p50, p99, max time.Duration) {
	if len(d) == 0 {
		return
	}
	c := append([]time.Duration(nil), d...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return c[len(c)*50/100], c[len(c)*99/100], c[len(c)-1]
}

func hotKey(r *rng, keys int) string {
	if r.Intn(100) < 80 {
		return fmt.Sprintf("int:%d", r.Intn(keys/10))
	}
	return fmt.Sprintf("int:%d", r.Intn(keys))
}
func makeTrace(keys int, maxSeq uint32, point, ranges, scans int, seed uint64) Trace {
	r := newRng(seed)
	t := Trace{Current: maxSeq}
	for i := 0; i < point; i++ {
		if i%4 == 3 {
			t.Ops = append(t.Ops, Op{Kind: opPoint, Key: hotKey(r, keys), At: 1 + uint32(r.Intn(int(maxSeq)))})
		} else {
			t.Ops = append(t.Ops, Op{Kind: opPoint, Key: hotKey(r, keys), At: maxSeq})
		}
	}
	for i := 0; i < ranges; i++ {
		lo := r.Intn(keys - 8)
		t.Ops = append(t.Ops, Op{Kind: opRange, Lo: fmt.Sprintf("int:%d", lo), Hi: fmt.Sprintf("int:%d", lo+7), At: maxSeq})
	}
	for i := 0; i < scans; i++ {
		t.Ops = append(t.Ops, Op{Kind: opScan, At: 1 + uint32(r.Intn(int(maxSeq)))})
	}
	// stable order: deterministic shuffle
	r2 := newRng(seed ^ 0x9e3779b9)
	for i := len(t.Ops) - 1; i > 0; i-- {
		j := r2.Intn(i + 1)
		t.Ops[i], t.Ops[j] = t.Ops[j], t.Ops[i]
	}
	return t
}

type rng struct{ s uint64 }

func newRng(seed uint64) *rng { return &rng{s: seed*6364136223846793005 + 1442695040888963407} }
func (r *rng) next() uint64   { r.s ^= r.s << 13; r.s ^= r.s >> 7; r.s ^= r.s << 17; return r.s }
func (r *rng) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

func digest(recs []Rec) string {
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].Key != recs[j].Key {
			return recs[i].Key < recs[j].Key
		}
		return recs[i].Seq < recs[j].Seq
	})
	h := sha256.New()
	for _, r := range recs {
		h.Write(encRec(nil, r))
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}
func runBaseline(b *baseline, t Trace) (string, time.Duration, int64) {
	h := sha256.New()
	start := time.Now()
	rows := 0
	lat := make([]time.Duration, 0, len(t.Ops))
	pick := func(key string, at uint32) {
		if r, ok := b.read(key, at); ok {
			h.Write(encRec(nil, r))
			rows++
		}
	}
	for _, op := range t.Ops {
		o0 := time.Now()
		switch op.Kind {
		case opPoint:
			pick(op.Key, op.At)
		case opRange:
			for _, k := range b.keys {
				if k >= op.Lo && k <= op.Hi {
					pick(k, op.At)
				}
			}
		case opScan:
			for _, k := range b.keys {
				pick(k, op.At)
			}
		}
		lat = append(lat, time.Since(o0))
	}
	p50, p99, mx := percentiles(lat)
	fmt.Printf("LAT layout=all-resident p50_us=%.0f p99_us=%.0f max_us=%.0f\n", float64(p50.Microseconds()), float64(p99.Microseconds()), float64(mx.Microseconds()))
	return fmt.Sprintf("%x", h.Sum(nil))[:16], time.Since(start), int64(rows)
}
func runHistory(h *history, t Trace, sequentialScan bool) (string, time.Duration, int64, int64, int64) {
	hs := sha256.New()
	start := time.Now()
	rows := 0
	pick := func(key string, at uint32) {
		if r, ok := h.read(key, at); ok {
			hs.Write(encRec(nil, r))
			rows++
		}
	}
	for _, op := range t.Ops {
		switch op.Kind {
		case opPoint:
			pick(op.Key, op.At)
		case opRange:
			for _, k := range h.keys {
				if k >= op.Lo && k <= op.Hi {
					pick(k, op.At)
				}
			}
		case opScan:
			if sequentialScan {
				for _, r := range h.scanSequential(op.At) {
					hs.Write(encRec(nil, r))
					rows++
				}
			} else {
				for _, k := range h.keys {
					pick(k, op.At)
				}
			}
		}
	}
	return fmt.Sprintf("%x", hs.Sum(nil))[:16], time.Since(start), int64(rows), h.preads, h.preadBytes
}
func runPaged(p *paged, t Trace) (string, time.Duration, int64, int64, int64) {
	return runPagedScan(p, t, false)
}
func runPagedScan(p *paged, t Trace, seqScan bool) (string, time.Duration, int64, int64, int64) {
	hs := sha256.New()
	start := time.Now()
	rows := 0
	pick := func(key string, at uint32) {
		if r, ok := p.read(key, at); ok {
			hs.Write(encRec(nil, r))
			rows++
		}
	}
	for _, op := range t.Ops {
		switch op.Kind {
		case opPoint:
			pick(op.Key, op.At)
		case opRange:
			for _, k := range p.keys {
				if k >= op.Lo && k <= op.Hi {
					pick(k, op.At)
				}
			}
		case opScan:
			for _, k := range p.keys {
				pick(k, op.At)
			}
		}
	}
	return fmt.Sprintf("%x", hs.Sum(nil))[:16], time.Since(start), int64(rows), p.preads, p.preadBytes
}

func runHistoryResident(h *history, t Trace, seqScan bool) (string, time.Duration, int64, int64, int64) {
	hs := sha256.New()
	start := time.Now()
	rows := 0
	pick := func(key string, at uint32) {
		if r, ok := h.readResident(key, at); ok {
			hs.Write(encRec(nil, r))
			rows++
		}
	}
	for _, op := range t.Ops {
		switch op.Kind {
		case opPoint:
			pick(op.Key, op.At)
		case opRange:
			for _, k := range h.keys {
				if k >= op.Lo && k <= op.Hi {
					pick(k, op.At)
				}
			}
		case opScan:
			if seqScan {
				for _, r := range h.scanSequential(op.At) {
					hs.Write(encRec(nil, r))
					rows++
				}
			} else {
				for _, k := range h.keys {
					pick(k, op.At)
				}
			}
		}
	}
	return fmt.Sprintf("%x", hs.Sum(nil))[:16], time.Since(start), int64(rows), h.preads, h.preadBytes
}

func runHybrid(h *hybrid, t Trace, seqScan bool) (string, time.Duration, int64, int64, int64) {
	hs := sha256.New()
	start := time.Now()
	rows := 0
	pick := func(key string, at uint32) {
		if r, ok := h.read(key, at); ok {
			hs.Write(encRec(nil, r))
			rows++
		}
	}
	for _, op := range t.Ops {
		switch op.Kind {
		case opPoint:
			pick(op.Key, op.At)
		case opRange:
			for _, k := range h.pages.keys {
				if k >= op.Lo && k <= op.Hi {
					pick(k, op.At)
				}
			}
		case opScan:
			if seqScan {
				for _, r := range h.scanSequential(op.At) {
					hs.Write(encRec(nil, r))
					rows++
				}
			} else {
				for _, k := range h.pages.keys {
					pick(k, op.At)
				}
			}
		}
	}
	return fmt.Sprintf("%x", hs.Sum(nil))[:16], time.Since(start), int64(rows), h.pages.preads, h.pages.preadBytes
}

// ---------- main ----------
func heap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func main() {
	mode := flag.String("mode", "full", "full | heap-locator | heap-baseline | writes")
	keys := flag.Int("keys", 100000, "distinct primary keys")
	revs := flag.Int("revs", 8, "base revisions per key")
	hot := flag.Int("hot", 8, "extra revisions applied to the hottest 10% of keys")
	text := flag.Int("text", 64, "text payload bytes per version")
	points := flag.Int("points", 20000, "point operations")
	rangesN := flag.Int("ranges", 2000, "narrow range operations width 8")
	scans := flag.Int("scans", 3, "wide old-snapshot scans")
	budget := flag.Int("cache", 16, "row/page cache budget MiB")
	pageKiB := flag.Int("page", 16, "page size KiB")
	traceMode := flag.String("trace", "mixed", "mixed | point | scan | range")
	flag.Parse()

	recs := genCorpus(*keys, *revs, *hot, *text)
	if *mode == "writes" {
		runwrite(*keys, *revs, *hot, *text, *pageKiB, *budget)
		return
	}
	if *mode == "heap-locator" || *mode == "heap-baseline" {
		dir2, _ := os.MkdirTemp("", "lamina-heapmode-")
		defer os.RemoveAll(dir2)
		if *mode == "heap-baseline" {
			h := newBaseline(recs)
			fmt.Printf("HEAPMODE baseline versions=%d rows=%d\n", len(recs), len(h.dir))
		} else {
			h, _ := newHistory(dir2+"/h.bin", recs, 0)
			fmt.Printf("HEAPMODE locator versions=%d rows=%d locator_bytes=%d\n", len(recs), len(h.dir), h.locatorBytes())
		}
		return
	}
	maxSeq := uint32(0)
	for _, r := range recs {
		if r.Seq > maxSeq {
			maxSeq = r.Seq
		}
	}
	diskBytes := int64(0)
	for _, r := range recs {
		diskBytes += int64(recSize(r))
	}
	fmt.Printf("CORPUS keys=%d versions=%d max_seq=%d text=%d compact_bytes=%d bytes_per_version=%.1f\n",
		*keys, len(recs), maxSeq, *text, diskBytes, float64(diskBytes)/float64(len(recs)))

	// build each layout, measuring resident heap and build time
	var base *baseline
	{
		b0 := heap()
		t0 := time.Now()
		base = newBaseline(recs)
		dt := time.Since(t0)
		b1 := heap()
		fmt.Printf("BUILD layout=all-resident heap_mb=%.1f build_ms=%.0f\n", float64(b1-b0)/(1<<20), float64(dt.Microseconds())/1000)
	}
	dir, _ := os.MkdirTemp("", "lamina-proto-")
	defer os.RemoveAll(dir)
	var hist *history
	{
		b0 := heap()
		t0 := time.Now()
		var err error
		hist, err = newHistory(dir+"/history.bin", recs, *budget<<20)
		if err != nil {
			panic(err)
		}
		dt := time.Since(t0)
		b1 := heap()
		fmt.Printf("BUILD layout=locator+history heap_mb=%.1f (of which cache<=%dMiB) build_ms=%.0f file_bytes=%d\n",
			float64(b1-b0)/(1<<20), *budget, float64(dt.Microseconds())/1000, hist.size)
	}
	var pg *paged
	{
		b0 := heap()
		t0 := time.Now()
		var err error
		pg, err = newPaged(dir+"/pages.bin", recs, *pageKiB<<10, *budget<<20)
		if err != nil {
			panic(err)
		}
		dt := time.Since(t0)
		b1 := heap()
		fmt.Printf("BUILD layout=key-ordered-pages heap_mb=%.1f pages=%d page_kib=%d build_ms=%.0f oversized_pages=%d\n",
			float64(b1-b0)/(1<<20), len(pg.metas), *pageKiB, float64(dt.Microseconds())/1000, pg.oversized)
	}

	var trace Trace
	switch *traceMode {
	case "point":
		trace = makeTrace(*keys, maxSeq, *points, 0, 0, 7)
	case "scan":
		trace = makeTrace(*keys, maxSeq, 0, 0, *scans, 7)
	case "range":
		trace = makeTrace(*keys, maxSeq, 0, *rangesN, 0, 7)
	default:
		trace = makeTrace(*keys, maxSeq, *points, *rangesN, *scans, 7)
	}
	fmt.Printf("TRACE ops=%d point=%d range=%d scan=%d cache_mib=%d page_kib=%d\n", len(trace.Ops), *points, *rangesN, *scans, *budget, *pageKiB)

	d0, t0, n0 := runBaseline(base, trace)
	fmt.Printf("RUN layout=all-resident digest=%s rows=%d elapsed_ms=%.0f preads=0 read_mib=0.0\n", d0, n0, float64(t0.Microseconds())/1000)
	d1, t1, n1, r1, b1 := runHistory(hist, trace, false)
	fmt.Printf("RUN layout=locator+history digest=%s match=%v rows=%d elapsed_ms=%.0f preads=%d read_mib=%.1f cache_hit=%.3f cache_miss=%d evict=%d\n",
		d1, d1 == d0, n1, float64(t1.Microseconds())/1000, r1, float64(b1)/(1<<20), hitRate(hist.cache), hist.cache.misses, hist.cache.evictions)
	d2, t2, n2, r2, b2 := runPagedScan(pg, trace, true)
	fmt.Printf("RUN layout=key-ordered-pages digest=%s match=%v rows=%d elapsed_ms=%.0f preads=%d read_mib=%.1f cache_hit=%.3f cache_miss=%d evict=%d\n",
		d2, d2 == d0, n2, float64(t2.Microseconds())/1000, r2, float64(b2)/(1<<20), hitRate(pg.cache), pg.cache.misses, pg.cache.evictions)
	hy, herr := newHybrid(dir+"/hybrid.bin", recs, *pageKiB<<10, *budget<<20)
	if herr != nil {
		panic(herr)
	}
	dr, tr, nr, rr, br := runHistoryResident(hist, trace, false)
	fmt.Printf("RUN layout=locator+resident-current digest=%s match=%v rows=%d elapsed_ms=%.0f preads=%d read_mib=%.1f cache_hit=%.3f\n",
		dr, dr == d0, nr, float64(tr.Microseconds())/1000, rr, float64(br)/(1<<20), hitRate(hist.cache))
	dh, th, nh, rh, bh := runHybrid(hy, trace, true)
	fmt.Printf("RUN layout=hybrid(resident-current+pages) digest=%s match=%v rows=%d elapsed_ms=%.0f preads=%d read_mib=%.1f cache_hit=%.3f\n",
		dh, dh == d0, nh, float64(th.Microseconds())/1000, rh, float64(bh)/(1<<20), hitRate(hy.pages.cache))

	// wide-scan-only comparison and thrash: measure a scan tail after narrow ops
	wide := Trace{}
	for i := 0; i < 3; i++ {
		wide.Ops = append(wide.Ops, Op{Kind: opScan, At: 1 + uint32(i*3)})
	}
	dw0, tw0, nw0 := runBaseline(base, wide)
	dw1, tw1, nw1, rw1, bw1 := runHistory(hist, wide, true)
	fmt.Printf("WIDE layout=all-resident digest=%s rows=%d ms=%.0f | locator+sequential-scan digest=%s match=%v rows=%d ms=%.0f preads=%d read_mib=%.1f\n",
		dw0, nw0, float64(tw0.Microseconds())/1000, dw1, dw1 == dw0, nw1, float64(tw1.Microseconds())/1000, rw1, float64(bw1)/(1<<20))
	dw2, tw2, nw2, rw2, bw2 := runPagedScan(pg, wide, true)
	fmt.Printf("WIDE layout=key-ordered-pages digest=%s match=%v rows=%d ms=%.0f preads=%d read_mib=%.1f\n",
		dw2, dw2 == dw0, nw2, float64(tw2.Microseconds())/1000, rw2, float64(bw2)/(1<<20))
	dwh, twh, nwh, rwh, bwh := runHybrid(hy, wide, true)
	fmt.Printf("WIDE layout=hybrid(resident-current+pages) digest=%s match=%v rows=%d ms=%.0f preads=%d read_mib=%.1f\n",
		dwh, dwh == dw0, nwh, float64(twh.Microseconds())/1000, rwh, float64(bwh)/(1<<20))

	// retention accounting: retained versions if horizon is the oldest pinned snapshot
	fmt.Printf("HORIZON\n")
	for _, pin := range []uint32{1, maxSeq / 2, maxSeq - 1, maxSeq} {
		retained, bytes := retainedAt(recs, pin)
		fmt.Printf("  pinned_at=%d retained_versions=%d retained_bytes=%d pct=%.1f\n", pin, retained, bytes, 100*float64(retained)/float64(len(recs)))
	}
	fmt.Printf("FOOTPRINT disk_compact_mib=%.1f baseline_heap_mb=%.1f locator_heap_mb=%.1f page_meta_entries=%d\n",
		float64(diskBytes)/(1<<20), float64(heapAfterBaseline(recs))/(1<<20), float64(heapAfterHistory(recs))/(1<<20), len(pg.metas))
	verify(base, hist, pg, maxSeq)
	for _, at := range []uint32{1, maxSeq / 2, maxSeq} {
		want := map[string]bool{}
		for _, k := range base.keys {
			if r, ok := base.read(k, at); ok {
				want[string(encRec(nil, r))] = true
			}
		}
		got := map[string]bool{}
		for _, r := range hist.scanSequential(at) {
			got[string(encRec(nil, r))] = true
		}
		missing, extra := 0, 0
		for x := range want {
			if !got[x] {
				missing++
			}
		}
		for x := range got {
			if !want[x] {
				extra++
			}
		}
		fmt.Printf("SEQSCAN at=%d want=%d got=%d missing=%d extra=%d\n", at, len(want), len(got), missing, extra)
	}
	if os.Getenv("DEBUG") != "" {
		for _, k := range []string{"int:1040", "int:1087"} {
			pg.debugKey(k)
		}
	}
}

func verify(base *baseline, h *history, p *paged, maxSeq uint32) {
	keys := append([]string(nil), base.keys...)
	bad := 0
	for _, k := range keys {
		for at := uint32(0); at <= maxSeq; at++ {
			b, bok := base.read(k, at)
			hrec, hok := h.read(k, at)
			prec, pok := p.read(k, at)
			if bok != hok || bok != pok {
				if bad < 5 {
					fmt.Printf("VERIFY_MISMATCH key=%s at=%d base=%v h=%v p=%v\n", k, at, bok, hok, pok)
				}
				bad++
				continue
			}
			if bok {
				be, he, pe := encRec(nil, b), encRec(nil, hrec), encRec(nil, prec)
				if string(be) != string(he) || string(be) != string(pe) {
					if bad < 5 {
						fmt.Printf("VERIFY_VALUE_MISMATCH key=%s at=%d\n", k, at)
					}
					bad++
				}
			}
		}
	}
	fmt.Printf("VERIFY keys=%d seqs=%d mismatches=%d\n", len(keys), maxSeq+1, bad)
}

func hitRate(c *lru) float64 {
	if c.hits+c.misses == 0 {
		return 0
	}
	return float64(c.hits) / float64(c.hits+c.misses)
}

func retainedAt(recs []Rec, horizon uint32) (int, int64) {
	byKey := map[string][]Rec{}
	for _, r := range recs {
		byKey[r.Key] = append(byKey[r.Key], r)
	}
	n := 0
	var b int64
	for _, chain := range byKey {
		keep := 0
		for keep+1 < len(chain) && chain[keep+1].Seq <= horizon {
			keep++
		}
		if chain[len(chain)-1].Seq <= horizon && chain[len(chain)-1].Del {
			continue
		}
		for _, r := range chain[keep:] {
			n++
			b += int64(recSize(r))
		}
	}
	return n, b
}
func heapAfterBaseline(recs []Rec) uint64 {
	b0 := heap()
	b := newBaseline(recs)
	b1 := heap()
	runtime.KeepAlive(b)
	if b1 < b0 {
		return 0
	}
	return b1 - b0
}
func heapAfterHistory(recs []Rec) uint64 {
	dir, _ := os.MkdirTemp("", "lamina-h2-")
	defer os.RemoveAll(dir)
	runtime.GC()
	b0 := heap()
	h, _ := newHistory(dir+"/h.bin", recs, 0)
	runtime.GC()
	b1 := heap()
	runtime.KeepAlive(h)
	if b1 < b0 {
		return 0
	}
	return b1 - b0
}

var _ = fnv.New64a
