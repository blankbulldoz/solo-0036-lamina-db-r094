#!/usr/bin/env python3
"""Public-API equivalence probe: does a commit position explain the same rows
across checkpoint+restart and across vacuum+restart?

Usage: python3 analysis/equiv/equiv.py [--binary bin/laminad-linux]
Runs in its own temp dir; prints one JSON summary.
"""
import argparse, json, os, pathlib, shutil, socket, subprocess, tempfile, time, urllib.request, urllib.error

class S:
    def __init__(self, binary, root):
        self.binary=str(binary); self.root=pathlib.Path(root); self.p=None; self.log=None
    def start(self):
        with socket.socket() as s: s.bind(('127.0.0.1',0)); port=s.getsockname()[1]
        self.url=f'http://127.0.0.1:{port}/v1/lamina'
        self.log=tempfile.TemporaryFile()
        self.p=subprocess.Popen([self.binary,'-data',str(self.root),'-listen',f'127.0.0.1:{port}'],stdout=self.log,stderr=self.log)
        end=time.monotonic()+15
        while time.monotonic()<end:
            if self.p.poll() is not None:
                self.log.seek(0); raise RuntimeError(self.log.read().decode())
            try: self.call('GET','/health'); return self
            except Exception: time.sleep(.03)
        raise RuntimeError('not ready')
    def stop(self):
        if self.p and self.p.poll() is None:
            self.p.terminate()
            try: self.p.wait(timeout=15)
            except subprocess.TimeoutExpired: self.p.kill(); self.p.wait()
        if self.log: self.log.close(); self.log=None
    def call(self, method, path, data=None, status=200):
        body=None if data is None else json.dumps(data).encode()
        req=urllib.request.Request(self.url+path,data=body,method=method,headers={'Content-Type':'application/json'})
        try:
            with urllib.request.urlopen(req,timeout=30) as r: code=r.status; raw=r.read()
        except urllib.error.HTTPError as e: code=e.code; raw=e.read()
        if code!=status: raise RuntimeError(f'{method} {path}: {code}: {raw.decode()[:300]}')
        return json.loads(raw)

def snapshot_at(s, at):
    try:
        return s.call('POST','/query',{'relation':'spectra','at':at})
    except RuntimeError as e:
        if '410' in str(e): return {'gone': True}
        raise

def all_positions(s, lo, hi):
    return {str(at): snapshot_at(s, at) for at in range(lo, hi+1)}

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--binary',default='bin/laminad-linux'); a=ap.parse_args()
    binary=pathlib.Path(a.binary).resolve()
    out={'phases':{}}
    with tempfile.TemporaryDirectory(prefix='lamina-equiv-') as td:
        s=S(binary, pathlib.Path(td)/'db').start()
        try:
            s.call('POST','/relations',{'name':'spectra','primary':'sample','columns':[
                {'name':'sample','type':'int'},{'name':'band','type':'text'},
                {'name':'amplitude','type':'int','nullable':True},{'name':'valid','type':'bool'}]},201)
            s.call('POST','/indexes',{'name':'by_band','relation':'spectra','column':'band'},201)
            seq=[]
            def commit(rows, deletes=()):
                tx=s.call('POST','/transactions',{},201)['id']
                for r in rows: s.call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':r})
                for k in deletes: s.call('POST',f'/transactions/{tx}/delete',{'relation':'spectra','key':k})
                c=s.call('POST',f'/transactions/{tx}/commit'); seq.append(c['sequence']); return c['sequence']
            def row(i, rev, amp=None):
                return {'sample':i,'band':f'b{i}-r{rev}','amplitude': (i*10+rev) if amp is None else amp,'valid':rev%2==0}
            for rev in range(1,6):
                commit([row(i,rev) for i in range(1,41)])
            commit([], deletes=[7, 13])
            commit([row(7,9), row(41,1)])
            lo=s.call('GET','/catalog')['relations'][0]['created']
            head=s.call('GET','/stats')['sequence']
            before=all_positions(s, lo, head)
            out['phases']['before']={'lo':lo,'head':head,'positions':len(before),
                'visible_counts':{k:(0 if 'gone' in v else len(v['rows'])) for k,v in before.items()}}
            # checkpoint, restart, compare every position in retained interval
            s.call('POST','/checkpoint')
            s.stop(); s.start()
            after=all_positions(s, lo, head)
            diffs=[k for k in before if before[k]!=after[k]]
            out['phases']['after_restart']={'diffs':diffs,'positions':len(after),
                'matched':len(before)-len(diffs)}
            # vacuum to head with no active readers; positions below head must be gone,
            # positions >= head must be unchanged
            v=s.call('POST','/vacuum',{'horizon':head})
            out['phases']['vacuum']=v
            post=all_positions(s, lo, head)
            tail_diffs=[k for k in post if k>=str(head) and before.get(k)!=post[k]]
            pre_gone=[k for k in post if k<str(head) and 'gone' not in post[k]]
            out['phases']['after_vacuum']={'tail_diffs':tail_diffs,'pre_horizon_still_visible':pre_gone,
                'post_horizon_positions':[k for k in post if k>=str(head)]}
            s.call('POST','/checkpoint'); s.stop(); s.start()
            after2=all_positions(s, head, head)
            out['phases']['after_vacuum_restart']={'head_equal':after2.get(str(head))==post.get(str(head))}
            # now a pinned long reader: begin at head, advance head, vacuum, checkpoint
            s.call('POST','/transactions',{'read_only':True},201)
            tx=s.call('POST','/transactions',{},201)['id']
            for i in range(42,45): s.call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':row(i,1)})
            s.call('POST',f'/transactions/{tx}/commit')
            newhead=s.call('GET','/stats')['sequence']
            pinned=s.call('POST','/vacuum',{'horizon':newhead})
            out['phases']['pinned_vacuum']=pinned
            out['final_stats']=s.call('GET','/stats')
        finally:
            s.stop()
    print(json.dumps(out,indent=1,sort_keys=True))

if __name__=='__main__': main()
