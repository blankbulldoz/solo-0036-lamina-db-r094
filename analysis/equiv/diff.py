#!/usr/bin/env python3
import json, pathlib, subprocess, sys, tempfile
sys.path.insert(0,'/workspace/analysis/equiv')
from equiv import S
binary=pathlib.Path('/workspace/bin/laminad-linux')
with tempfile.TemporaryDirectory(prefix='lamina-diff-') as td:
    s=S(binary, pathlib.Path(td)/'db').start()
    try:
        s.call('POST','/relations',{'name':'spectra','primary':'sample','columns':[
            {'name':'sample','type':'int'},{'name':'band','type':'text'},
            {'name':'amplitude','type':'int','nullable':True},{'name':'valid','type':'bool'}]},201)
        s.call('POST','/indexes',{'name':'by_band','relation':'spectra','column':'band'},201)
        def commit(rows,deletes=()):
            tx=s.call('POST','/transactions',{},201)['id']
            for r in rows: s.call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':r})
            for k in deletes: s.call('POST',f'/transactions/{tx}/delete',{'relation':'spectra','key':k})
            return s.call('POST',f'/transactions/{tx}/commit')['sequence']
        def row(i,rev,amp=None): return {'sample':i,'band':f'b{i}-r{rev}','amplitude':(i*10+rev) if amp is None else amp,'valid':rev%2==0}
        for rev in range(1,6): commit([row(i,rev) for i in range(1,41)])
        commit([],deletes=[7,13]); last=commit([row(7,9),row(41,1)])
        before=s.call('POST','/query',{'relation':'spectra','at':last})
        s.call('POST','/vacuum',{'horizon':last})
        after=s.call('POST','/query',{'relation':'spectra','at':last})
        print('last',last,'before plan',before['plan'],'after plan',after['plan'])
        print('before rows',len(before['rows']),'after rows',len(after['rows']))
        b={r['sample']:r for r in before['rows']}; a={r['sample']:r for r in after['rows']}
        for k in sorted(set(b)|set(a)):
            if b.get(k)!=a.get(k): print('DIFF',k,b.get(k),a.get(k))
        print('stats',s.call('GET','/stats'))
    finally: s.stop()
