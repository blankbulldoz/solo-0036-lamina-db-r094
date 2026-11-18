#!/usr/bin/env python3
"""(a) plan facts drift; (b) horizon pinning blocks retention; (c) checkpoint size crossing."""
import json, pathlib, sys, tempfile
sys.path.insert(0,'/workspace/analysis/equiv')
from equiv import S
b=pathlib.Path('/workspace/bin/laminad-linux')
out={}
with tempfile.TemporaryDirectory(prefix='lamina-hz-') as td:
    s=S(b, pathlib.Path(td)/'db').start()
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
        def row(i,rev): return {'sample':i,'band':f'b{i}-r{rev}','amplitude':(i*10+rev),'valid':rev%2==0}
        for rev in range(1,6): commit([row(i,rev) for i in range(1,41)])
        commit([],deletes=[7,13])
        # (a) facts vs rows around a vacuum: query at a position that IS the horizon
        lo=s.call('GET','/catalog')['relations'][0]['created']
        head=s.call('GET','/stats')['sequence']
        q=lambda at: s.call('POST','/query',{'relation':'spectra','at':at})
        before={str(at):q(at) for at in range(lo,head+1)}
        s.call('POST','/vacuum',{'horizon':head})
        after={str(at):q(at) for at in range(lo,head+1) if False}
        at=str(head); after_h=q(head)
        out['facts_vs_rows']={'position':at,
          'rows_equal':before[at]['rows']==after_h['rows'],
          'plan_before':before[at]['plan'],'plan_after':after_h['plan'],
          'scanned_before':before[at]['scanned'],'scanned_after':after_h['scanned']}
        # (b) pinned long reader: how far can vacuum advance, and how much history stays?
        ro=s.call('POST','/transactions',{'read_only':True},201)['id']
        # advance head with 10 more revisions
        for rev in range(6,16): commit([row(i,rev) for i in range(1,41)])
        newhead=s.call('GET','/stats')['sequence']
        v1=s.call('POST','/vacuum',{'horizon':newhead})
        # active reader forces the effective horizon to its snapshot
        st=s.call('GET','/stats')
        pinned_at=s.call('GET','/stats')['active'][0]['snapshot']
        out['pinned_reader']={'requested':newhead,'result':v1,'pinned_snapshot':pinned_at,
          'retained_versions':st['versions'],'keys':st['keys'],
          'retained_ratio_per_key':round(st['versions']/max(1,st['keys']),2)}
        # release the reader, then vacuum again
        s.call('POST',f'/transactions/{ro}/rollback')
        v2=s.call('POST','/vacuum',{'horizon':newhead})
        st2=s.call('GET','/stats')
        out['after_release']={'result':v2,'retained_versions':st2['versions'],
          'retained_ratio_per_key':round(st2['versions']/max(1,st2['keys']),2)}
        out['versions_after']=st2['versions']
        # (c) does a checkpoint capture the wide historical interval while pinned?
        ro2=s.call('POST','/transactions',{'read_only':True},201)['id']
        for rev in range(16,26): commit([row(i,rev) for i in range(1,41)])
        cp=s.call('POST','/checkpoint')
        out['checkpoint_with_pin']={'sequence':cp['sequence'],'active_pins':1}
    finally: s.stop()
print(json.dumps(out,indent=1,sort_keys=True))
