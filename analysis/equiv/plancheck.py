#!/usr/bin/env python3
import json, pathlib, sys, tempfile
sys.path.insert(0,'/workspace/analysis/equiv')
from equiv import S
b=pathlib.Path('/workspace/bin/laminad-linux')
with tempfile.TemporaryDirectory(prefix='lamina-plan-') as td:
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
        for rev in range(1,4): commit([row(i,rev) for i in range(1,21)])
        commit([],deletes=[5]); last=commit([row(5,9),row(21,1)])
        q=lambda at: s.call('POST','/query',{'relation':'spectra','at':at,'where':[{'column':'amplitude','op':'>=','value':0}]})
        b_rows, b_plan = q(last)['rows'], q(last)['plan']
        s.call('POST','/vacuum',{'horizon':last})
        a=q(last)
        print(json.dumps({'before_plan':b_plan,'after_plan':a['plan'],
          'rows_equal':b_rows==a['rows'],'before_n':len(b_rows),'after_n':len(a['rows']),
          'before_scanned':q(last)['scanned'],'after_scanned':a['scanned'],
          'before_matched':q(last)['matched'],'after_matched':a['matched']},sort_keys=True))
    finally: s.stop()
