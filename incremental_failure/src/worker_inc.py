"""Fresh owner and store; three predeclared service stages, no online gold gating."""
import argparse, asyncio, contextlib, importlib, os, sys, traceback, time
from .common import *
from .clock import now_ns, wait_until_ns
from .native import Session
from . import native_base as rt
from .fault_hooks import FaultHooks

class Unresolved(RuntimeError):pass
_configure=rt.configure
def configure(out):
    mapping=read(ROOT/'configs/RUNTIME_STORAGE.json')
    runtime_root=Path(mapping['runtime_root'])
    relative=Path(out).resolve().relative_to(ROOT.resolve())
    from .storage_paths import storage_path
    native_out=storage_path(runtime_root,relative)
    native_out.mkdir(parents=True,exist_ok=False)
    system=_configure(native_out)
    save(Path(out)/'configuration.json',read(native_out/'configuration.json'))
    save(Path(out)/'STORAGE_LOCATION.json',{'native_root':str(native_out),'receipts':str(out),'filesystem':'NTFS','same_round_only':True})
    return system
rt.configure=configure

async def health(s):
    return {'gate':s.gate.state,'epoch':await s.epoch.snapshot(),'acknowledged_head':s.head}

def persist(s):
    save(s.out/'READS.json',s.reads);save(s.out/'WRITES.json',s.writes)

async def bounded(awaitable,seconds,s,label):
    task=asyncio.ensure_future(awaitable)
    done,_=await asyncio.wait([task],timeout=seconds)
    if not done:
        persist(s)
        save(s.out/'UNRESOLVED.json',{'at':utc(),'operation':label,'bound_seconds':seconds,'state':'NATIVE_WORK_UNRESOLVED','task_cancelled':False})
        s.observer.emit('lifecycle.unresolved',operation=label,bound_seconds=seconds)
        raise Unresolved(label)
    return task.result()

async def drain(s,label,remaining=120):
    module=importlib.import_module('cognee.modules.pipelines.layers.pipeline_execution_mode')
    pending=[t for t in module._BACKGROUND_PIPELINE_TASKS if not t.done()]
    pending+= [t for t in s.tasks if not t.done() and t not in pending]
    s.observer.emit('lifecycle.drain_observed',label=label,pending=len(pending))
    if pending:
        await bounded(asyncio.gather(*pending,return_exceptions=True),max(0,remaining),s,label+'.drain')
    s.observer.emit('lifecycle.native_terminated',label=label,pending=0)
    return now_ns()

async def checkpoint(s,phase):
    token=rt.OP.set('observer:'+phase);started=now_ns()
    s.observer.emit('checkpoint.begin',phase=phase,role='read_only_observer_not_service')
    try:
        result=await bounded(s.probe(phase),120,s,'checkpoint.'+phase)
        from cognee.modules.data.methods.get_dataset_data import get_dataset_data
        from cognee.infrastructure.files.utils.open_data_file import open_data_file
        rows=await get_dataset_data(s.dataset);canonical=[]
        for row in rows:
            entry={c.name:rt.plain(getattr(row,c.name)) for c in row.__table__.columns}
            try:
                async with open_data_file(row.raw_data_location) as f:raw=f.read()
                text=raw.decode('utf-8') if isinstance(raw,bytes) else raw
                entry.update(text=text,sha256=hashlib.sha256(text.encode()).hexdigest())
            except Exception as e:entry['read_error']=repr(e)
            canonical.append(entry)
        receipt={'phase':phase,'started_ns':started,'ended_ns':now_ns(),'canonical_content':canonical,'backend_probe':phase+'.probe.json','health':await health(s),'role':'READ_ONLY_OBSERVER'}
        save(s.out/(phase+'.checkpoint.json'),receipt);s.observer.emit('checkpoint.complete',record=receipt)
        return receipt
    except Unresolved:raise
    except Exception as e:
        receipt={'phase':phase,'started_ns':started,'ended_ns':now_ns(),'error':repr(e),'traceback':traceback.format_exc(),'role':'READ_ONLY_OBSERVER'}
        save(s.out/(phase+'.checkpoint.json'),receipt);s.observer.emit('checkpoint.error',record=receipt);return receipt
    finally:rt.OP.reset(token)

async def service_stage(s,phase,zero):
    tasks=[]
    async def one(i,target):
        before=await health(s)
        row=await s.read(phase+'_'+str(i),arrival_ns=target,deadline=10,category=phase)
        row['health_before']=before;row['health_after']=await health(s)
        return row
    for i,offset in enumerate([0,1,2,4,8]):
        target=zero+offset*10**9;await wait_until_ns(target)
        tasks.append(asyncio.create_task(one(i,target)))
    await bounded(asyncio.gather(*tasks),120,s,phase+'.probe_drain')
    await drain(s,phase)
    persist(s)
    return await checkpoint(s,phase)

async def update(s,name,mode,recovery):
    s.requested_mode=mode;before=await health(s);submitted=now_ns()
    row=await bounded(s.update(name,1,arrival_ns=submitted,deadline=30,recovery=recovery),120,s,name)
    row['health_before']=before
    row['actual_quiescent_ns']=await drain(s,name,120-(now_ns()-submitted)/1e9)
    row['health_after']=await health(s)
    row['branch_entered']=s.hooks.branches.get(name)
    persist(s)
    return row

async def execute(c,out):
    assert 'sitecustomize' in sys.modules,'Remote guard must load before any native import'
    save(out/'REQUEST.json',c)
    s=Session(out,c['policy'],'DEFAULT_REQUEST');hooks=None;phases=[]
    try:
        await s.open()
        save(out/'GUARD_ACTIVE.json',{'at':utc(),'sitecustomize':sys.modules['sitecustomize'].__file__,'connect_module':__import__('socket').socket.connect.__module__})
        await bounded(s.read('initial',category='initial',deadline=10),120,s,'initial')
        await checkpoint(s,'initial')
        persist(s)
        # Qualification of complete v0 is a predeclared setup condition only.
        from .content import ReceiptIndex
        idx=ReceiptIndex(out);initial=s.reads[-1];call=next((x for x in initial.get('native_calls',[]) if x['name']==initial.get('published_native')),None)
        initial_score=idx.score(read(out/call['payload_path']),call['name']) if call else {}
        save(out/'INITIAL_CHECK.json',initial_score)
        if initial_score.get('compatibility')!='COMPATIBLE_WITHIN_DECLARED_SCOPE' or not initial_score.get('coverage_complete') or initial_score.get('versions') != [0]:raise RuntimeError('INITIAL_STATE_NOT_COMPLETE_V0')
        hooks=FaultHooks(s,c['site_id'],c['fault_condition']=='INJECT');hooks.install()
        target=await update(s,'target_v1','DEFAULT_REQUEST',False)
        hooks.disarm()
        zero=target['actual_quiescent_ns'];phases.append({'phase':'S0','zero_ns':zero,'update':'target_v1'})
        await service_stage(s,'S0',zero)
        earliest=zero+15*10**9;safe_ready=now_ns();await wait_until_ns(earliest)
        s.observer.emit('recovery.trigger',recovery='R1',earliest_ns=earliest,safe_ready_ns=safe_ready,actual_ns=now_ns())
        r1=await update(s,'retry_v1','DEFAULT_REQUEST',True)
        zero=r1['actual_quiescent_ns'];phases.append({'phase':'S1','zero_ns':zero,'update':'retry_v1','earliest_trigger_ns':earliest})
        await service_stage(s,'S1',zero)
        earliest=zero+15*10**9;safe_ready=now_ns();await wait_until_ns(earliest)
        s.observer.emit('recovery.trigger',recovery='R2',earliest_ns=earliest,safe_ready_ns=safe_ready,actual_ns=now_ns())
        r2=await update(s,'rebuild_v1','FORCED_FULL',True)
        zero=r2['actual_quiescent_ns'];phases.append({'phase':'S2','zero_ns':zero,'update':'rebuild_v1','earliest_trigger_ns':earliest})
        await service_stage(s,'S2',zero)
        hooks.close();hooks=None
        await s.close()
        save(out/'PHASES.json',phases)
        save(out/'RESULT.json',{'at':utc(),'execution':'TERMINATED_FULL_SEQUENCE','unit_id':c['unit_id'],'phase_count':len(phases),'service_probes':sum(r['category'] in ['S0','S1','S2'] for r in s.reads)})
    except Unresolved:
        persist(s)
        if hooks:hooks.close()
        save(out/'PHASES.json',phases)
        save(out/'RESULT.json',{'at':utc(),'execution':'NATIVE_WORK_UNRESOLVED','unit_id':c['unit_id'],'subsequent_stages':'NOT_EXECUTED'})
        s.observer.stream.flush()
        os._exit(75) # This owner only; quarantine its store, no crash-recovery claim.
    except BaseException as e:
        persist(s)
        save(out/'FAILURE.json',{'at':utc(),'error':repr(e),'traceback':traceback.format_exc(),'phase_records':phases})
        if hooks:hooks.close()
        with contextlib.suppress(BaseException):await asyncio.wait_for(s.close(),30)
        raise

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--out',required=True);a=p.parse_args()
    out=Path(a.out).resolve();assert out.is_relative_to(ROOT.resolve());out.mkdir(parents=True,exist_ok=False)
    asyncio.run(execute(read(a.config),out))
