"""Serial fresh owners with identity lease, raw sealing and explicit reconciliation."""
import argparse, csv, io, subprocess, sys, os, time, hashlib, json, platform
from .common import *
from .process_identity import identity
import shutil
from .owned_resources import observe,finish

def raw_files(out):
    return sorted(p for p in out.rglob('*') if p.is_file() and 'runtime' not in p.relative_to(out).parts and p.name not in ['RAW_SEAL.json','COMPLETE.json','CONTENT_SCORES.json','SCORED_READS.json'])
def seal(out,rc):
    entries=[{'path':p.relative_to(out).as_posix(),'bytes':p.stat().st_size,'sha256':sha(p)} for p in raw_files(out)]
    save(out/'RAW_SEAL.json',{'at':utc(),'files':entries,'scope':'Recorded raw receipts excluding native database/cache/runtime trees','returncode':rc})
    result=read(out/'RESULT.json') if (out/'RESULT.json').exists() else {}
    save(out/'COMPLETE.json',{'at':utc(),'raw_seal_sha256':sha(out/'RAW_SEAL.json'),'execution':result.get('execution','HARNESS_FAILED'),'returncode':rc})
def verify(out):
    seal=read(out/'RAW_SEAL.json');complete=read(out/'COMPLETE.json')
    assert complete['raw_seal_sha256']==sha(out/'RAW_SEAL.json')
    for f in seal['files']:assert (out/f['path']).stat().st_size==f['bytes'] and sha(out/f['path'])==f['sha256'],str(out/f['path'])
    return complete

def execute(mode):
    leasepath=ROOT/('ENGINEERING_LEASE.json' if mode=='engineering' else 'COORDINATOR_LEASE.json')
    if leasepath.exists():
        old=read(leasepath);alive=identity(old.get('pid',-1))
        if alive:raise RuntimeError('Existing PID alive; reconcile creation identity before starting')
        active=old.get('active',{})
        if active.get('pid') and identity(active['pid']):raise RuntimeError('Previous worker PID alive; reconcile before starting')
    me=identity(os.getpid())
    save(leasepath,{'pid':os.getpid(),'identity':me,'mode':mode,'state':'RUNNING','at':utc()})
    if mode=='formal':
        frozen=read(ROOT/'configs/FREEZE.json')
        for name,digest in frozen['files'].items():assert sha(ROOT/name)==digest,name
    rows=list(csv.DictReader((ROOT/'MATRIX.csv').open(encoding='utf-8',newline='')))
    if mode=='engineering':
        rows=[dict(unit_id=f'QUAL_{site}_{fault}_NATIVE',history_id='81001',site_id=site,policy='NATIVE',fault_condition=fault,engineering=True) for site in ['P1','P2'] for fault in ['INJECT','NO_FAULT']]
    completed=[]
    for i,c in enumerate(rows):
        unit=c['unit_id'];basedir=ROOT/('engineering' if mode=='engineering' else 'runs')/unit
        attempts=sorted(basedir.glob('attempt-*'))
        out=attempts[-1] if attempts else basedir/'attempt-001'
        if mode=='formal' and (basedir/'SELECTED_ATTEMPT.json').exists():
            selection=read(basedir/'SELECTED_ATTEMPT.json');out=basedir/selection['attempt']
        repair=basedir/'RETRY_AUTHORIZATION.json'
        if mode=='engineering' and repair.exists() and out.exists():
            auth=read(repair)
            if auth.get('failed_attempt')==out.name:
                assert verify(out)['execution']=='HARNESS_FAILED'
                out=basedir/auth['next_attempt']
        if out.exists():
            if (out/'COMPLETE.json').exists():
                status=verify(out)
                if status['execution'] in ['TERMINATED_FULL_SEQUENCE','NATIVE_WORK_UNRESOLVED']:
                    completed.append(unit);continue
            raise RuntimeError('Unsealed or failed existing attempt requires reconciliation: '+str(out))
        basedir.mkdir(parents=True,exist_ok=True);config=basedir/'CONFIG.json'
        if mode=='formal':c['implementation_revision']=frozen['version']
        save(config,c)
        log=ROOT/'runtime_logs'/(unit+'_'+out.name)
        with log.with_suffix('.stdout.log').open('wb') as stdout,log.with_suffix('.stderr.log').open('wb') as stderr:
            proc=subprocess.Popen([str(PYTHON),'-X','utf8','-u','-B','-m','src.worker_inc','--config',str(config),'--out',str(out)],cwd=ROOT,env=worker_env(out),stdout=stdout,stderr=stderr,stdin=subprocess.DEVNULL,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            known={};observe(proc.pid,known)
            info={'command':[str(PYTHON),'-X','utf8','-u','-B','-m','src.worker_inc','--config',str(config),'--out',str(out)],'pid':proc.pid,'identity':identity(proc.pid),'at':utc(),'unit':unit,'out':str(out),'mode':mode}
            save(basedir/'PROCESS.json',info)
            save(leasepath,{'pid':os.getpid(),'identity':me,'mode':mode,'state':'RUNNING','at':utc(),'active':info,'completed':len(completed),'planned':len(rows)})
            print(json.dumps({'started':unit,'index':i+1,'total':len(rows),'pid':proc.pid}),flush=True)
            try:
                deadline=time.monotonic()+1800
                while proc.poll() is None:
                    observe(proc.pid,known)
                    try:proc.wait(timeout=min(10,max(.01,deadline-time.monotonic())))
                    except subprocess.TimeoutExpired:
                        if time.monotonic()>=deadline:raise
                rc=proc.returncode
            except subprocess.TimeoutExpired:
                # Own Popen child only; preserve every attempt and quarantine the store.
                proc.terminate();proc.wait(timeout=30);rc=76
                if out.exists():save(out/'CONTROLLER_WATCHDOG.json',{'state':'INTERRUPTED_OWNED_WORKER','at':utc(),'seconds':1800})
        if out.exists():finish(known,out)
        save(basedir/'EXIT.json',{'at':utc(),'returncode':rc})
        if out.exists():
            for suffix in ['stdout.log','stderr.log']:
                shutil.copy2(log.with_suffix('.'+suffix),out/suffix)
            save(out/'PROCESS.json',info)
            save(out/'EXIT.json',{'at':utc(),'returncode':rc})
            seal(out,rc)
        result=read(out/'RESULT.json') if (out/'RESULT.json').exists() else {}
        if rc not in [0,75] or result.get('execution') not in ['TERMINATED_FULL_SEQUENCE','NATIVE_WORK_UNRESOLVED']:
            save(leasepath,{'pid':os.getpid(),'identity':me,'mode':mode,'state':'NEEDS_ENGINEERING_RECONCILIATION','at':utc(),'active':info,'returncode':rc})
            raise RuntimeError('Preserved unsuccessful engineering/harness attempt '+unit)
        completed.append(unit)
        save(ROOT/('ENGINEERING_CHECKPOINT.json' if mode=='engineering' else 'CHECKPOINT.json'),{'at':utc(),'mode':mode,'completed':completed,'count':len(completed),'planned':len(rows)})
        status=read(ROOT/'STATUS.json');status.update(updated_at=utc(),state='ENGINEERING_RUNNING' if mode=='engineering' else 'FORMAL_RUNNING')
        status['engineering_terminated' if mode=='engineering' else 'formal_completed']=len(completed)
        if mode=='formal':status['formal_started']=i+1
        save(ROOT/'STATUS.json',status)
        print(json.dumps({'completed':unit,'count':len(completed),'execution':result['execution']}),flush=True)
    save(leasepath,{'pid':os.getpid(),'identity':me,'mode':mode,'state':'MATRIX_TERMINATED','at':utc(),'completed':len(completed),'planned':len(rows)})
    status=read(ROOT/'STATUS.json');status.update(state='ENGINEERING_QUALIFIED' if mode=='engineering' else 'FORMAL_MATRIX_TERMINATED',updated_at=utc());save(ROOT/'STATUS.json',status)
    print(json.dumps({'mode':mode,'matrix_terminated':len(completed)}),flush=True)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--mode',choices=['engineering','formal'],required=True);a=p.parse_args()
    execute(a.mode)
