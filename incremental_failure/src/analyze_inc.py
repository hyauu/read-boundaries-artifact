"""Frozen offline analysis for the 160-unit incremental failure protocol."""
import csv, json, argparse, itertools
from collections import Counter,defaultdict
from .common import *
from .content import score_attempt
from .controller_inc import verify
from .outcomes import *

METRICS=('usable','conflict','partial','unknown','dirty','native_error','deadline_or_exhausted')
def csvout(path,rows):
    keys=list(dict.fromkeys(k for r in rows for k in r))
    with Path(path).open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader()
        for r in rows:w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(list,dict)) else v for k,v in r.items()})
def get(path,default=None):return read(path) if Path(path).exists() else ({} if default is None else default)
def available_attempt(unit):
    base=ROOT/'runs'/unit;attempts=sorted(base.glob('attempt-*'))
    if not attempts:return None
    selection=get(base/'SELECTED_ATTEMPT.json')
    if len(attempts)>1:
        assert selection.get('attempt'),'Multiple attempts require explicit group revision selection'
        out=base/selection['attempt']
    else:out=attempts[0]
    assert (out/'COMPLETE.json').exists(),'In-flight/unsealed unit '+unit
    verify(out);return out
def checkpoint_row(out,phase,meta):
    cp=get(out/(phase+'.checkpoint.json'));raw=get(out/(phase+'.probe.json'))
    def values(x):
        if isinstance(x,dict):
            for k,v in x.items():
                if k in ('text','edge_text','relationship_name') and isinstance(v,str):yield v
                elif isinstance(v,(dict,list)):yield from values(v)
        elif isinstance(x,list):
            for v in x:yield from values(v)
    result=dict(meta,phase=phase,executed=bool(cp),canonical_version=canonical_version(cp),canonical_rows=len(cp.get('canonical_content',[])),checkpoint_ms=(cp.get('ended_ns',0)-cp.get('started_ns',0))/1e6 if cp else None,health=cp.get('health'),error=cp.get('error'),sampling_start_ns=cp.get('started_ns'),sampling_end_ns=cp.get('ended_ns'))
    result['canonical_target_match']=result['canonical_version']==1 if result['canonical_version'] is not None else None
    result['graph_nodes']=len(raw.get('graph_nodes',[]));result['graph_edges']=len(raw.get('graph_edges',[]))
    result['vector_collections']={k:len(v) for k,v in raw.get('vectors',{}).items()}
    result['graph_content_versions']=sorted({v for txt in values({'nodes':raw.get('graph_nodes',[]),'edges':raw.get('graph_edges',[])}) for v,src in enumerate(SOURCES) if src in txt or any(s+'.' in txt for s in src.split('. ') if s and not s.endswith('.'))})
    result['vector_content_versions']=sorted({v for txt in values(raw.get('vectors',{})) for v,src in enumerate(SOURCES) if src in txt or src.split('. ')[0]+'.' in txt or src.split('. ')[1] in txt})
    result['scope']='Read-only multi-interface sample interval; inventories are not a global atomic snapshot or service success'
    return result
def audit(out,events,reads,writes,phases,meta):
    checks=[]
    def check(name,ok,detail=None):checks.append(dict(meta,check=name,pass_check=bool(ok),detail=detail))
    req=get(out/'REQUEST.json');fault=get(out/'FAULT.json');result=get(out/'RESULT.json')
    check('request_identity',req.get('unit_id')==meta['unit_id'])
    check('guard_activation',get(out/'GUARD_ACTIVE.json').get('connect_module')=='sitecustomize')
    check('no_provider_invocations',not any(e['event']=='provider.denied' for e in events))
    check('fault_at_most_once',fault.get('fired',0)<=1)
    check('fault_only_target',all(e.get('operation')=='target_v1' for e in events if e['event']=='fault.fired'))
    check('all_fired_prerequisites',all(all(e.get('prerequisites',{}).values()) for e in events if e['event']=='fault.site_reached' and e.get('target_operation')))
    check('controls_never_injected',meta['fault_condition']!='NO_FAULT' or fault.get('fired',0)==0)
    check('hook_restored',any(e['event']=='fault.hooks_restored' for e in events) or not fault)
    for r in reads:
        check('read_not_early:'+r['id'],r.get('actual_enqueue_ns',0)>=r.get('target_ns',0))
        if r.get('service')=='UNAVAILABLE_DIRTY':check('dirty_not_published:'+r['id'],not r.get('published_native'))
        if r.get('service')=='RETURNED':check('publication_deadline:'+r['id'],r.get('published_at_ns',0)<=r.get('deadline_ns',0))
    for w in writes:
        args=w.get('arguments')
        if args:
            check('update_arguments:'+w['id'],args.get('data')==SOURCES[1] and (args.get('chunk_level_diff') is False if w['id']=='rebuild_v1' else 'chunk_level_diff' not in args))
    for e in events:
        if e['event']=='recovery.trigger':
            check('recovery_after_script_and_observer:'+e['recovery'],e['actual_ns']>=max(e['earliest_ns'],e['safe_ready_ns']))
    if result.get('execution')=='TERMINATED_FULL_SEQUENCE':
        check('full_sequence_counts',len([r for r in reads if r.get('category') in PHASES])==15 and len(writes)==3 and len(phases)==3)
        for p in phases:
            cp=get(out/(p['phase']+'.checkpoint.json'))
            phase_reads=[r for r in reads if r.get('category')==p['phase']]
            check('checkpoint_after_service:'+p['phase'],bool(cp) and all(cp.get('started_ns',0)>=r.get('native_terminated_ns',r.get('published_at_ns',0)) for r in phase_reads))
    return checks
def analyze():
    analysis=ROOT/'analysis';analysis.mkdir(exist_ok=True)
    matrix=list(csv.DictReader((ROOT/'MATRIX.csv').open(encoding='utf-8',newline='')))
    assert len(matrix)==160 and len({r['unit_id'] for r in matrix})==160
    probes=[];updates=[];runs=[];checkpoints=[];checks=[];attempt_rows=[]
    for c in matrix:
        meta={k:c[k] for k in ('unit_id','history_id','site_id','fault_condition','policy')}
        out=available_attempt(c['unit_id']);scored=[];writes=[];events=[];phases=[];fault={};result={}
        if out:
            scored,idx=score_attempt(out);writes=get(out/'WRITES.json',[]);events=list(jsonlines(out/'events.jsonl'));phases=get(out/'PHASES.json',[]);fault=get(out/'FAULT.json');result=get(out/'RESULT.json')
            checks+=audit(out,events,scored,writes,phases,meta)
            checkpoint_map={p:checkpoint_row(out,p,meta) for p in PHASES}
            checkpoints+=list(checkpoint_map.values())
            for r in scored:
                for call in r.get('native_calls',[]):attempt_rows.append(dict(meta,read_id=r['id'],phase=r.get('category'),**call))
        else:checkpoint_map={}
        byid={r['id']:r for r in scored}
        for phase in PHASES:
            cp=checkpoint_map.get(phase,{})
            for j,offset in enumerate(OFFSETS):
                r=byid.get(phase+'_'+str(j),{});kind=classify(r)
                item=dict(meta,phase=phase,probe=j,offset_s=offset,executed=bool(r),service=r.get('service','NOT_EXECUTED'),outcome=kind,usable=bool(r.get('usable_by_deadline')) if r else None,conflict=r.get('direct_conflict',False) if r else None,partial=not r.get('coverage_complete',False) if r.get('service')=='RETURNED' else False if r else None,unknown=r.get('compatibility')=='UNKNOWN' if r else None,dirty=r.get('service')=='UNAVAILABLE_DIRTY' if r else None,native_error=r.get('service')=='NATIVE_ERROR' if r else None,deadline_or_exhausted=r.get('service') in ['READ_DEADLINE','VALIDATION_EXHAUSTED'] if r else None,coverage_complete=r.get('coverage_complete'),compatibility=r.get('compatibility'),versions=r.get('versions'),roles=r.get('roles'),mapping_errors=r.get('mapping_errors'),canonical_version=cp.get('canonical_version'),health_before=r.get('health_before'),health_after=r.get('health_after'),native_calls=len(r.get('native_calls',[])),discarded=r.get('epoch_discarded',0))
                versions=r.get('versions',[])
                item['matches_requested_v1']=versions==[1] if r.get('service')=='RETURNED' else None
                item['matches_canonical']=versions==[cp['canonical_version']] if r.get('service')=='RETURNED' and cp.get('canonical_version') is not None else None
                for k in ['target_ns','actual_enqueue_ns','deadline_ns','published_at_ns','native_terminated_ns']:item[k]=r.get(k)
                item.update(latency_ms=duration_ms(r,'published_at','target'),body_ms=duration_ms(r,'body_finished','dispatcher_start'),queue_ms=duration_ms(r,'dispatcher_start','actual_enqueue'),epoch_wait_ms=r.get('epoch_wait_ns',0)/1e6)
                probes.append(item)
        for name,phase in [('target_v1','S0'),('retry_v1','S1'),('rebuild_v1','S2')]:
            w=next((r for r in writes if r['id']==name),{});ev=[e for e in events if e.get('operation')==name];branches=[e.get('branch') for e in ev if e['event']=='path.branch_entered']
            path='incremental_then_fallback' if 'incremental' in branches and 'full_rebuild' in branches else 'incremental' if 'incremental' in branches else 'full_rebuild' if 'full_rebuild' in branches else 'unchanged' if w.get('returned_status')=='unchanged' else 'unknown'
            cp=checkpoint_map.get(phase,{})
            end=next((e['monotonic_ns'] for e in reversed(ev) if e['event'] in ['api.return','api.error'] and e.get('boundary')==name),None)
            native_ms=(end-w['body_start_ns'])/1e6 if end and w.get('body_start_ns') else None
            phaseprobes=[r for r in probes if r['unit_id']==meta['unit_id'] and r['phase']==phase]
            full=len(phaseprobes)==5 and all(r['executed'] for r in phaseprobes)
            trigger=next((e for e in events if e['event']=='recovery.trigger' and e.get('recovery')==('R1' if name=='retry_v1' else 'R2')), {}) if name!='target_v1' else {}
            updates.append(dict(meta,operation=name,phase=phase,executed=bool(w),service=w.get('service','NOT_EXECUTED'),returned_status=w.get('returned_status'),native_success=w.get('native_success',False) if w else None,error=w.get('error'),actual_path=path,branch_sequence=branches,fallback_reason=w.get('fallback_reason'),fallback_detail=w.get('fallback_detail'),arguments=w.get('arguments'),canonical_version=cp.get('canonical_version'),canonical_target_match=cp.get('canonical_target_match'),all_five_usable=all(r['usable'] for r in phaseprobes) if full else None,success_without_service_recovery=bool(w.get('native_success')) and not all(r['usable'] for r in phaseprobes) if full else None,health_before=w.get('health_before'),health_after=w.get('health_after'),native_body_ms=native_ms,queue_ms=duration_ms(w,'body_start','actual_submit'),native_and_drain_ms=duration_ms(w,'actual_quiescent','body_start'),script_delay_s=15 if trigger else None,observer_safety_extra_ms=max(0,trigger.get('safe_ready_ns',0)-trigger.get('earliest_ns',0))/1e6 if trigger else None,script_wait_after_observer_ms=max(0,trigger.get('earliest_ns',0)-trigger.get('safe_ready_ns',0))/1e6 if trigger else None,actual_submit_ns=w.get('actual_submit_ns'),actual_quiescent_ns=w.get('actual_quiescent_ns'),deadline_ns=w.get('deadline_ns'),logical_deadline_ns=w.get('deadline_seen_ns'),terminated_ns=w.get('native_terminated_ns')))
        target=updates[-3];hit=fault.get('fired',0)>0
        status='NOT_EXECUTED' if not out else 'NATIVE_WORK_UNRESOLVED' if result.get('execution')=='NATIVE_WORK_UNRESOLVED' else 'FAULT_FIRED_NATIVE_HANDLED_SUCCESS' if hit and target.get('native_success') else 'FAULT_FIRED_NATIVE_FAILED' if hit else 'NO_FAULT_SUCCESS' if c['fault_condition']=='NO_FAULT' and target.get('native_success') else 'NO_FAULT_NATIVE_FAILED' if c['fault_condition']=='NO_FAULT' else 'SITE_NOT_REACHED'
        runs.append(dict(meta,attempt=out.name if out else None,execution=result.get('execution','NOT_EXECUTED'),outcome=status,fault_fired=fault.get('fired',0),site_reached=any(e['event']=='fault.site_reached' and e.get('target_operation') for e in events),actual_target_path=target['actual_path'],native_error_handler_events=sum(e['event']=='inc.native_error_handler.begin' for e in events),provider_denials=sum(e['event']=='provider.denied' for e in events),remote_denials=len(list(jsonlines(out/'network_guard.jsonl'))) if out and (out/'network_guard.jsonl').exists() else 0,planned_service_probes=15,executed_service_probes=sum(r['unit_id']==meta['unit_id'] and r['executed'] for r in probes),study_updates=len(writes),raw_seal_sha256=sha(out/'RAW_SEAL.json') if out else None))
    for name,rows in [('run_outcomes',runs),('update_and_recovery_paths',updates),('probe_outcomes',probes),('checkpoint_states',checkpoints),('machine_checks',checks),('native_attempts',attempt_rows)]:csvout(analysis/(name+'.csv'),rows)
    summaries=[];histories=[]
    groups=defaultdict(list)
    for r in probes:groups[tuple(r[k] for k in ['site_id','fault_condition','policy','phase'])].append(r)
    for key,rows in sorted(groups.items()):
        summary=dict(zip(['site_id','fault_condition','policy','phase'],key));summary.update(planned=len(rows),executed=sum(r['executed'] for r in rows),outcomes=dict(Counter(r['outcome'] for r in rows)))
        summary.update({m:sum(bool(r[m]) for r in rows) for m in METRICS})
        for h in sorted({r['history_id'] for r in rows}):
            hr=[r for r in rows if r['history_id']==h];full=len(hr)==5 and all(r['executed'] for r in hr)
            histories.append(dict(zip(['site_id','fault_condition','policy','phase'],key),history_id=h,complete_phase=full,all_five_usable=all(r['usable'] for r in hr) if full else None,**{m:sum(bool(r[m]) for r in hr)/5 if full else None for m in METRICS}))
        summary['complete_histories']=sum(h['complete_phase'] for h in histories if all(h[k]==summary[k] for k in ['site_id','fault_condition','policy','phase']))
        summary['all_five_usable_histories']=sum(h['all_five_usable'] is True for h in histories if all(h[k]==summary[k] for k in ['site_id','fault_condition','policy','phase']))
        summaries.append(summary)
    csvout(analysis/'stage_summary.csv',summaries);csvout(analysis/'history_rates.csv',histories)
    pairs=paired(histories);csvout(analysis/'paired_summary.csv',pairs)
    counterexamples=[r for r in probes if r['executed'] and (not r['usable'] or r.get('matches_requested_v1') is False or r.get('matches_canonical') is False)]
    csvout(analysis/'counterexample_index.csv',counterexamples)
    save(analysis/'ANALYSIS.json',{'at':utc(),'planned_units':160,'terminated_units':sum(r['execution']=='TERMINATED_FULL_SEQUENCE' for r in runs),'unresolved_units':sum(r['execution']=='NATIVE_WORK_UNRESOLVED' for r in runs),'planned_probes':2400,'executed_probes':sum(r['executed'] for r in probes),'study_updates':sum(r['study_updates'] for r in runs),'fault_hits':sum(r['fault_fired'] for r in runs),'failed_machine_checks':[r for r in checks if not r['pass_check']],'bootstrap_resamples':10000,'bootstrap_seed':2026111302,'counterexample_probes':len(counterexamples),'scope':'Mechanism repetitions, sequential recovery, descriptive paired history intervals; missing stages are not failures.'})
    print(json.dumps(get(analysis/'ANALYSIS.json'),ensure_ascii=False))
def paired(histories):
    import numpy as np
    rng=np.random.default_rng(2026111302);out=[]
    for site,fault,phase in itertools.product(['P1','P2'],['INJECT','NO_FAULT'],PHASES):
        records=[r for r in histories if (r['site_id'],r['fault_condition'],r['phase'])==(site,fault,phase)]
        for left,right in itertools.combinations(POLICIES,2):
            a={r['history_id']:r for r in records if r['policy']==left and r['complete_phase']}
            b={r['history_id']:r for r in records if r['policy']==right and r['complete_phase']}
            ids=sorted(a.keys()&b.keys());n=len(ids);draw=rng.integers(n,size=(10000,n)) if n else None
            for m in METRICS:
                delta=np.array([b[h][m]-a[h][m] for h in ids],dtype=float)
                low,high=np.quantile(delta[draw].mean(axis=1),[.025,.975]) if n else (None,None)
                out.append(dict(site_id=site,fault_condition=fault,phase=phase,contrast=right+' minus '+left,metric=m,n_paired_histories=n,planned_histories=10,mean_difference=float(delta.mean()) if n else None,ci95_low=float(low) if n else None,ci95_high=float(high) if n else None,resamples=10000,seed=2026111302,interval='Descriptive history paired percentile bootstrap; no equivalence/zero-risk claim'))
    return out
if __name__=='__main__':analyze()
