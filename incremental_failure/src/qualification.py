"""Engineering acceptance checks use receipts, not an effect-size target."""
import json
from .common import *
from .controller_inc import verify
from .content import score_attempt
from .independent_check import crosscheck
from .analyze_inc import audit
from .outcomes import PHASES
def main():
    rows=[]
    for site in ('P1','P2'):
        for fault in ('INJECT','NO_FAULT'):
            unit=f'QUAL_{site}_{fault}_NATIVE';base=ROOT/'engineering'/unit
            attempts=sorted(base.glob('attempt-*'));assert attempts,unit
            out=attempts[-1];complete=verify(out);assert complete['execution']=='TERMINATED_FULL_SEQUENCE',unit
            config=read(out/'REQUEST.json');meta={k:config[k] for k in ('unit_id','history_id','site_id','fault_condition','policy')}
            scored,index=score_attempt(out);events=list(jsonlines(out/'events.jsonl'));writes=read(out/'WRITES.json');phases=read(out/'PHASES.json');f=read(out/'FAULT.json')
            checks=audit(out,events,scored,writes,phases,meta)
            hits=[e for e in events if e['event']=='fault.site_reached' and e.get('target_operation')]
            assert len(hits)==1 and all(hits[0]['prerequisites'].values()),unit
            assert f['fired']==int(fault=='INJECT'),unit
            assert writes[0]['branch_entered']=='incremental',unit
            assert not any(e['event']=='fault.fired' and e.get('operation')!='target_v1' for e in events)
            if fault=='INJECT':
                assert any(e['event']=='inc.native_error_handler.begin' for e in events),unit
                assert not any(e['event']=='inc.canonical_publish.ack' and e.get('operation')=='target_v1' for e in events)
            else:assert writes[0]['native_success'],unit
            assert all(c['pass_check'] for c in checks),[c for c in checks if not c['pass_check']]
            independent=crosscheck(out);assert not independent['disagreements'],independent['disagreements']
            save(ROOT/'reports/engineering_crosschecks'/(unit+'.json'),independent)
            rows.append({'unit':unit,'attempt':out.name,'checks':checks,'raw_seal_sha256':sha(out/'RAW_SEAL.json'),'fired':f['fired'],'native_paths':[w.get('branch_entered') for w in writes],'independent_candidates':independent['public_candidates_checked'],'status':'PASS'})
    save(ROOT/'reports/ENGINEERING_QUALIFICATION.json',{'at':utc(),'qualified_units':len(rows),'rows':rows,'effect_size_gate':False,'source':'Four permitted native engineering units plus local tests'})
    print('Qualified four native engineering units')
if __name__=='__main__':main()
