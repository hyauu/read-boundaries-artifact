import unittest,copy,asyncio,tempfile
from pathlib import Path
from types import SimpleNamespace
from src.outcomes import classify,SOURCES,ROLES
from src.fault_hooks import FaultHooks,InjectedIncrementalFailure
from src import native_base as rt
from src.common import ROOT,read
from src.content import ReceiptIndex
from src.independent_check import scan,check_payload
from src.analyze_inc import paired,METRICS
class OutcomeTests(unittest.TestCase):
    def test_nonreturn_never_usable_class(self):
        for status in ['UNAVAILABLE_DIRTY','NATIVE_ERROR','READ_DEADLINE','VALIDATION_EXHAUSTED','NOT_EXECUTED']:
            self.assertEqual(classify({'service':status,'coverage_complete':True}),status)
    def test_priority_retains_conflict_over_missing(self):
        self.assertEqual(classify({'service':'RETURNED','direct_conflict':True,'coverage_complete':False}),'DIRECT_CONFLICT')
        self.assertEqual(classify({'service':'RETURNED','coverage_complete':False,'compatibility':'UNKNOWN'}),'PARTIAL')
        self.assertEqual(classify({'service':'RETURNED','coverage_complete':True,'compatibility':'UNKNOWN'}),'UNKNOWN')
    def test_known_pair_difference_and_missing(self):
        from src.outcomes import POLICIES
        rows=[dict(site_id='P1',fault_condition='INJECT',phase='S0',policy=p,history_id=str(h),complete_phase=True,**{m:float(p!='NATIVE') for m in METRICS}) for p in POLICIES for h in range(10)]
        result=paired(rows)
        r=next(r for r in result if r['contrast']=='COORDINATED minus NATIVE' and r['metric']=='usable' and r['site_id']=='P1' and r['fault_condition']=='INJECT' and r['phase']=='S0')
        self.assertEqual((r['mean_difference'],r['ci95_low'],r['ci95_high']),(1,1,1))
        absent=next(r for r in result if r['site_id']=='P2');self.assertIsNone(absent['mean_difference']);self.assertEqual(absent['n_paired_histories'],0)
class HookTests(unittest.TestCase):
    def session(self):
        self.events=[]
        return SimpleNamespace(observer=SimpleNamespace(emit=lambda event,**kw:self.events.append((event,kw))),hooks=SimpleNamespace(branches={'target_v1':'incremental','retry_v1':'incremental'}))
    def ready(self,h):
        h.state().update(write_entered=True,new_write_acks=1,delete_ids=['c'],delete_started=h.site=='P2',delete_completed=h.site=='P2')
    def test_one_shot_target_and_controls(self):
        for site in ['P1','P2']:
            h=FaultHooks(self.session(),site,True);token=rt.OP.set('target_v1')
            try:
                self.ready(h)
                with self.assertRaises(InjectedIncrementalFailure):h.reach(site)
                h.reach(site);self.assertEqual(h.fired,1)
            finally:rt.OP.reset(token)
            token=rt.OP.set('retry_v1')
            try:h.reach(site);self.assertEqual(h.fired,1)
            finally:rt.OP.reset(token)
            h=FaultHooks(self.session(),site,False);token=rt.OP.set('target_v1')
            try:self.ready(h);h.reach(site);self.assertEqual(h.fired,0)
            finally:rt.OP.reset(token)
    def test_missing_ack_is_harness_error_not_injection(self):
        h=FaultHooks(self.session(),'P1',True);token=rt.OP.set('target_v1')
        try:
            with self.assertRaisesRegex(RuntimeError,'HARNESS_SITE_PRECONDITION_FAILED'):h.reach('P1')
            self.assertEqual(h.fired,0)
        finally:rt.OP.reset(token)
class ReceiptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.out=ROOT/'engineering/QUAL_P1_INJECT_NATIVE/attempt-002'
        if not (cls.out/'COMPLETE.json').exists():raise unittest.SkipTest('qualification receipts not yet sealed')
        cls.idx=ReceiptIndex(cls.out);cls.ind=scan(cls.out)
    def both(self,payload,name):
        a=self.idx.score(payload,name);b=check_payload(payload,name,self.ind)
        for key in ('coverage_complete','direct_conflict','roles','versions','compatibility'):self.assertEqual(a[key],b[key],key)
        return a
    def test_complete_old_new_and_real_conflict(self):
        a=self.both(read(self.out/'responses/initial.n1.json'),'initial.n1')
        self.assertEqual(a['versions'],[0]);self.assertTrue(a['coverage_complete'])
        a=self.both(read(self.out/'responses/S0_0.n1.json'),'S0_0.n1');self.assertTrue(a['direct_conflict'])
        a=self.both(read(self.out/'responses/S1_0.n1.json'),'S1_0.n1');self.assertEqual(a['versions'],[1])
    def test_empty_partial_and_unbound(self):
        a=self.both([],'S1_0.n1');self.assertEqual(a['compatibility'],'UNKNOWN');self.assertFalse(a['coverage_complete'])
        p=read(self.out/'responses/S1_0.n1.json');p[0]['objects_result']['chunk_summaries']={}
        a=self.both(p,'S1_0.n1');self.assertFalse(a['roles']['summary'])
        p=read(self.out/'responses/S1_0.n1.json');p[0]['objects_result']['chunks'][0]['payload']['text']='tampered'
        a=self.both(p,'S1_0.n1');self.assertEqual(a['compatibility'],'UNKNOWN')
    def test_runtime_has_no_gold_gate(self):
        for path in ['epoch.py','coordinator.py','native.py']:
            code=(ROOT/'src'/path).read_text(encoding='utf-8')
            for forbidden in ['ReceiptIndex','coverage_complete','score_attempt']:self.assertNotIn(forbidden,code)
if __name__=='__main__':unittest.main()
