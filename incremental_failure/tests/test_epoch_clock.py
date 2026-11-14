import asyncio,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from src.clock import now_ns,wait_until_ns
from src.epoch import EpochState
from src.coordinator import DirtyUnavailable
from src.materialize import materialize
from src.native import Session,native_success
from src.common import ROOT

class ClockTests(unittest.IsolatedAsyncioTestCase):
 async def test_early_and_late_wakeup(self):
  times=[100,105,119,120];sleeps=[]
  async def sleep(d):sleeps.append(d)
  self.assertEqual(await wait_until_ns(120,lambda:times.pop(0),sleep),120)
  self.assertEqual(len(sleeps),3);self.assertTrue(all(x>=.001 for x in sleeps))
  self.assertEqual(await wait_until_ns(10,lambda:30,sleep),30)
 async def test_type_and_cancel(self):
  with self.assertRaises(TypeError):await wait_until_ns(1.2)
  task=asyncio.create_task(wait_until_ns(now_ns()+10**12));await asyncio.sleep(0);task.cancel()
  with self.assertRaises(asyncio.CancelledError):await task

class EpochTests(unittest.IsolatedAsyncioTestCase):
 async def run_read(self,state,operation):
  return await state.read_validated('r',operation,now_ns()+3*10**9)
 async def test_stable_read_and_error(self):
  e=EpochState()
  for status in ['RETURNED','NATIVE_ERROR']:
   async def call(i):return {'name':f'r.n{i+1}','status':status}
   result,c=await self.run_read(e,call);self.assertEqual(result,status)
   self.assertEqual(c['validation'],'ACCEPTED' if status=='RETURNED' else 'STABLE_NATIVE_ERROR')
 async def test_writer_does_not_wait_optimistic_body(self):
  e=EpochState();entered=asyncio.Event();release=asyncio.Event();calls=[]
  async def call(i):
   calls.append(i)
   if i==0:entered.set();await release.wait()
   return {'name':f'r.n{i+1}','status':'RETURNED'}
  task=asyncio.create_task(self.run_read(e,call));await entered.wait()
  token=await asyncio.wait_for(e.begin_write('w'),.1);await e.finish_write('w',token,True)
  self.assertFalse(task.done());release.set();result,c=await task
  self.assertEqual(calls,[0,1]);self.assertEqual(c['epoch_before']['epoch'],2)
 async def test_wait_during_write_and_two_writes(self):
  e=EpochState();one=await e.begin_write('w1');calls=[]
  async def call(i):calls.append(i);return {'name':'r.n1','status':'RETURNED'}
  task=asyncio.create_task(self.run_read(e,call));await e.waiting.wait();self.assertFalse(calls)
  await e.finish_write('w1',one,True);two=await e.begin_write('w2');await e.finish_write('w2',two,True)
  _,c=await task;self.assertEqual(c['epoch_before']['epoch'],4)
 async def test_crossing_error_is_discarded(self):
  e=EpochState();calls=[]
  async def call(i):
   c={'name':f'r.n{i+1}','status':'NATIVE_ERROR' if i==0 else 'RETURNED'};calls.append(c)
   if i==0:t=await e.begin_write('w');await e.finish_write('w',t,True)
   return c
  result,c=await self.run_read(e,call);self.assertEqual(result,'RETURNED');self.assertEqual(calls[0]['validation'],'DISCARDED')
 async def test_materialization_change_is_detected(self):
  e=EpochState();calls=[]
  async def lazy():
   t=await e.begin_write('w');await e.finish_write('w',t,True);yield {'full':'public'}
  async def call(i):
   payload=await materialize(lazy()) if i==0 else await materialize([{'full':'public'}])
   c={'name':f'r.n{i+1}','status':'RETURNED','payload':payload};calls.append(c);return c
  _,c=await self.run_read(e,call);self.assertEqual(len(calls),2);self.assertEqual(calls[0]['validation'],'DISCARDED')
 async def test_failed_exception_dirty_and_recovery_generation(self):
  e=EpochState();old=await e.begin_write('old');await e.finish_write('old',old,False)
  with self.assertRaises(DirtyUnavailable):await e.stable('r',now_ns()+10**9)
  with self.assertRaises(DirtyUnavailable):await e.begin_write('not-recovery')
  token=await e.begin_write('recovery',True);self.assertGreater(token,old)
  await e.finish_write('old',old,True);self.assertEqual(e.health,'WRITING')
  await e.finish_write('recovery',token,True);self.assertEqual(e.health,'READY')
 async def test_timeout_late_success_stays_dirty(self):
  e=EpochState();token=await e.begin_write('w');await e.dirty('timeout');await e.finish_write('w',token,True)
  self.assertEqual(e.health,'DIRTY')
 async def test_eight_attempts_exhausted_no_ninth(self):
  e=EpochState();calls=[]
  async def call(i):
   c={'name':f'r.n{i+1}','status':'RETURNED'};calls.append(c);t=await e.begin_write(f'w{i}');await e.finish_write(f'w{i}',t,True);return c
  status,c=await self.run_read(e,call);self.assertEqual(status,'VALIDATION_EXHAUSTED');self.assertIsNone(c);self.assertEqual(len(calls),8)
 async def test_dirty_after_candidate_cannot_publish(self):
  e=EpochState();calls=[]
  async def call(i):
   c={'name':'r.n1','status':'RETURNED'};calls.append(c);await e.dirty('failed write');return c
  with self.assertRaises(DirtyUnavailable):await self.run_read(e,call)
  self.assertEqual(calls[0]['discard_reason'],'DIRTY')

class AdapterTests(unittest.IsolatedAsyncioTestCase):
 async def make(self,policy='EPOCH_VALIDATED',mode='DEFAULT_REQUEST'):
  p=ROOT/'runs/test_records';p.mkdir(parents=True,exist_ok=True)
  out=Path(tempfile.mkdtemp(prefix='adapter_',dir=p));s=Session(out,policy,mode);s.data='data';s.dataset='dataset';return s
 async def test_success_enumeration(self):
  for status in ['incremental','full_rebuild','unchanged']:self.assertTrue(native_success({'status':status,'error':None}))
  self.assertFalse(native_success({'status':'failed'}));self.assertFalse(native_success({'status':'incremental','error':'bad'}))
 async def test_failed_return_exception_and_default_argument(self):
  for result in [{'status':'failed','error':{'message':'failed'}},RuntimeError('native failed')]:
   s=await self.make();seen=[]
   async def update(**kw):
    seen.append(kw)
    if isinstance(result,Exception):raise result
    return result
   s.system=SimpleNamespace(update=update)
   with patch('src.native.importlib.import_module',return_value=SimpleNamespace(_BACKGROUND_PIPELINE_TASKS=set())):
    row=await s.update('w',1)
   self.assertEqual(row['service'],'UPDATE_FAILED');self.assertEqual(s.epoch.health,'DIRTY');self.assertNotIn('chunk_level_diff',seen[0]);await s.close()
 async def test_full_true_and_incremental_success(self):
  for mode,value in [('FORCED_FULL',False),('EXPLICIT_TRUE',True),('DEFAULT_REQUEST',None)]:
   s=await self.make(mode=mode);seen=[]
   async def update(**kw):seen.append(kw);return {'status':'incremental','error':None}
   s.system=SimpleNamespace(update=update)
   with patch('src.native.importlib.import_module',return_value=SimpleNamespace(_BACKGROUND_PIPELINE_TASKS=set())):row=await s.update('w',1)
   self.assertEqual(row['service'],'SUCCESS');self.assertEqual(s.epoch.health,'READY');self.assertEqual(seen[0].get('chunk_level_diff'),value);await s.close()
 async def test_outer_timeout_does_not_release_native_task_or_clear_dirty(self):
  s=await self.make();entered=asyncio.Event();release=asyncio.Event()
  async def update(**kw):entered.set();await release.wait();return {'status':'incremental'}
  s.system=SimpleNamespace(update=update)
  with patch('src.native.importlib.import_module',return_value=SimpleNamespace(_BACKGROUND_PIPELINE_TASKS=set())):
   task=asyncio.create_task(s.update('w',1,deadline=.02));await entered.wait();await asyncio.sleep(.04)
   self.assertEqual(s.epoch.health,'DIRTY');self.assertFalse(task.done());release.set();row=await task
  self.assertEqual(row['service'],'UPDATE_DEADLINE');self.assertEqual(s.epoch.health,'DIRTY');await s.close()
