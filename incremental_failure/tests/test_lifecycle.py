import asyncio,tempfile,unittest
from src.common import ROOT
from src.native import Session
class LifecycleTests(unittest.IsolatedAsyncioTestCase):
 def session(self,policy):
  path=ROOT/'runs/test_records';path.mkdir(exist_ok=True)
  from pathlib import Path
  return Session(Path(tempfile.mkdtemp(prefix='lifecycle_',dir=path)),policy)
 async def test_read_deadline_retains_native_and_lease(self):
  s=self.session('COORDINATED');started=asyncio.Event();release=asyncio.Event()
  async def fake(name):started.set();await release.wait();return []
  s.native_read=fake;t=asyncio.create_task(s.read('test',deadline=.02));await started.wait();await asyncio.sleep(.03)
  self.assertEqual(s.gate.readers,1);self.assertFalse(t.done());release.set();row=await t
  self.assertEqual(row['service'],'READ_DEADLINE');self.assertEqual(s.gate.readers,0);await s.close()
 async def test_cancel_outer_read_retains_native(self):
  s=self.session('COORDINATED');started=asyncio.Event();release=asyncio.Event()
  async def fake(name):started.set();await release.wait();return []
  s.native_read=fake;t=asyncio.create_task(s.read('test'));await started.wait();t.cancel()
  with self.assertRaises(asyncio.CancelledError):await t
  self.assertEqual(s.gate.readers,1);self.assertTrue(s.tasks);release.set();await s.close();self.assertEqual(s.gate.readers,0)
 async def test_retry_after_terminated_error_only_publishes_second(self):
  s=self.session('DOUBLE_READ');names=[]
  async def fake(name):
   names.append(name)
   if len(names)==1:raise ValueError('first terminated error')
   return ['second']
  s.native_read=fake;r=await s.read('test')
  self.assertEqual(names,['test.n1','test.n2']);self.assertEqual(r['published_native'],'test.n2');self.assertEqual(r['service'],'RETURNED');await s.close()
 async def test_retry_does_not_start_second_after_deadline(self):
  s=self.session('DOUBLE_READ');names=[]
  async def fake(name):names.append(name);await asyncio.sleep(.04);return []
  s.native_read=fake;r=await s.read('test',deadline=.01)
  self.assertEqual(names,['test.n1']);self.assertEqual(r['service'],'READ_DEADLINE');await s.close()
 async def test_writer_timeout_retains_dirty_after_late_success(self):
  s=self.session('COORDINATED')
  async def fake(row,v,recovery):
   async with s.gate.write(row['id']) as lease:
    import time
    row['body_start']=time.perf_counter();await asyncio.sleep(.04);lease['ok']=True
   return 'SUCCESS'
  s._update_body=fake;r=await s.update('test',1,deadline=.01)
  self.assertEqual(r['service'],'UPDATE_DEADLINE');self.assertEqual(s.gate.state,'DIRTY');await s.close()
