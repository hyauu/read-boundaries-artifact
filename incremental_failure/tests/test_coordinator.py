import asyncio,unittest
from src.coordinator import RWGate,DirtyUnavailable
class GateTests(unittest.IsolatedAsyncioTestCase):
 async def test_readers_share_and_writer_waits(self):
  g=RWGate();release=asyncio.Event();order=[]
  async def r(n):
   async with g.read(n):order.append(n);await release.wait()
  a=asyncio.create_task(r('a'));b=asyncio.create_task(r('b'));await asyncio.sleep(.01)
  self.assertEqual(g.readers,2)
  async def w():
   async with g.write('w') as result:order.append('w');result['ok']=True
  writing=asyncio.create_task(w());await asyncio.sleep(.01)
  late=asyncio.create_task(r('late'));await asyncio.sleep(.01);self.assertEqual(order,['a','b'])
  release.set();await asyncio.gather(a,b,writing,late)
  self.assertEqual(order,['a','b','w','late']);self.assertEqual(g.state,'READY')
 async def test_failed_update_dirty_recovery(self):
  g=RWGate()
  async with g.write('failed'):pass
  with self.assertRaises(DirtyUnavailable):
   async with g.read('r'):pass
  async with g.write('recovery',recovery=True) as result:result['ok']=True
  async with g.read('r'):pass
 async def test_waiter_cancel_does_not_leak(self):
  g=RWGate()
  async with g.read('a'):
   async def waiting():
    async with g.write('w'):pass
   t=asyncio.create_task(waiting());await asyncio.sleep(.01);t.cancel()
   with self.assertRaises(asyncio.CancelledError):await t
   self.assertEqual(g.waiting_writers,0)
  self.assertEqual(g.state,'READY');self.assertEqual(g.readers,0)
 async def test_cancel_active_writer_keeps_dirty(self):
  g=RWGate();started=asyncio.Event()
  async def writing():
   async with g.write('w') as result:started.set();await asyncio.sleep(10);result['ok']=True
  t=asyncio.create_task(writing());await started.wait();t.cancel()
  with self.assertRaises(asyncio.CancelledError):await t
  self.assertFalse(g.writer);self.assertEqual(g.state,'DIRTY')
 async def test_late_success_does_not_clear_timeout_dirty(self):
  g=RWGate()
  async with g.write('w') as result:await g.dirty('deadline');result['ok']=True
  self.assertEqual(g.state,'DIRTY')
if __name__=='__main__':unittest.main()
