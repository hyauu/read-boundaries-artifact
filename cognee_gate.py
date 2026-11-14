"""Single-process writer-preference async RW gate, with separate health state."""
import asyncio
from contextlib import asynccontextmanager
class DirtyUnavailable(RuntimeError):pass
class RWGate:
 def __init__(self,emit=lambda *a,**k:None):
  self.cv=asyncio.Condition();self.readers=0;self.writer=False;self.waiting_writers=0;self.state='READY';self.emit=emit;self.max_readers=0;self.dirty_epoch=0
 async def dirty(self,reason):
  async with self.cv:
   self.state='DIRTY';self.dirty_epoch+=1;self.emit('health.dirty',reason=reason,epoch=self.dirty_epoch);self.cv.notify_all()
 @asynccontextmanager
 async def read(self,name):
  async with self.cv:
   self.emit('gate.read_requested',name=name)
   while True:
    if self.state=='DIRTY':raise DirtyUnavailable('UNAVAILABLE_DIRTY')
    if not self.writer and not self.waiting_writers:break
    self.emit('gate.read_waiting',name=name)
    await self.cv.wait()
   self.readers+=1;self.max_readers=max(self.max_readers,self.readers)
   self.emit('gate.read_acquired',name=name,active_readers=self.readers)
  try:yield
  finally:
   async with self.cv:
    self.readers-=1;self.emit('gate.read_released',name=name,active_readers=self.readers);self.cv.notify_all()
 @asynccontextmanager
 async def write(self,name,recovery=False):
  async with self.cv:
   self.waiting_writers+=1;self.emit('gate.write_requested',name=name)
   try:
    while self.writer or self.readers:
     self.emit('gate.write_waiting',name=name);await self.cv.wait()
    if self.state=='DIRTY' and not recovery:raise DirtyUnavailable('UNAVAILABLE_DIRTY')
    self.writer=True;self.state='WRITING';epoch=self.dirty_epoch
    self.emit('gate.write_acquired',name=name,recovery=recovery)
   finally:self.waiting_writers-=1;self.cv.notify_all()
  result={'ok':False}
  try:yield result
  except BaseException:
   await self.dirty('write raised or cancelled after admission');raise
  finally:
   async with self.cv:
    if result['ok'] and self.dirty_epoch==epoch:self.state='READY'
    elif self.state!='DIRTY':self.state='DIRTY';self.dirty_epoch+=1
    self.writer=False;self.emit('gate.write_released',name=name,health=self.state);self.cv.notify_all()
