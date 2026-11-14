"""Single-owner service epoch. No gold/checker/cache or product modifications."""
import asyncio
from .clock import now_ns
from .coordinator import DirtyUnavailable

class EpochState:
    def __init__(self, emit=lambda *a, **k: None):
        self.cv = asyncio.Condition()
        self.epoch = 0
        self.health = 'READY'
        self.operation_id = None
        self.emit = emit
        self.invalidated = asyncio.Event()
        self.waiting = asyncio.Event()

    def value(self):
        # Called only while holding cv, all transitions in this event loop.
        return {'epoch': self.epoch, 'health': self.health, 'operation_id': self.operation_id}

    async def snapshot(self):
        async with self.cv:
            return self.value()

    async def begin_write(self, operation_id, recovery=False):
        async with self.cv:
            if self.health == 'DIRTY' and not recovery:
                raise DirtyUnavailable('UNAVAILABLE_DIRTY')
            if self.health == 'WRITING':
                raise RuntimeError('Concurrent writer outside common FIFO')
            self.epoch += 1 if self.epoch % 2 == 0 else 2
            self.health = 'WRITING'
            self.operation_id = operation_id
            token = self.epoch
            self.emit('epoch.write_begin', recovery=recovery, **self.value())
            self.cv.notify_all()
            return token

    async def finish_write(self, operation_id, token, success):
        async with self.cv:
            # Timeout/cancellation DIRTY is sticky. A late success cannot clear
            # it, and an obsolete operation cannot finish a new generation.
            same = self.operation_id == operation_id and self.epoch == token
            if same and self.health == 'WRITING' and success:
                self.epoch += 1
                self.health = 'READY'
            elif same:
                self.health = 'DIRTY'
            self.emit('epoch.write_end', success=success, token=token, same_operation=same, **self.value())
            self.cv.notify_all()

    async def dirty(self, reason):
        async with self.cv:
            self.health = 'DIRTY'
            self.emit('epoch.dirty', reason=reason, **self.value())
            self.cv.notify_all()

    async def stable(self, name, deadline_ns):
        async with self.cv:
            while True:
                if self.health == 'DIRTY':
                    raise DirtyUnavailable('UNAVAILABLE_DIRTY')
                if now_ns() >= deadline_ns:
                    raise TimeoutError('Logical read deadline while waiting for epoch')
                if self.health == 'READY' and self.epoch % 2 == 0:
                    value = self.value()
                    self.emit('epoch.read_before', name=name, **value)
                    return value
                self.waiting.set()
                self.emit('epoch.waiting_for_stable', name=name, **self.value())
                try:
                    await asyncio.wait_for(self.cv.wait(), max(.001,(deadline_ns-now_ns())/1e9))
                except asyncio.TimeoutError:
                    # Recheck the actual clock; an early timer is not a deadline.
                    pass

    async def read_validated(self, name, attempt, deadline_ns, attempts=8):
        if attempts != 8:
            raise ValueError('The frozen limit is eight native attempts')
        for index in range(attempts):
            waiting_begin = now_ns()
            before = await self.stable(name, deadline_ns)
            waiting_end = now_ns()
            if waiting_end >= deadline_ns:
                return 'READ_DEADLINE', None
            # attempt must await native I/O AND recursive materialization.
            call = await attempt(index)
            call['epoch_wait_start_ns'] = waiting_begin
            call['epoch_wait_end_ns'] = waiting_end
            call['epoch_wait_ns'] = waiting_end - waiting_begin
            after = await self.snapshot()
            call['epoch_before'] = before
            call['epoch_after'] = after
            self.emit('epoch.read_after', name=name, native_name=call['name'], before=before, after=after)
            stable = after['health']=='READY' and before['epoch']==after['epoch'] and after['epoch']%2==0
            if stable:
                call['validation'] = 'ACCEPTED' if call['status']=='RETURNED' else 'STABLE_NATIVE_ERROR'
                self.emit('epoch.accepted', name=name, native_name=call['name'], outcome=call['validation'], epoch=after['epoch'])
                return call['status'], call
            call['validation'] = 'DISCARDED'
            call['discard_reason'] = 'DIRTY' if after['health']=='DIRTY' else ('WRITING' if after['health']=='WRITING' else 'EPOCH_CHANGED')
            self.invalidated.set()
            self.emit('epoch.invalidated', name=name, native_name=call['name'], reason=call['discard_reason'], before=before, after=after)
            if after['health']=='DIRTY':
                raise DirtyUnavailable('UNAVAILABLE_DIRTY')
            if now_ns()>=deadline_ns:
                return 'READ_DEADLINE', None
        self.emit('epoch.exhausted', name=name, native_attempts=attempts)
        return 'VALIDATION_EXHAUSTED', None
