"""Experimental read-only shared snapshot through public backend connection APIs.

The native retrieval uses parallel connections. Export one PostgreSQL snapshot,
then import it in each recall-scoped REPEATABLE READ transaction. No connection
internals, result filtering, native SQL rewriting, or worker waiting is used.
"""
import contextvars,re,time
from contextlib import asynccontextmanager
from workload.hindsight import Session as NativeSession
from recovery.state import append,atomic

SNAPSHOT=contextvars.ContextVar('paper_patch_snapshot',default=None)

class Session(NativeSession):
    async def open(self):
        await super().open()
        self.backend=await self.engine._get_backend()
        self.original_acquire=self.backend.acquire
        self.snapshot_transactions=[]
        @asynccontextmanager
        async def acquire():
            snapshot=SNAPSHOT.get()
            async with self.original_acquire() as conn:
                if snapshot is None:
                    yield conn
                    return
                if not re.fullmatch(r'[0-9A-Fa-f-]+',snapshot['id']):raise RuntimeError('Unexpected server snapshot identifier')
                began=time.perf_counter_ns()
                async with conn.transaction():
                    await conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                    await conn.execute("SET TRANSACTION SNAPSHOT '"+snapshot['id']+"'")
                    entry=dict(call=snapshot['call'],snapshot_id=snapshot['id'],start_ns=began,public_connection_api=True,
                               isolation='repeatable_read',read_only=True)
                    self.snapshot_transactions.append(entry)
                    yield conn
                entry['end_ns']=time.perf_counter_ns();entry['duration_ms']=(entry['end_ns']-began)/1e6
                append(self.out/'snapshot_transactions.jsonl',entry)
        self.backend.acquire=acquire

    async def capture(self,label='read'):
        begin=time.perf_counter_ns();first=len(self.snapshot_transactions)
        if self.config['variant']=='baseline':
            value=await super().capture(label)
        else:
            async with self.original_acquire() as keeper:
                tx_begin=time.perf_counter_ns()
                async with keeper.transaction():
                    await keeper.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                    snapshot_id=await keeper.fetchval('SELECT pg_export_snapshot()')
                    token=SNAPSHOT.set(dict(id=snapshot_id,call=f'{label}_{self.seq+1:04d}'))
                    try:value=await super().capture(label)
                    finally:SNAPSHOT.reset(token)
                tx_end=time.perf_counter_ns()
            value['snapshot_id']=snapshot_id
            value['snapshot_keeper_duration_ms']=(tx_end-tx_begin)/1e6
        end=time.perf_counter_ns()
        value['patch_request_latency_ms']=(end-begin)/1e6
        value['patch_request_start_ns']=begin;value['patch_request_end_ns']=end
        entries=self.snapshot_transactions[first:]
        value['snapshot_transactions']=entries
        value['snapshot_export_reads']=int(self.config['variant']=='snapshot')
        value['snapshot_control_commands']=2*len(entries)+int(self.config['variant']=='snapshot')
        if self.config['variant']=='snapshot' and (not entries or any(x['snapshot_id']!=value['snapshot_id'] for x in entries)):
            raise RuntimeError('Recall snapshot membership was not established')
        atomic(self.native/(value['name']+'.sidecar.json'),{k:v for k,v in value.items() if k!='native_payload'})
        return value

    async def close(self):
        if hasattr(self,'original_acquire'):self.backend.acquire=self.original_acquire
        await super().close()
