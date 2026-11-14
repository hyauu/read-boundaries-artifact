"""Pinned native APIs; fixture only replaces extraction/embedding, never stores."""
import asyncio,contextlib,importlib,os,time,sys
from collections import Counter
from . import native_base as rt
from .common import *
from .coordinator import RWGate,DirtyUnavailable

CITIES=['Seattle','San Francisco','Boston','Denver','Austin','Portland']
COMPANIES=['Company X','Company Y','Company Z','Company Q','Company R','Company S']
def source(v):return f'Alice lives in {CITIES[v]}. Alice works at {COMPANIES[v]}.'

class Observer(rt.Observer):
 def __init__(self,out):
  super().__init__(out);self.counts=Counter();self.operation_counts={};self.stream=(out/'events.jsonl').open('a',encoding='utf-8',buffering=1)
 def emit(self,event,**data):
  self.seq+=1;self.counts[event]+=1
  value=dict(seq=self.seq,utc=utc(),monotonic_ns=time.perf_counter_ns(),operation=rt.OP.get(),event=event,**rt.plain(data))
  self.stream.write(json.dumps(value,ensure_ascii=False)+'\n')
  if event=='source.publish':self.last_canonical_source={k:value.get(k) for k in ['seq','data_id','dataset_id','text','sha256','content_hash']}
  return value

def install_fixture(observer):
 from cognee.infrastructure.llm.LLMGateway import LLMGateway
 from cognee.shared.data_models import KnowledgeGraph,Node,Edge,SummarizedContent
 from cognee.infrastructure.databases.vector.embeddings.LiteLLMEmbeddingEngine import LiteLLMEmbeddingEngine
 import litellm
 def denied(*args,**kwargs):
  observer.emit('provider.denied');raise RuntimeError('Paid or unrecognized model path forbidden')
 async def adenied(*args,**kwargs):return denied(*args,**kwargs)
 for name in ['completion','embedding','responses']:setattr(litellm,name,denied)
 for name in ['acompletion','aembedding','aresponses']:setattr(litellm,name,adenied)
 async def structured(text_input,system_prompt,response_model,**kwargs):
  observer.emit('fixture.ai_input',response_model=getattr(response_model,'__name__',str(response_model)),text_input=text_input)
  text=str(text_input);matches=[v for v in range(len(CITIES)) if source(v) in text]
  if response_model is str and text=='test':return 'offline connectivity fixture'
  if len(matches)!=1:raise RuntimeError('Unrecognized deterministic source input')
  v=matches[0];city=CITIES[v];company=COMPANIES[v]
  if isinstance(response_model,type) and issubclass(response_model,KnowledgeGraph):
   return KnowledgeGraph(nodes=[Node(id='Alice',name='Alice',type='Person',description='Alice'),Node(id=city,name=city,type='City',description=city),Node(id=company,name=company,type='Company',description=company)],edges=[Edge(source_node_id='Alice',target_node_id=city,relationship_name='lives_in',description=f'Alice lives in {city}.'),Edge(source_node_id='Alice',target_node_id=company,relationship_name='works_at',description=f'Alice works at {company}.')])
  if isinstance(response_model,type) and issubclass(response_model,SummarizedContent):return SummarizedContent(summary=source(v),description='')
  return denied()
 async def embed(self,texts):
  observer.emit('fixture.embedding',texts=texts);return [[1.0]+[0.0]*7 for _ in texts]
 LLMGateway.acreate_structured_output=staticmethod(structured)
 for name in ['create_transcript','transcribe_image']:setattr(LLMGateway,name,staticmethod(adenied))
 LiteLLMEmbeddingEngine.embed_text=embed

class Hooks:
 def __init__(self,obs):
  self.obs=obs;self.kind=None;self.target_read=None;self.target_write=None;self.read_reached=asyncio.Event();self.passage=asyncio.Event();self.write_reached=asyncio.Event();self.release_read=asyncio.Event();self.release_write=asyncio.Event();self.read_held=False;self.write_held=False;self.first_change=False;self.fault=False;self.changed_operations=set()
 async def lane_return(self,operation,result):
  if operation==self.target_read:self.passage.set()
 async def before(self,backend,method,args,kwargs):
  op=rt.OP.get()
  if self.kind=='AR' and op==self.target_read and backend=='graph' and method=='get_neighborhood' and not self.read_held:
   self.read_held=True;await self.passage.wait();self.obs.emit('schedule.read_held');self.read_reached.set();await self.release_read.wait();self.obs.emit('schedule.read_released')
  if self.kind=='AR' and op==self.target_write and backend=='vector' and method=='index_data_points' and args[:2]==('EdgeType','relationship_name') and not self.write_held:
   self.write_held=True;self.obs.emit('schedule.writer_publication_held');self.write_reached.set();await self.release_write.wait();self.obs.emit('schedule.writer_released')
 async def after(self,backend,method,args,kwargs,result):
  op=rt.OP.get()
  if backend=='vector' and method=='delete_data_points' and len(args)>1 and args[1] and op not in self.changed_operations:
   self.changed_operations.add(op);self.obs.emit('update.first_visible_change_ack',backend=backend,method=method,collection=args[0],ids=args[1])
   if op!=self.target_write:return
   self.first_change=True
   if self.fault:
    self.fault=False;self.obs.emit('injection.once',point='after_first_nonempty_vector_delete_ack');raise RuntimeError('INJECTED_AFTER_FIRST_VECTOR_DELETE_ACK')
   if self.kind=='AW':
    self.write_held=True;self.write_reached.set();self.obs.emit('schedule.writer_first_change_held');await self.release_write.wait();self.obs.emit('schedule.writer_released')
 def release(self):self.release_read.set();self.release_write.set()

class _LegacySession:
 def __init__(self,out,strategy):
  self.out=out;self.strategy=strategy;self.observer=Observer(out);self.hooks=Hooks(self.observer);self.gate=RWGate(self.observer.emit);self.head=0;self.reads=[];self.writes=[];self.tasks=set();self.read_counter=0;self.write_lock=asyncio.Lock()
 async def open(self):
  self.system=rt.configure(self.out);install_fixture(self.observer);self.observer.install();self.observer.gate=self.hooks
  from importlib.metadata import version
  import cognee
  actual=Path(cognee.__file__).resolve();expected=PROJECT/'replication/.venv/Lib/site-packages/cognee/__init__.py'
  assert actual==expected.resolve(),str(actual)
  save(self.out/'runtime_version.json',{'python':sys.version,'cognee_file':str(actual),'cognee':version('cognee'),'lancedb':version('lancedb'),'ladybug':version('ladybug'),'aiosqlite':version('aiosqlite')})
  await rt.native(self.observer,'setup_add',self.system.add(source(0),dataset_name='coordination'))
  from cognee.modules.users.methods import get_default_user
  from cognee.modules.data.methods.get_datasets import get_datasets
  from cognee.modules.data.methods.get_dataset_data import get_dataset_data
  datasets=await get_datasets((await get_default_user()).id);assert len(datasets)==1
  self.dataset=datasets[0].id;rows=await get_dataset_data(self.dataset);assert len(rows)==1;self.data=rows[0].id
  save(self.out/'identity.json',{'dataset_id':str(self.dataset),'data_id':str(self.data)})
  await rt.record_source(self.observer,self.dataset)
  result=await rt.native(self.observer,'setup_cognify',self.system.cognify(datasets=['coordination'],chunk_size=512))
  save(self.out/'initialization.json',rt.plain(result))
  return self
 async def native_read(self,name):
  from cognee.api.v1.search import SearchType
  raw=await rt.native(self.observer,name,self.system.search(query_text='Where does Alice live and where does Alice work?',query_type=SearchType.HYBRID_COMPLETION,dataset_ids=[self.dataset],only_context=True,verbose=True,include_references=True,top_k=20,retriever_specific_config={'chunks_top_k':10,'entities_top_k':10,'max_edges_per_entity':20,'facts_top_k':30}))
  return rt.plain(raw)
 async def _read_body(self,row):
  row['dispatcher_start']=time.perf_counter();row['head_at_start']=self.head
  name=row['id'];calls=[];row['native_calls']=calls
  row['lease_requested']=time.perf_counter()
  async with self.gate.read(name) if self.strategy=='COORDINATED' else contextlib.nullcontext():
   if self.strategy=='ENTRY_ONLY':
    async with self.gate.read(name):pass
   row['lease_acquired']=time.perf_counter();last=None;error=None
   for index in range(2 if self.strategy=='RETRY_ONCE' else 1):
    # No second call after the logical deadline, and no overlapping retries.
    if time.perf_counter()>=row['deadline_at']:break
    call={'name':name+f'.n{index+1}','started':time.perf_counter()};calls.append(call)
    try:
     last=await self.native_read(call['name']);error=None;call['status']='RETURNED'
     call['payload']=last
    except Exception as exc:
     last=None;error=repr(exc);call['status']='NATIVE_ERROR';call['error']=error
    call['ended']=time.perf_counter()
   row['materialized_at']=time.perf_counter();row['head_at_return']=self.head
  # Raw persistence occurs after materialization/lease release, equally for all policies.
  for call in calls:
   payload=call.pop('payload',None)
   if payload is not None:
    path=self.out/'responses'/f"{call['name']}.json";save(path,payload);call['payload_path']=path.relative_to(self.out).as_posix();call['response_bytes']=path.stat().st_size
  row['body_finished']=time.perf_counter()
  if len(calls)<(2 if self.strategy=='RETRY_ONCE' else 1):return 'READ_DEADLINE'
  if error is not None:row['error']=error;return 'NATIVE_ERROR'
  row['published_native']=calls[-1]['name'];return 'RETURNED'
 async def read(self,name,arrival=None,deadline=30,category='primary',enqueued=None):
  arrival=time.perf_counter() if arrival is None else arrival
  row={'id':name,'category':category,'target_arrival':arrival,'enqueued':time.perf_counter() if enqueued is None else enqueued,'deadline_at':arrival+deadline,'strategy':self.strategy}
  self.reads.append(row);self.observer.emit('logical.read_request',**row)
  task=asyncio.create_task(self._read_body(row));self.tasks.add(task)
  try:
   remaining=max(0,row['deadline_at']-time.perf_counter())
   done,_=await asyncio.wait([task],timeout=remaining)
   if not done:
    row['service']='READ_DEADLINE';row['published_at']=time.perf_counter()
    self.observer.emit('logical.read_deadline',id=name)
    # Retain native task and lease until actual I/O ends. No extra retry or early release.
    if not row.get('native_calls'):
     task.cancel()
     with contextlib.suppress(asyncio.CancelledError):await task
    else:
     status=await task;row['late_status']=status;row['late_finished']=time.perf_counter()
   else:
    row['service']=task.result();row['published_at']=time.perf_counter()
    if row['published_at']>row['deadline_at']:
     row['late_status']=row['service'];row['service']='READ_DEADLINE';row['late_finished']=row['published_at']
  except DirtyUnavailable:
   if row.get('service')!='READ_DEADLINE':row['service']='UNAVAILABLE_DIRTY';row['published_at']=time.perf_counter()
   else:row['late_status']='UNAVAILABLE_DIRTY'
  except Exception as exc:
   row['error']=repr(exc)
   if row.get('service')!='READ_DEADLINE':row['service']='NATIVE_ERROR';row['published_at']=time.perf_counter()
   else:row['late_status']='NATIVE_ERROR'
  except asyncio.CancelledError:
   row['service']='HARNESS_INVALID';row['cancelled_at']=time.perf_counter()
   if not row.get('native_calls'):task.cancel()
   raise
  finally:
   if task.done():self.tasks.discard(task)
   self.observer.emit('logical.read_finish',record=row)
  return row
 async def _update_body(self,row,v,recovery):
  row['writer_queued']=time.perf_counter()
  async with self.write_lock:
   row['driver_acquired']=time.perf_counter();row['writer_gate_requested']=time.perf_counter()
   async with self.gate.write(row['id'],recovery=recovery) if self.strategy in ['COORDINATED','ENTRY_ONLY'] else contextlib.nullcontext({'ok':False}) as lease:
    row['writer_gate_acquired']=time.perf_counter();row['body_start']=time.perf_counter()
    result=await rt.native(self.observer,row['id'],self.system.update(data_id=self.data,data=source(v),dataset_id=self.dataset,chunk_level_diff=False))
    row['native_result']=rt.plain(result)
    module=importlib.import_module('cognee.modules.pipelines.layers.pipeline_execution_mode')
    active=[t for t in module._BACKGROUND_PIPELINE_TASKS if not t.done()]
    row['active_background_pipeline_tasks']=len(active)
    if active:
     await self.gate.dirty('unexpected native background pipeline')
     await asyncio.gather(*active,return_exceptions=True)
     raise RuntimeError('BOUNDARY_UNRESOLVED: unexpected background pipeline joined inside lease')
    result=row['native_result']
    ok=isinstance(result,dict) and result.get('status') in ['full_rebuild','unchanged'] and not result.get('error')
    lease['ok']=ok
    if ok:self.head=v
    row['native_success']=ok;row['body_end']=time.perf_counter()
    return 'SUCCESS' if ok else 'UPDATE_FAILED'
 async def update(self,name,v,arrival=None,deadline=60,recovery=False):
  arrival=time.perf_counter() if arrival is None else arrival
  row={'id':name,'target_version':v,'target_arrival':arrival,'deadline_at':arrival+deadline,'recovery':recovery};self.writes.append(row)
  self.observer.emit('logical.update_request',**row)
  task=asyncio.create_task(self._update_body(row,v,recovery));self.tasks.add(task)
  try:
   done,_=await asyncio.wait([task],timeout=max(0,row['deadline_at']-time.perf_counter()))
   if not done:
    row['service']='UPDATE_DEADLINE';row['deadline_seen']=time.perf_counter()
    if self.strategy=='COORDINATED' and 'body_start' in row:await self.gate.dirty('writer deadline after admission')
    if 'body_start' not in row:
     task.cancel()
     with contextlib.suppress(asyncio.CancelledError):await task
    else:row['late_status']=await task
   else:row['service']=task.result()
  except DirtyUnavailable:
   if row.get('service')!='UPDATE_DEADLINE':row['service']='UNAVAILABLE_DIRTY'
   else:row['late_status']='UNAVAILABLE_DIRTY'
  except Exception as exc:
   row['error']=repr(exc)
   if row.get('service')!='UPDATE_DEADLINE':row['service']='UPDATE_FAILED'
   else:row['late_status']='UPDATE_FAILED'
  except asyncio.CancelledError:
   row['service']='HARNESS_INVALID';row['cancelled_at']=time.perf_counter()
   if 'body_start' not in row:task.cancel()
   elif self.strategy in ['COORDINATED','ENTRY_ONLY']:await self.gate.dirty('outer writer cancelled with native task retained')
   raise
  finally:
   if task.done():row['terminated']=time.perf_counter();self.tasks.discard(task)
   self.observer.emit('logical.update_finish',record=row)
  return row
 async def probe(self,name):
  return await rt.probe(self.observer,self.dataset,self.data,name)
 async def close(self):
  self.hooks.release()
  pending=[t for t in self.tasks if not t.done()]
  if pending:await asyncio.gather(*pending,return_exceptions=True)
  save(self.out/'READS.json',self.reads);save(self.out/'WRITES.json',self.writes)
  save(self.out/'COUNTERS.json',{'events':dict(self.observer.counts),'max_concurrent_read_leases':self.gate.max_readers,'health':self.gate.state,'remote_model_calls':0,'provider_denied':self.observer.counts['provider.denied']})
  if not self.observer.stream.closed:self.observer.stream.flush();self.observer.stream.close()

# New round overrides are explicit; the inherited setup/fixture above is kept
# byte-for-byte except the class name, for an inspectable comparison.
from .clock import now_ns,stamp
from .epoch import EpochState
from .materialize import materialize

_LegacyHooks=Hooks
class Hooks(_LegacyHooks):
 def __init__(self,obs):
  super().__init__(obs);self.branches={}
 async def after(self,backend,method,args,kwargs,result):
  op=rt.OP.get()
  # Incremental adds new native graph/vector content before deleting old chunks.
  # Its real first acknowledged mutating call is observed, never substituted
  # with the full-rebuild deletion hook.
  if self.branches.get(op)=='incremental' and op not in self.changed_operations and method in ['add_nodes','add_edges','index_data_points','create_data_points','delete_nodes','delete_data_points'] and args and args[0]:
   self.changed_operations.add(op);self.obs.emit('update.first_visible_change_ack',backend=backend,method=method,args=args,actual_branch='incremental')
   if op==self.target_write:
    self.first_change=True
    if self.kind=='AW':
     self.write_held=True;self.write_reached.set();self.obs.emit('schedule.writer_first_change_held',actual_branch='incremental');await self.release_write.wait();self.obs.emit('schedule.writer_released')
   return
  await super().after(backend,method,args,kwargs,result)

class Session(_LegacySession):
 def __init__(self,out,strategy,requested_mode='FORCED_FULL'):
  super().__init__(out,strategy)
  self.epoch=EpochState(self.observer.emit);self.requested_mode=requested_mode
 async def open(self):
  await super().open()
  from .path_observer import install
  install(self)
  return self
 async def native_read(self,name):
  from cognee.api.v1.search import SearchType
  raw=await rt.native(self.observer,name,self.system.search(query_text='Where does Alice live and where does Alice work?',query_type=SearchType.HYBRID_COMPLETION,dataset_ids=[self.dataset],only_context=True,verbose=True,include_references=True,top_k=20,retriever_specific_config={'chunks_top_k':10,'entities_top_k':10,'max_edges_per_entity':20,'facts_top_k':30}))
  self.observer.emit('materialize.begin',native_name=name)
  result=await materialize(raw)
  self.observer.emit('materialize.complete',native_name=name)
  return result
 async def _read_body(self,row):
  stamp(row,'dispatcher_start');row['head_at_start']=self.head
  name=row['id'];calls=[];row['native_calls']=calls
  async def attempt(index):
   call={'name':name+f'.n{index+1}'};stamp(call,'started');calls.append(call)
   try:
    call['payload']=await self.native_read(call['name']);call['status']='RETURNED'
   except Exception as exc:
    call['status']='NATIVE_ERROR';call['error']=repr(exc);call['exception_type']=type(exc).__name__
   stamp(call,'ended')
   return call
  stamp(row,'lease_requested');status=None;accepted=None
  try:
   if self.strategy=='EPOCH_VALIDATED':
    stamp(row,'validation_start')
    status,accepted=await self.epoch.read_validated(name,attempt,row['deadline_ns'])
    stamp(row,'validation_end')
   else:
    async with self.gate.read(name) if self.strategy=='COORDINATED' else contextlib.nullcontext():
     stamp(row,'lease_acquired')
     count=2 if self.strategy=='DOUBLE_READ' else 1
     for index in range(count):
      if now_ns()>=row['deadline_ns']:break
      accepted=await attempt(index)
     status=accepted['status'] if accepted and len(calls)==count else 'READ_DEADLINE'
   stamp(row,'materialized_at');row['head_at_return']=self.head
  finally:
   # Persist every candidate, including discarded / failed validation attempts.
   # It is already immutable and fully materialized before epoch acceptance.
   for call in calls:
    if 'payload' in call:
     payload=call.pop('payload');path=self.out/'responses'/f"{call['name']}.json";save(path,payload);call['payload_path']=path.relative_to(self.out).as_posix();call['response_bytes']=path.stat().st_size
   stamp(row,'body_finished')
  if status=='RETURNED':row['published_native']=accepted['name']
  if status=='NATIVE_ERROR' and accepted:row['error']=accepted['error']
  return status
 async def read(self,name,arrival_ns=None,deadline=10,category='primary',enqueued_ns=None,slot=None):
  arrival_ns=now_ns() if arrival_ns is None else arrival_ns
  assert isinstance(arrival_ns,int)
  enqueue=now_ns() if enqueued_ns is None else enqueued_ns
  row={'id':name,'category':category,'target_ns':arrival_ns,'target_arrival_ns':arrival_ns,'target_arrival':arrival_ns/1e9,'actual_enqueue_ns':enqueue,'enqueued_ns':enqueue,'enqueued':enqueue/1e9,'deadline_ns':arrival_ns+round(deadline*1e9),'deadline_at':(arrival_ns+round(deadline*1e9))/1e9,'strategy':self.strategy,'slot':slot}
  assert enqueue>=arrival_ns,'EARLY_DISPATCH'
  self.reads.append(row);self.observer.emit('logical.read_request',**row)
  task=asyncio.create_task(self._read_body(row));self.tasks.add(task)
  try:
   done,_=await asyncio.wait([task],timeout=max(0,(row['deadline_ns']-now_ns())/1e9))
   if not done:
    row['service']='READ_DEADLINE';stamp(row,'published_at');row['response_published_ns']=row['published_at_ns'];self.observer.emit('logical.read_deadline',id=name)
    if not row.get('native_calls'):
     task.cancel()
     with contextlib.suppress(asyncio.CancelledError):await task
    else:
     row['late_status']=await task;stamp(row,'late_finished')
   else:
    row['service']=task.result();stamp(row,'published_at');row['response_published_ns']=row['published_at_ns']
    if row['published_at_ns']>row['deadline_ns']:
     row['late_status']=row['service'];row['service']='READ_DEADLINE';stamp(row,'late_finished')
  except DirtyUnavailable:
   if row.get('service')!='READ_DEADLINE':row['service']='UNAVAILABLE_DIRTY';stamp(row,'published_at')
   else:row['late_status']='UNAVAILABLE_DIRTY'
  except (TimeoutError,asyncio.TimeoutError):
   row['service']='READ_DEADLINE';stamp(row,'published_at')
  except Exception as exc:
   row['error']=repr(exc)
   if row.get('service')!='READ_DEADLINE':row['service']='NATIVE_ERROR';stamp(row,'published_at')
   else:row['late_status']='NATIVE_ERROR'
  except asyncio.CancelledError:
   row['service']='HARNESS_INVALID';stamp(row,'cancelled_at')
   if not row.get('native_calls'):task.cancel()
   raise
  finally:
   if task.done():self.tasks.discard(task)
   if 'published_at_ns' in row:row['response_published_ns']=row['published_at_ns']
   if row.get('native_calls'):row['native_terminated_ns']=max(c.get('ended_ns',0) for c in row['native_calls'])
   row['epoch_discarded']=sum(c.get('validation')=='DISCARDED' for c in row.get('native_calls',[]))
   row['epoch_wait_ns']=sum(c.get('epoch_wait_ns',0) for c in row.get('native_calls',[]))
   assert row.get('published_at_ns',arrival_ns)>=arrival_ns
   self.observer.emit('logical.read_finish',record=row)
  return row
 async def _update_body(self,row,v,recovery):
  stamp(row,'writer_queued')
  async with self.write_lock:
   stamp(row,'driver_acquired');stamp(row,'writer_gate_requested')
   protected=self.strategy=='COORDINATED'
   async with self.gate.write(row['id'],recovery=recovery) if protected else contextlib.nullcontext({'ok':False}) as lease:
    token=None;stamp(row,'writer_gate_acquired')
    if self.strategy=='EPOCH_VALIDATED':token=await self.epoch.begin_write(row['id'],recovery)
    stamp(row,'body_start');ok=False
    try:
     arguments={'data_id':self.data,'data':source(v),'dataset_id':self.dataset}
     if self.requested_mode=='FORCED_FULL':arguments['chunk_level_diff']=False
     elif self.requested_mode=='EXPLICIT_TRUE':arguments['chunk_level_diff']=True
     elif self.requested_mode!='DEFAULT_REQUEST':raise ValueError(self.requested_mode)
     row['arguments']=rt.plain(arguments);row['requested_mode']=self.requested_mode
     row['canonical_before']=getattr(self.observer,'last_canonical_source',None)
     self.observer.emit('update.arguments',arguments=arguments,requested_mode=self.requested_mode)
     result=await rt.native(self.observer,row['id'],self.system.update(**arguments));row['native_result']=await materialize(result)
     row['canonical_after']=getattr(self.observer,'last_canonical_source',None)
     module=importlib.import_module('cognee.modules.pipelines.layers.pipeline_execution_mode');active=[t for t in module._BACKGROUND_PIPELINE_TASKS if not t.done()];row['active_background_pipeline_tasks']=len(active)
     if active:
      if protected:await self.gate.dirty('unexpected native background pipeline')
      if self.strategy=='EPOCH_VALIDATED':await self.epoch.dirty('unexpected native background pipeline')
      await asyncio.gather(*active,return_exceptions=True)
      raise RuntimeError('BOUNDARY_UNRESOLVED: unexpected background pipeline')
     result=row['native_result'];ok=native_success(result)
     row['returned_status']=result.get('status') if isinstance(result,dict) else None
     row['branch_entered']=self.hooks.branches.get(row['id'])
     fallback=result.get('fallback') or {} if isinstance(result,dict) else {}
     row['fallback_reason']=fallback.get('reason');row['fallback_detail']=fallback.get('detail')
     row['changed_reused_chunk_counts']={k:result.get(k) for k in ['regions','deleted_chunks','added_chunks','reused_chunks','kept_chunks','reindexed_chunks','total_chunks']}
     row['unexpected_noop']=None # Offline target-change endpoint, never a runtime oracle.
     row['native_success']=ok
     if ok:self.head=v
     stamp(row,'body_end');lease['ok']=ok
     return 'SUCCESS' if ok else 'UPDATE_FAILED'
    finally:
     if token is not None:await self.epoch.finish_write(row['id'],token,ok)
 async def update(self,name,v,arrival_ns=None,deadline=30,recovery=False):
  arrival_ns=now_ns() if arrival_ns is None else arrival_ns;assert isinstance(arrival_ns,int)
  row={'id':name,'target_version':v,'target_ns':arrival_ns,'target_arrival_ns':arrival_ns,'target_arrival':arrival_ns/1e9,'deadline_ns':arrival_ns+round(deadline*1e9),'deadline_at':(arrival_ns+round(deadline*1e9))/1e9,'recovery':recovery};self.writes.append(row)
  stamp(row,'actual_submit');assert row['actual_submit_ns']>=arrival_ns
  self.observer.emit('logical.update_request',**row)
  task=asyncio.create_task(self._update_body(row,v,recovery));self.tasks.add(task)
  try:
   done,_=await asyncio.wait([task],timeout=max(0,(row['deadline_ns']-now_ns())/1e9))
   if not done:
    row['service']='UPDATE_DEADLINE';stamp(row,'deadline_seen')
    if 'body_start' in row:
     if self.strategy=='COORDINATED':await self.gate.dirty('writer deadline after admission')
     if self.strategy=='EPOCH_VALIDATED':await self.epoch.dirty('writer deadline after admission')
     row['late_status']=await task
    else:
     task.cancel()
     with contextlib.suppress(asyncio.CancelledError):await task
   else:row['service']=task.result()
  except DirtyUnavailable:
   if row.get('service')!='UPDATE_DEADLINE':row['service']='UNAVAILABLE_DIRTY'
   else:row['late_status']='UNAVAILABLE_DIRTY'
  except Exception as exc:
   row['error']=repr(exc)
   if row.get('service')!='UPDATE_DEADLINE':row['service']='UPDATE_FAILED'
   else:row['late_status']='UPDATE_FAILED'
  except asyncio.CancelledError:
   row['service']='HARNESS_INVALID';stamp(row,'cancelled_at')
   if 'body_start' not in row:task.cancel()
   elif self.strategy=='COORDINATED':await self.gate.dirty('writer cancelled with native task retained')
   elif self.strategy=='EPOCH_VALIDATED':await self.epoch.dirty('writer cancelled with native task retained')
   raise
  finally:
   if task.done():stamp(row,'terminated');row['native_terminated_ns']=row['terminated_ns'];self.tasks.discard(task)
   self.observer.emit('logical.update_finish',record=row)
  return row
 async def close(self):
  await super().close()
  counters=read(self.out/'COUNTERS.json');counters['epoch_state']=await self.epoch.snapshot()
  if self.strategy=='EPOCH_VALIDATED':counters['health']=self.epoch.health
  save(self.out/'COUNTERS.json',counters)

def native_success(result):
 return isinstance(result,dict) and result.get('status') in ['full_rebuild','incremental','unchanged'] and not result.get('error')
