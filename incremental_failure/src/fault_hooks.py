"""One-shot ordinary exceptions at acknowledged incremental publication sites."""
import functools, importlib
from . import native_base as rt
from .common import save

class InjectedIncrementalFailure(RuntimeError):
    pass

class FaultHooks:
    def __init__(self, session, site, inject):
        self.s=session; self.site=site; self.inject=inject
        self.target='target_v1'; self.armed=True; self.fired=0
        self.states={}; self.originals=[]
    def state(self):
        return self.states.setdefault(rt.OP.get(),dict(write_entered=False,new_write_acks=0,delete_started=False,delete_completed=False,delete_ids=[],publish_started=False,publish_completed=False,sites=[]))
    def emit(self,event,**kw): return self.s.observer.emit(event,**kw)
    def patch(self,module,name,fn):
        original=getattr(module,name); self.originals.append((module,name,original)); setattr(module,name,fn(original))
    def install(self):
        inc=importlib.import_module('cognee.api.v1.update.incremental')
        self.emit('fault.armed',target_operation=self.target,site=self.site,condition='INJECT' if self.inject else 'NO_FAULT')
        def write(original):
            @functools.wraps(original)
            async def f(*a,**k):
                self.state()['write_entered']=True
                self.emit('inc.write_publish.enter',function='_write_and_publish')
                try:
                    result=await original(*a,**k);self.emit('inc.write_publish.return');return result
                except Exception as e:
                    self.emit('inc.write_publish.error',error=repr(e),exception_type=type(e).__name__);raise
            return f
        self.patch(inc,'_write_and_publish',write)
        def ack(original):
            @functools.wraps(original)
            async def f(*a,**k):
                self.emit('inc.new_derived_write.begin',items=a[0] if a else k)
                result=await original(*a,**k)
                state=self.state()
                if state['write_entered']:state['new_write_acks']+=1
                self.emit('inc.new_derived_write.ack',items=a[0] if a else k,state=state)
                return result
            return f
        self.patch(inc,'add_data_points',ack)
        def deletion(original):
            @functools.wraps(original)
            async def f(*a,**k):
                state=self.state();state['delete_ids']=rt.plain(a[0] if a else k.get('chunk_ids',[]))
                if self.site=='P1':self.reach('P1')
                state['delete_started']=True;self.emit('inc.withdraw.begin',state=state)
                result=await original(*a,**k)
                state['delete_completed']=True;self.emit('inc.withdraw.ack',state=state,result=result)
                return result
            return f
        self.patch(inc,'delete_chunks_incremental',deletion)
        def publish(original):
            @functools.wraps(original)
            async def f(*a,**k):
                if self.site=='P2':self.reach('P2')
                state=self.state();state['publish_started']=True;self.emit('inc.canonical_publish.begin',state=state)
                result=await original(*a,**k)
                state['publish_completed']=True;self.emit('inc.canonical_publish.ack',state=state)
                return result
            return f
        self.patch(inc,'publish_updated_data',publish)
        def errorlog(original):
            @functools.wraps(original)
            async def f(*a,**k):
                self.emit('inc.native_error_handler.begin',args=a,kwargs=k)
                try:
                    result=await original(*a,**k);self.emit('inc.native_error_handler.return');return result
                except Exception as e:self.emit('inc.native_error_handler.error',error=repr(e));raise
            return f
        self.patch(inc,'log_pipeline_run_error',errorlog)
    def reach(self,site):
        state=self.state()
        prerequisites={
          'actual_incremental':self.s.hooks.branches.get(rt.OP.get())=='incremental',
          'write_body_entered':state['write_entered'],
          'new_derived_write_ack':state['new_write_acks']>0,
          'nonempty_delete':bool(state['delete_ids']),
          'canonical_not_started':not state['publish_started'],
          'withdraw_expected':not state['delete_started'] if site=='P1' else state['delete_completed']
        }
        state['sites'].append(site)
        self.emit('fault.site_reached',site=site,prerequisites=prerequisites,state=state,target_operation=rt.OP.get()==self.target)
        if rt.OP.get()!=self.target:return
        if not all(prerequisites.values()):
            self.emit('fault.site_unqualified',site=site,prerequisites=prerequisites)
            raise RuntimeError('HARNESS_SITE_PRECONDITION_FAILED')
        if self.armed and self.inject:
            self.armed=False;self.fired+=1
            self.emit('fault.fired',site=site,count=self.fired,exception_type='InjectedIncrementalFailure',state=state)
            raise InjectedIncrementalFailure('INJECTED_'+site)
    def disarm(self):
        self.armed=False;self.emit('fault.disarmed',fired=self.fired)
    def close(self):
        for module,name,original in reversed(self.originals):setattr(module,name,original)
        self.emit('fault.hooks_restored',count=len(self.originals))
        save(self.s.out/'FAULT.json',dict(site=self.site,condition='INJECT' if self.inject else 'NO_FAULT',fired=self.fired,target=self.target,states=self.states))
