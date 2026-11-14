"""Forward native pinned functions unchanged, recording real branches/stages."""
import functools,importlib
from .common import *
from . import native_base as rt

def install(session):
    obs=session.observer
    update=importlib.import_module('cognee.api.v1.update.update')
    inc=importlib.import_module('cognee.api.v1.update.incremental')
    original_reason=update._full_rebuild_reason
    @functools.wraps(original_reason)
    def reason(*args,**kw):
        result=original_reason(*args,**kw)
        obs.emit('path.preflight',chunk_level_diff=args[0],fallback=rt.plain(result))
        if result is not None: session.hooks.branches[rt.OP.get()]='full_rebuild'
        return result
    update._full_rebuild_reason=reason
    for module,name,branch in [(update,'incremental_update','incremental_preflight'),(inc,'_run_incremental_update','incremental'),(update,'forget','full_rebuild')]:
        target=getattr(module,name)
        def wrap(target,name,branch):
            @functools.wraps(target)
            async def forward(*args,**kw):
                op=rt.OP.get();session.hooks.branches[op]=branch
                obs.emit('path.branch_entered',function=name,branch=branch)
                try:
                    result=await target(*args,**kw)
                    obs.emit('path.branch_return',function=name,branch=branch,result=result)
                    return result
                except BaseException as exc:
                    obs.emit('path.branch_error',function=name,branch=branch,error=repr(exc),refusal_reason=getattr(getattr(exc,'reason',None),'value',None))
                    raise
            return forward
        setattr(module,name,wrap(target,name,branch))
    stage=inc._stage_new_content
    @functools.wraps(stage)
    async def staged(*args,**kw):
        result=await stage(*args,**kw)
        text=await inc._read_processed_text(result.raw_data_location)
        obs.emit('source.candidate',data_id=session.data,dataset_id=session.dataset,raw_data_location=result.raw_data_location,content_hash=result.content_hash,text=text,sha256=hashlib.sha256(text.encode()).hexdigest(),not_canonical_publish=True)
        return result
    inc._stage_new_content=staged
    publish=inc.publish_updated_data
    @functools.wraps(publish)
    async def published(*args,**kw):
        obs.emit('path.publish_begin')
        result=await publish(*args,**kw)
        await rt.record_source(obs,session.dataset)
        obs.emit('path.publish_complete')
        return result
    inc.publish_updated_data=published
