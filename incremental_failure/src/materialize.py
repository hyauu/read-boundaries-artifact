"""Fully materialize public native results before gate exit / epoch check."""
import dataclasses, inspect
from . import native_base as rt

async def materialize(value):
    if inspect.isawaitable(value):
        return await materialize(await value)
    if inspect.isasyncgen(value):
        return [await materialize(x) async for x in value]
    if inspect.isgenerator(value):
        return [await materialize(x) for x in value]
    if hasattr(value,'model_dump'):
        return await materialize(value.model_dump())
    if dataclasses.is_dataclass(value):
        return await materialize(dataclasses.asdict(value))
    if isinstance(value,dict):
        return {str(k):await materialize(v) for k,v in value.items()}
    if isinstance(value,(list,tuple,set)):
        return [await materialize(x) for x in value]
    result=rt.plain(value)
    if isinstance(result,dict) and 'python_type' in result and 'repr' in result:
        raise TypeError('Unsupported lazy native object: '+type(value).__name__)
    return result
