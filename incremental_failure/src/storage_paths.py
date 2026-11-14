"""Bounded paths inside the user-approved temporary native storage root."""
import hashlib
from pathlib import Path
def storage_path(runtime_root,relative):
    token=hashlib.sha256(Path(relative).as_posix().encode('utf-8')).hexdigest()[:16]
    path=Path(runtime_root)/'s'/token
    # Reserve the longest observed backend suffix plus additional basename margin.
    longest=path/'runtime/system/databases/cognee.lancedb/EdgeType_relationship_name.lance/data'/('0'*64+'.lance')
    if len(str(longest))>=240:raise RuntimeError('Configured native root exceeds conservative Windows path budget')
    return path
