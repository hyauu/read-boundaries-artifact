"""Offline outcome definitions, frozen before formal runs."""
PHASES=('S0','S1','S2')
POLICIES=('NATIVE','DOUBLE_READ','COORDINATED','EPOCH_VALIDATED')
OFFSETS=(0,1,2,4,8)
ROLES=('passage','summary','graph_city','graph_company','fact_city','fact_company')
SOURCES=('Alice lives in Seattle. Alice works at Company X.','Alice lives in San Francisco. Alice works at Company Y.')

def classify(row):
    service=row.get('service','NOT_EXECUTED')
    if service!='RETURNED':return service
    if row.get('direct_conflict'):return 'DIRECT_CONFLICT'
    if not row.get('coverage_complete'):return 'PARTIAL'
    if row.get('compatibility')!='COMPATIBLE_WITHIN_DECLARED_SCOPE':return 'UNKNOWN'
    return 'COMPATIBLE_COMPLETE'

def canonical_version(checkpoint):
    rows=checkpoint.get('canonical_content',[])
    if len(rows)!=1:return None
    value=rows[0].get('text')
    return SOURCES.index(value) if value in SOURCES else None

def duration_ms(row,end,start):
    a,b=row.get(end+'_ns'),row.get(start+'_ns')
    return (a-b)/1e6 if a is not None and b is not None else None

def health_name(health,policy):
    return health.get('epoch',{}).get('health') if policy=='EPOCH_VALIDATED' else health.get('gate') if policy=='COORDINATED' else 'UNPROTECTED'
