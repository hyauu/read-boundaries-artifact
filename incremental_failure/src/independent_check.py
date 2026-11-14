"""Separate small receipt cross-check. Does not import or invoke the main checker."""
import hashlib,json
from collections import defaultdict
from .common import ROOT,read,save,jsonlines
from .outcomes import SOURCES,ROLES,canonical_version
from .sampling_inc import selected

def scan(out):
    identity=read(out/'identity.json');acks={};pending={};origin=None;bound_chunks={};ends={};issues=[]
    for e in jsonlines(out/'events.jsonl'):
        kind=e['event'];seq=e['seq']
        if kind in ('source.publish','source.candidate'):
            valid=e.get('text') in SOURCES and e.get('sha256')==hashlib.sha256(e.get('text','').encode()).hexdigest() and all(e.get(k)==identity[k] for k in ('data_id','dataset_id'))
            origin=(SOURCES.index(e['text']),e) if valid else None
            if not valid:issues.append((seq,'source'))
        if kind=='backend.request' and e.get('method') in ('add_nodes','add_edges','index_data_points'):pending[seq]=(e,origin)
        if kind=='backend.response' and e.get('request_seq') in pending:
            req,src=pending.pop(e['request_seq']);args=req['args'];method=req['method']
            if src is None:issues.append((seq,'missing origin'));continue
            v,origin_record=src
            def put(kind,uid,obj):acks.setdefault((kind,str(uid)),[]).append((seq,v,obj))
            if method=='add_nodes':
                for obj in args[0]:
                    put('node',obj['id'],obj)
                    if obj['type']=='DocumentChunk':
                        valid=obj.get('text')==SOURCES[v] and obj.get('document_id')==identity['data_id']
                        if origin_record.get('not_canonical_publish'):
                            valid=valid and obj.get('content_hash')==origin_record['sha256'] and obj.get('source_content_hash') in (None,origin_record.get('content_hash'))
                        else:valid=valid and obj.get('source_content_hash')==origin_record.get('content_hash')
                        if valid:put('chunk',obj['id'],obj);bound_chunks[(str(obj['id']),v)]=min(seq,bound_chunks.get((str(obj['id']),v),seq))
                        else:issues.append((seq,'chunk'))
                    elif obj['type']=='TextSummary':
                        if obj.get('text')==SOURCES[v]:put('summary',obj['source_chunk_id'],obj)
                        else:issues.append((seq,'summary'))
            elif method=='add_edges':
                for source,target,rel,props in args[0]:
                    owner=req['kwargs']['source_ref_key'].get(str((source,target,rel)),'')
                    expected='source_ref:v2:'+identity['dataset_id']+':'+identity['data_id']+':'
                    cid=owner[len(expected):] if owner.startswith(expected) else None
                    if cid is not None and bound_chunks.get((cid,v),10**20)<=seq:
                        put('edge',props['edge_object_id'],dict(source_id=source,target_id=target,relationship=rel,text=props.get('edge_text')))
                    else:issues.append((seq,'edge owner'))
            elif args[:2]==['EdgeType','relationship_name']:
                for obj in args[2]:put('fact',obj['id'],obj)
        if kind in ('api.return','api.error'):ends[e['boundary']]=seq
        if kind=='materialize.complete':ends[e['native_name']]=seq
    return identity,acks,bound_chunks,ends,issues

def display(text):
    prefix='Document chunk mentions '
    return (text[len(prefix):][:1].upper()+text[len(prefix):][1:] if text.startswith(prefix) else text)

def check_payload(payload,boundary,index):
    identity,acks,chunks,ends,issues=index;end=ends.get(boundary,0)
    failures=[issue for seq,issue in issues if seq<=end];roles={r:False for r in ROLES};claims={'city':set(),'company':set()};versions=set();bindings=[]
    def bind(kind,uid,fields,neutral=False):
        candidates=[(seq,v,obj) for seq,v,obj in acks.get((kind,str(uid)),[]) if seq<=end and all((display(obj.get('relationship_name','')) if kind=='fact' and k=='text' else obj.get(k))==val for k,val in fields.items())]
        if kind=='summary':candidates=[(seq,v,obj) for seq,v,obj in candidates if chunks.get((str(uid),v),10**20)<=seq]
        vs={v for seq,v,obj in candidates}
        if not candidates or (not neutral and len(vs)!=1):failures.append(kind+':'+str(uid));return None
        bindings.append((kind,str(uid),min(seq for seq,v,obj in candidates)))
        return candidates[0][1]
    def claim(text,v,role):
        city,company=SOURCES[v].split('. ',1)
        for key,value in [('city',city+'.'),('company',company)]:
            if value in text:
                claims[key].add(v);versions.add(v)
                if role in ('graph','fact'):roles[role+'_'+key]=True
    if not isinstance(payload,list) or not payload:failures.append('missing bundle')
    for bundle in payload if isinstance(payload,list) else []:
        obj=bundle.get('objects_result',{})
        if not isinstance(obj,dict) or not all(k in obj for k in ('chunks','chunk_summaries','entities','facts')):failures.append('missing fields');continue
        for row in obj['chunks']:
            p=row.get('payload',{});v=bind('chunk',row.get('id'),{k:p.get(k) for k in ('text','document_id')})
            if v is not None:roles['passage']=True;claim(p['text'],v,'passage')
        for cid,text in obj['chunk_summaries'].items():
            v=bind('summary',cid,{'text':text})
            if v is not None:roles['summary']=True;claim(text,v,'summary')
        for ent in obj['entities']:
            bind('node',ent['id'],{'name':ent.get('name')},True)
            for edge in ent['edges']:
                v=bind('edge',edge['edge_object_id'],{k:edge.get(k) for k in ('source_id','target_id','relationship','text')},edge['relationship']=='is_a')
                if v is not None and edge['relationship']!='is_a':claim(edge.get('text',''),v,'graph')
        for fact in obj['facts']:
            text=fact.get('text','');neutral=not any(part in text for src in SOURCES for part in src.split('. '))
            v=bind('fact',fact['id'],{'text':text},neutral)
            if v is not None:claim(text,v,'fact')
    conflict=any(len(v)>1 for v in claims.values());complete=all(roles.values())
    return {'roles':roles,'coverage_complete':complete,'direct_conflict':conflict,'versions':sorted(versions),'compatibility':'INCOMPATIBLE' if conflict else 'UNKNOWN' if failures or not versions else 'COMPATIBLE_WITHIN_DECLARED_SCOPE','binding_failures':failures,'checked_bindings':len(bindings)}

def crosscheck(out):
    index=scan(out);primary=read(out/'CONTENT_SCORES.json');reads=read(out/'READS.json');rows=[];errors=[]
    for row in reads:
        if row.get('category') not in ('S0','S1','S2'):continue
        for call in row.get('native_calls',[]):
            if not call.get('payload_path'):continue
            result=check_payload(read(out/call['payload_path']),call['name'],index)
            ref=primary[call['name']]
            delta={k:[ref.get(k),result[k]] for k in ('roles','coverage_complete','direct_conflict','versions','compatibility') if ref.get(k)!=result[k]}
            if delta:errors.append({'call':call['name'],'differences':delta})
            rows.append({'call':call['name'],'phase':row['category'],'published':row.get('published_native')==call['name'] and row['service']=='RETURNED','result':result,'differences':delta})
        if row['service']=='UNAVAILABLE_DIRTY' and row.get('published_native'):errors.append({'read':row['id'],'error':'dirty refusal has published candidate'})
    canonical=[]
    for phase in ('S0','S1','S2'):
        p=out/(phase+'.checkpoint.json')
        if p.exists():
            cp=read(p);raw=cp.get('canonical_content',[]);cv=canonical_version(cp)
            canonical.append({'phase':phase,'rows':len(raw),'version':cv,'hash_matches':all(r.get('sha256')==hashlib.sha256(r.get('text','').encode()).hexdigest() for r in raw),'role':'observer not service'})
    writes=[{k:w.get(k) for k in ('id','service','native_success','returned_status','fallback_reason','arguments')} for w in read(out/'WRITES.json')]
    return {'unit_id':read(out/'REQUEST.json')['unit_id'],'implementation':'independent receipt parser, no main final score call','public_candidates_checked':len(rows),'candidate_results':rows,'canonical_checks':canonical,'update_outcomes':writes,'disagreements':errors,'review_status':'AUTOMATED_CROSSCHECK_ONLY_NOT_TRACE_REVIEW'}
def main():
    import csv
    from .analyze_inc import available_attempt
    matrix=list(csv.DictReader((ROOT/'MATRIX.csv').open(encoding='utf-8',newline='')));results=[]
    for unit in selected(matrix):
        out=available_attempt(unit)
        if out:
            r=crosscheck(out);save(ROOT/'reports/crosscheck'/(unit+'.json'),r);results.append(r)
    save(ROOT/'reports/INDEPENDENT_CROSSCHECK.json',{'selected_units':32,'completed_units':len(results),'candidates':sum(r['public_candidates_checked'] for r in results),'disagreements':[{'unit':r['unit_id'],'items':r['disagreements']} for r in results if r['disagreements']],'scope':'Separate algorithm for binding, roles, refusal arithmetic and canonical/API cross-check. Not independent human review.'})
if __name__=='__main__':main()
