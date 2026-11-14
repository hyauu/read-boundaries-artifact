"""Offline local receipt/content validator. No global commit-cut claims."""
import ast
from collections import defaultdict
from .common import *

CITIES=['Seattle','San Francisco','Boston','Denver','Austin','Portland']
COMPANIES=['Company X','Company Y','Company Z','Company Q','Company R','Company S']
SOURCES=[f'Alice lives in {c}. Alice works at {x}.' for c,x in zip(CITIES,COMPANIES)]
# Export path adjustment; pinned upstream source bytes are unchanged.
fact_source=ROOT/'inputs/pinned_sources/cognee/modules/retrieval/hybrid/facts.py'
tree=ast.parse(fact_source.read_text(encoding='utf-8'))
parts=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_fact_display_text' or isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='CONTAINS_FACT_PREFIX' for t in n.targets)]
display={};exec(compile(ast.Module(body=parts,type_ignores=[]),str(fact_source),'exec'),display)
display_fact=display['_fact_display_text']

class ReceiptIndex:
 def __init__(self,out):
  self.out=Path(out);self.identity=read(self.out/'identity.json');self.chunks=defaultdict(list);self.summaries=defaultdict(list);self.edges=defaultdict(list);self.facts=defaultdict(list);self.nodes=defaultdict(list);self.ends={};self.errors=[];self.error_events=[];self.first_changes=[]
  requests={};current=None
  for e in jsonlines(self.out/'events.jsonl'):
   kind=e['event']
   if kind in ['source.publish','source.candidate']:
    try:
     assert e['data_id']==self.identity['data_id'] and e['dataset_id']==self.identity['dataset_id']
     v=SOURCES.index(e['text']);assert hashlib.sha256(e['text'].encode()).hexdigest()==e['sha256'];current=(v,e)
    except (ValueError,AssertionError):self.issue(e['seq'],'Unbound canonical/staged source');current=None
   elif kind=='backend.request' and e.get('method') in ['add_nodes','add_edges','index_data_points']:requests[e['seq']]=(e,current)
   elif kind=='backend.response' and e['request_seq'] in requests:
    req,origin=requests.pop(e['request_seq']);seq=e['seq'];args=req['args'];method=req['method']
    if origin is None:self.issue(seq,'Write lacks canonical/staged source observation');continue
    v,src=origin
    try:
     if method=='add_nodes':
      for p in args[0]:
       self.nodes[p['id']].append((seq,v,p))
       if p['type']=='DocumentChunk':
        assert p['document_id']==self.identity['data_id'] and p['text']==SOURCES[v]
        # Native incremental chunks have source_content_hash=None. Bind the
        # exact same staged document using its actual SHA-256 content hash,
        # stable document ID, staged-file observation and acknowledged write.
        # Do not fill in a missing field or label stage as canonical publish.
        if src.get('not_canonical_publish'):
         assert p.get('source_content_hash') in [None,src['content_hash']]
         assert p.get('content_hash')==src['sha256']
        else:assert p['source_content_hash']==src['content_hash']
        self.chunks[p['id']].append((seq,v,p))
       elif p['type']=='TextSummary':
        assert p['text']==SOURCES[v]
        self.summaries[p['source_chunk_id']].append((seq,v,p))
     elif method=='add_edges':
      for a,b,rel,props in args[0]:
       owner=req['kwargs']['source_ref_key'][str((a,b,rel))];prefix,ver,ds,doc,chunk=owner.split(':')
       assert (prefix,ver,ds,doc)==('source_ref','v2',self.identity['dataset_id'],self.identity['data_id'])
       assert any(cv==v and cs<=seq for cs,cv,p in self.chunks[chunk])
       p=dict(source_id=a,target_id=b,relationship=rel,text=props.get('edge_text'),edge_object_id=props['edge_object_id'],owner=owner)
       self.edges[p['edge_object_id']].append((seq,v,p))
     elif args[:2]==['EdgeType','relationship_name']:
      for p in args[2]:self.facts[p['id']].append((seq,v,p))
    except (KeyError,ValueError,AssertionError) as exc:self.issue(seq,f'Unbound native publication {seq}: {exc!r}')
   elif kind in ['api.return','api.error']:self.ends[e['boundary']]=e['seq']
   elif kind=='materialize.complete':self.ends[e['native_name']]=e['seq']
   elif kind=='update.first_visible_change_ack':self.first_changes.append(e)
  for cid,observations in self.summaries.items():
   for seq,v,p in observations:
    if not any(cv==v and cs<=seq for cs,cv,cp in self.chunks[cid]):self.issue(seq,'Summary lacks bound owner chunk '+cid)
 def issue(self,seq,message):
  self.errors.append(message);self.error_events.append((seq,message))
 def score(self,payload,boundary):
  end=self.ends.get(boundary,0);errors=[message for seq,message in self.error_events if seq<=end];claims={'city':set(),'company':set()};basis=set();mapped=[]
  roles={'passage':False,'summary':False,'graph_city':False,'graph_company':False,'fact_city':False,'fact_company':False}
  def match(observations,predicate,path,neutral=False):
   hits=[(seq,v,p) for seq,v,p in observations if seq<=end and predicate(p)]
   if not hits:errors.append('Unknown/unacknowledged native item '+path);return None
   vs={v for seq,v,p in hits}
   if not neutral and len(vs)!=1:errors.append('Ambiguous source version '+path);return None
   v=hits[0][1];mapped.append({'path':path,'version':None if neutral else v,'ack_seq':hits[0][0]})
   return v
  def content(text,v,role):
   found=set()
   for k,statement in [('city',f'Alice lives in {CITIES[v]}.'),('company',f'Alice works at {COMPANIES[v]}.')]:
    if statement in str(text):claims[k].add(v);found.add(k);basis.add(v)
   if role in ['graph','fact']:
    for k in found:roles[role+'_'+k]=True
  if not isinstance(payload,list) or not payload:errors.append('Missing native hybrid bundle')
  else:
   for bi,b in enumerate(payload):
    o=b.get('objects_result')
    if not isinstance(o,dict) or any(k not in o for k in ['chunks','chunk_summaries','entities','facts']):errors.append('Missing hybrid object fields');continue
    for i,c in enumerate(o['chunks']):
     p=c.get('payload',{});v=match(self.chunks[c['id']],lambda x:x['text']==p.get('text') and x['document_id']==p.get('document_id'),f'{bi}.chunks.{i}')
     if v is not None:roles['passage']=True;content(p['text'],v,'passage')
    for cid,txt in o['chunk_summaries'].items():
     v=match(self.summaries[cid],lambda p:p['text']==txt,f'{bi}.summaries.{cid}')
     if v is not None:roles['summary']=True;content(txt,v,'summary')
    for i,entity in enumerate(o['entities']):
     match(self.nodes[entity['id']],lambda p:p.get('name')==entity.get('name'),f'{bi}.entity.{i}',neutral=True)
     for j,edge in enumerate(entity['edges']):
      v=match(self.edges[edge['edge_object_id']],lambda p:all(p.get(k)==edge.get(k) for k in ['source_id','target_id','relationship','text']),f'{bi}.entity.{i}.edge.{j}',neutral=edge['relationship']=='is_a')
      if v is not None and edge['relationship']!='is_a':content(edge['text'],v,'graph')
    for i,f in enumerate(o['facts']):
     # Invariant type/membership labels carry no city/job claim; exact ID and text still required.
     observations=self.facts[f['id']];neutral=not any(f'Alice lives in {c}.' in f['text'] for c in CITIES) and not any(f'Alice works at {c}.' in f['text'] for c in COMPANIES)
     v=match(observations,lambda p:display_fact(p['relationship_name'])==f['text'],f'{bi}.facts.{i}',neutral=neutral)
     if v is not None:content(f['text'],v,'fact')
  conflict=any(len(v)>1 for v in claims.values())
  compatibility='INCOMPATIBLE' if conflict else ('COMPATIBLE_WITHIN_DECLARED_SCOPE' if not errors and basis else 'UNKNOWN')
  return {'compatibility':compatibility,'direct_conflict':conflict,'roles':roles,'coverage_complete':all(roles.values()),'versions':sorted(basis),'claims':{k:sorted(v) for k,v in claims.items()},'mapping_errors':errors,'mapping':mapped}

def score_attempt(out):
 out=Path(out);index=ReceiptIndex(out);reads=read(out/'READS.json');native={};rows=[]
 for r in reads:
  for call in r.get('native_calls',[]):
   if 'payload_path' in call:native[call['name']]=index.score(read(out/call['payload_path']),call['name'])
  result=native.get(r.get('published_native'),{}) if r['service']=='RETURNED' else {}
  result={k:v for k,v in result.items() if k!='mapping'}
  row={**r,**result}
  row['usable_by_deadline']=r['service']=='RETURNED' and r['published_at']<=r['deadline_at'] and result.get('compatibility')=='COMPATIBLE_WITHIN_DECLARED_SCOPE' and result.get('coverage_complete',False)
  versions=result.get('versions',[])
  for at in ['start','return']:row['lag_at_'+at]=r.get('head_at_'+at,0)-versions[0] if len(versions)==1 else None
  rows.append(row)
 save(out/'CONTENT_SCORES.json',native);save(out/'SCORED_READS.json',rows)
 return rows,index
