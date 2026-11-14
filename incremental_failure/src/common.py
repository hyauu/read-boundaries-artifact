from pathlib import Path
import datetime,hashlib,json,os,time
ROOT=Path(__file__).resolve().parents[1]
PROJECT=ROOT.parent
PYTHON=PROJECT/'replication/.venv/Scripts/python.exe'
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8'))
def jsonlines(p):
 with Path(p).open(encoding='utf-8') as stream:
  for line in stream:
   if line.strip():yield json.loads(line)
def save(p,value):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 temp=p.with_suffix(p.suffix+'.tmp');temp.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8');os.replace(temp,p)
def append(p,value):
 with Path(p).open('a',encoding='utf-8') as f:f.write(json.dumps(value,ensure_ascii=False,default=str)+'\n');f.flush()
def worker_env(out):
 env=dict(os.environ,PYTHONUTF8='1',PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(ROOT/'guard'),TM_GUARD_LOG=str(out/'network_guard.jsonl'))
 for k in list(env):
  if k.endswith('API_KEY') or k in {'DATABASE_URL','VECTOR_DB_URL','GRAPH_DATABASE_URL'}:env.pop(k,None)
 return env
