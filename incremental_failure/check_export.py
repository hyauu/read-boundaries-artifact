"""Verify sanitized public evidence; no native execution or network calls."""
from pathlib import Path,PurePosixPath
import argparse,csv,hashlib,json,zipfile
R=Path(__file__).resolve().parent
sha=lambda b:hashlib.sha256(b).hexdigest()
def rows(name):
 with (R/'analysis'/name).open(encoding='utf-8',newline='') as f:return list(csv.DictReader(f))
def main():
 ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--extract-raw',action='store_true');a=ap.parse_args()
 manifest=json.loads((R/'PUBLIC_FILES.json').read_text())
 for x in manifest['files']:
  b=(R/x['path']).read_bytes();assert len(b)==x['bytes'] and sha(b)==x['sha256'],x['path']
 seen=set()
 for item in json.loads((R/'RAW_ARCHIVES.json').read_text())['parts']:
  p=R/item['file'];assert sha(p.read_bytes())==item['sha256']
  with zipfile.ZipFile(p) as z:
   assert z.testzip() is None; expected={x['path']:x for x in item['members']};assert set(z.namelist())==set(expected)
   for n in z.namelist():
    assert n not in seen;seen.add(n);b=z.read(n);assert sha(b)==expected[n]['export_sha256']
    if a.extract_raw:
     rel=PurePosixPath(n);assert not rel.is_absolute() and '..' not in rel.parts and '\\' not in n and ':' not in n
     target=R.joinpath(*rel.parts).resolve();assert target.is_relative_to(R.resolve())
     if target.exists():assert target.read_bytes()==b,'Existing different content: '+n
     else:target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b)
 r=rows('run_outcomes.csv');q=rows('probe_outcomes.csv');u=rows('update_and_recovery_paths.csv')
 assert len(r)==160 and len(q)==2400 and len(u)==480
 assert all(x['executed']=='True' for x in q) and sum(int(x['fault_fired']) for x in r)==80
 assert sum(x['usable']=='True' for x in q)==2100
 assert sum(x['conflict']=='True' for x in q)==100
 assert sum(x['dirty']=='True' for x in q)==200
 assert sum(x['service']=='RETURNED' and x['matches_canonical']=='False' for x in q)==200
 fault=[x for x in u if x['operation']=='retry_v1' and x['fault_condition']=='INJECT'];assert len(fault)==80
 assert all(x['actual_path']=='incremental_then_fallback' and x['returned_status']=='full_rebuild' and x['fallback_reason']=='chunks_not_tiling' and x['all_five_usable']=='True' for x in fault)
 assert len(rows('paired_summary.csv'))==504
 print(json.dumps({'verified':True,'selected_sequences':160,'service_probes':2400,'study_updates':480,'fault_hits':80,'raw_files':len(seen),'native_runs':0}))
if __name__=='__main__':main()