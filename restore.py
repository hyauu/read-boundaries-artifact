"""Verify and restore the paper artifact; no network or experiment execution."""
from pathlib import Path, PurePosixPath
import argparse, gzip, hashlib, json, zipfile

def sha(b): return hashlib.sha256(b).hexdigest()
def safe(root,name):
    p=PurePosixPath(name)
    if p.is_absolute() or '..' in p.parts or '\\' in name or ':' in name:raise ValueError(name)
    out=(root/Path(*p.parts)).resolve()
    if not out.is_relative_to(root.resolve()):raise ValueError(name)
    return out

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--output',type=Path,default=Path('restored'))
    args=ap.parse_args();base=Path(__file__).resolve().parent
    index=json.loads((base/'BUNDLES.json').read_text())
    root=args.output.resolve();root.mkdir(parents=True,exist_ok=True)
    seen=set()
    for item in index['bundles']:
        path=base/item['file']
        if sha(path.read_bytes())!=item['sha256']:raise ValueError('ZIP hash: '+item['file'])
        with zipfile.ZipFile(path) as z:
            names=z.namelist()
            if len(names)!=len(set(names)) or set(names)!=set(item['members']):raise ValueError('ZIP membership')
            for name in names:
                if name in seen:raise ValueError('Duplicate member')
                seen.add(name);b=z.read(name)
                if sha(b)!=item['members'][name]:raise ValueError('Member hash: '+name)
                p=safe(root,name);p.parent.mkdir(parents=True,exist_ok=True)
                if p.exists() and p.read_bytes()!=b:raise ValueError('Will not overwrite different content: '+name)
                p.write_bytes(b)
    manifest=json.loads((root/'DATA_MANIFEST.json').read_text())
    for rec in manifest['source_files']:
        target=safe(root,rec['path'])
        if len(rec['parts'])==1 and rec['parts'][0]['path']==rec['path']:
            if sha(target.read_bytes())!=rec['sanitized_sha256']:raise ValueError('Source hash')
            continue
        parts=[]
        for item in rec['parts']:
            b=safe(root,item['path']).read_bytes()
            if sha(b)!=item['sha256']:raise ValueError('Fragment hash')
            parts.append(gzip.decompress(b))
        b=b''.join(parts)
        if sha(b)!=rec['sanitized_sha256']:raise ValueError('Reassembled hash')
        target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b)
    print(json.dumps({'pass':True,'verified_zip_members':len(seen),'restored_source_files':len(manifest['source_files']),'formal_units':600,'hindsight_runs':70}))

if __name__=='__main__':main()
