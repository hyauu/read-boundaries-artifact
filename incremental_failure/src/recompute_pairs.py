"""Rebuild paired intervals from complete per-history rates without native dependencies."""
import csv,json
from .common import ROOT
from .analyze_inc import paired,csvout,METRICS
def main():
    with (ROOT/'analysis/history_rates.csv').open(encoding='utf-8',newline='') as f:rows=list(csv.DictReader(f))
    for r in rows:
        r['complete_phase']=r['complete_phase']=='True'
        for m in METRICS:r[m]=float(r[m]) if r[m] else None
    result=paired(rows);csvout(ROOT/'analysis/paired_recomputed.csv',result);print(len(result))
if __name__=='__main__':main()
