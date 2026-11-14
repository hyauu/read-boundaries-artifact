"""Selection is independent of scientific outcomes, fixed before formal execution."""
import csv
from .common import ROOT,save
def selected(rows):return [r['unit_id'] for r in rows if int(r['history_id']) in (81001,81002)]
def main():
    rows=list(csv.DictReader((ROOT/'MATRIX.csv').open(encoding='utf-8',newline='')))
    units=selected(rows);assert len(units)==32 and len(set(units))==32
    save(ROOT/'reports/REVIEW_SCOPE.json',{'selection':'histories 81001 and 81002, all sites, policies and fault conditions','units':units,'count':len(units),'phases':['S0','S1','S2'],'review_kind':'Independent implementation of offline cross-check; execution-agent public trace review. Not an independent human or blinded review.','all_other_units':'Full machine checks and complete counterexample index','record_only_after_reading':True})
if __name__=='__main__':main()
