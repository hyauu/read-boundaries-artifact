import unittest,csv
from pathlib import Path
from src.common import ROOT,read
from src.storage_paths import storage_path
class StorageTests(unittest.TestCase):
    def test_matrix_and_attempts_are_unique_bounded(self):
        mapping=read(ROOT/'configs/RUNTIME_STORAGE.json');paths=[]
        with (ROOT/'MATRIX.csv').open(encoding='utf-8',newline='') as f:rows=list(csv.DictReader(f))
        for r in rows:
            for attempt in ['attempt-001','attempt-002']:
                p=storage_path(mapping['runtime_root'],Path('runs')/r['unit_id']/attempt);paths.append(p)
                worst=p/'runtime/system/databases/cognee.lancedb/EdgeType_relationship_name.lance/data'/('0'*64+'.lance')
                self.assertLess(len(str(worst)),240)
                self.assertTrue(p.is_relative_to(Path(mapping['runtime_root'])))
        self.assertEqual(len(paths),len(set(paths)))
    def test_oversized_root_fails_before_native(self):
        with self.assertRaises(RuntimeError):storage_path('C:/'+('a'*220),'runs/unit/attempt-001')
    def test_same_scope_is_stable(self):
        root=read(ROOT/'configs/RUNTIME_STORAGE.json')['runtime_root']
        self.assertEqual(storage_path(root,'runs/u/attempt-002'),storage_path(root,Path('runs')/'u'/'attempt-002'))
