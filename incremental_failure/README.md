# Incremental failure and sequential recovery

Supplemental code and data for *Read Boundaries in Agent Memory: Consistency, Completeness, and Repair*.

The fixed matrix contains 160 fresh-store sequences: ten histories × two failure sites × fault/control × four policies. All selected sequences completed, producing 2,400 service probes and 480 study updates. These runs are separate from the original 600-run study.

## Inspect and verify

1. Download this directory, including all five `raw/traces-*.zip` archives. Each archive is independently readable and below 8 MB.
2. Run `python -B check_export.py`. It verifies release hashes, each raw member and the result counts without network access or native experiments. Add `--extract-raw` to restore all selected raw sequences under `runs/`.
3. With NumPy installed, run `python -B -m src.recompute_pairs` from this directory. This repeats the frozen 10,000-resample paired-history bootstrap (seed 2026111302), writes `analysis/paired_recomputed.csv`, and should reproduce all 504 rows of `analysis/paired_summary.csv` byte for byte. The required pinned Cognee facts source and its Apache-2.0 license are included.

## Evidence map

| Item | Location |
| --- | --- |
| Failure sites, controls, timing and sequential recovery | `PROTOCOL.md`, `SITE_MAP.md`, `MATRIX.csv` |
| Native hooks, driver and protection policies | `src/fault_hooks.py`, `src/worker_inc.py`, `src/coordinator.py`, `src/epoch.py` |
| Classification and frozen statistics | `src/content.py`, `src/outcomes.py`, `src/analyze_inc.py` |
| All service probes and actual update/recovery paths | `analysis/probe_outcomes.csv`, `analysis/update_and_recovery_paths.csv` |
| Canonical and derived storage observations | `analysis/checkpoint_states.csv` |
| All six policy contrasts and descriptive intervals | `analysis/paired_summary.csv`, `analysis/history_rates.csv` |
| Run selection and superseded attempts | `analysis/run_outcomes.csv`, `ATTEMPT_INVENTORY.json` |
| All selected raw sequences | `raw/`, `RAW_ARCHIVES.json` |
| Fixed 32-run trace review and cross-check scope | `reports/REVIEW_SCOPE.json`, `reports/trace_review/`, `reports/INDEPENDENT_CROSSCHECK.json` |

`NATIVE` = Unprotected; `DOUBLE_READ` = Double read; `COORDINATED` = Gate; `EPOCH_VALIDATED` = Epoch. P1 follows new graph/vector writes before old-chunk withdrawal. P2 follows withdrawal before canonical Data publication. S0 is after the target update, S1 after the same-input default retry R1, and S2 after the later explicit full rebuild R2. The 15-second minimum wait is an experimental trigger, separate from native recovery time.

Local compatibility and six-role completeness do not imply agreement with the canonical Data record. A refusal has no delivered source basis. R1 uses omitted-flag default requests; observed recovery via `chunks_not_tiling` is full-rebuild fallback. R2 follows R1 in the same store, so the two stages are not independent causal recovery arms. Ten histories repeat one source mechanism; five probes are nested observations, not independent samples.

## Provenance and execution scope

The selected set uses attempt-002 for the full eight-cell 81001/P2 and 81002/P2 groups, following a path-length setup failure and a storage interruption, respectively. The selection inventory records all 171 formal attempts; 11 superseded formal attempts and five qualification attempts are excluded. Original excluded receipts remain in the local provenance archive. Missing interrupted observations have not been reconstructed or scored as negative outcomes.

Exported copies replace local paths and environment account strings. IDs, contents, ordering, timings, seeds and numeric outcomes are retained. `EXPORT_MANIFEST.json` records source and exported hashes; `RAW_ARCHIVES.json` authenticates the redacted raw members. Historical `RAW_SEAL.json`, freeze hashes and original seals refer to original bytes, not the sanitized copies. The exported content module points at the included facts source; its scoring functions are unchanged.

The fixed review and offline cross-check are not independent human certification. Reproduction checks do not start workers or call external models. New native execution requires the pinned Cognee revision and compatible local stores/dependencies; the frozen runners retain their original environment assumptions and should not be launched as an unconfigured service.
