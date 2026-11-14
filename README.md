# Read Boundaries in Agent Memory

Reproduction and inspection materials for **Read Boundaries in Agent Memory: Consistency, Completeness, and Repair**.

This repository contains the Cognee 600-run main study, the historical Hindsight snapshot study, and a separate 160-run incremental-failure and sequential-recovery study. The two Cognee data sets retain their original denominators and analysis plans.

## Main study: 600 Cognee runs and Hindsight

1. Download `artifact-*.zip`, `BUNDLES.json`, and `restore.py` into the same directory. Each ZIP is a complete, independently readable archive, not a split ZIP volume. Each main-study ZIP is below 8 MB.
2. Run `python -B restore.py --output restored`. This verifies each ZIP and member, extracts safely, verifies compressed fragments, reconstructs sanitized source files byte for byte, and checks their hashes. Allow approximately 3 GB of local disk space. It uses the Python standard library and does not connect to a network or run experiments.
3. Run `python -B restored/cognee/review_tools/check_core_tables.py --root restored/cognee`. This checks all 244,440 logical rows, 276,776 native attempts, 680 run/stage rows and 4,700 saved comparison rows, including count/role/latency consistency. It does not reread raw trajectories or rescore evidence.
4. Run `python -B restored/paper_map/check_paper.py --root restored`. This reconstructs the manuscript's controlled and open-arrival count tables and Hindsight direct-conflict/terminal counts, and checks the saved no-write summary fields. The 200 Figure 2 seed values are in `paper_map/figure2_all_200_runs.csv`.

With NumPy installed, add `--recompute-pairs` to step 3 to recompute the Cognee paired comparisons with the original statistical function, 10,000 resamples and seed 2026101702. The default check validates saved method parameters; it does not repeat the bootstrap. This offline recomputation needs only NumPy and the restored files; it does not require a Cognee checkout. Raw-content rescoring and new native runs additionally require the pinned upstream source.

## Incremental failure: 160 separate sequences

See [`incremental_failure/README.md`](incremental_failure/README.md) for the P1/P2 protocol, code, full result tables, all 160 selected execution traces, and verification commands. It records 2,400 service probes and 480 study updates; 80 target faults were injected. The first default retry recovered all 80 fault sequences through native full-rebuild fallback, while controls returned unchanged. Failed-state refusals, conflicting deliveries and locally complete content ahead of canonical publication are all retained.

The main-study claims about 440 successful incremental updates describe the original 600-run matrix. They do not describe the supplemental failure targets or their fallback recovery.

## Names used in the paper and data

The CSV/configuration identifiers below refer to the same conditions described in the manuscript. The recorded identifiers are retained so that scripts, tables and traces remain joinable.

| Field or context | Recorded identifier | Paper terminology and meaning |
|---|---|---|
| `requested_mode` | `FORCED_FULL` | **Full** / full rebuild; `chunk_level_diff=False` is explicitly requested. |
| `requested_mode` | `DEFAULT_REQUEST` | **Default** / default incremental updates; the request omits `chunk_level_diff`. All 440 observed default updates took the incremental path. |
| `strategy` | `NATIVE` | **Unprotected**; a single native read without the read-side repairs. |
| `strategy` | `DOUBLE_READ` | **Double read**. |
| `strategy` | `COORDINATED` | **Gate**; read/write coordination. |
| `strategy` | `EPOCH_VALIDATED` | **Epoch**; generation and health validation. |
| Block B `stratum` / workload profile | `OCCASIONAL_UPDATE` | **Spaced writes** (6, 27 and 48 seconds). |
| Block B `stratum` / workload profile | `UPDATE_BURST` | **Burst writes** (21, 21.1, 21.2 and 21.3 seconds). |
| Block B `stratum` | `READ_ONLY` | **Read only**; the shared B0 control. Its `requested_mode` is `SHARED_READ_ONLY`; count these 40 runs once. |
| `block` | `A` | **Controlled**, Table III: 320 runs. |
| `block` | `B` | **Open arrival**, Table III: 200 runs, including the 40 shared read-only runs. |
| `block` | `C` | **Update failure**, Table III: 80 runs using full rebuild. |
| Block A `stratum` / entrance order | `AR` / `AW` | **Read-first** / **write-first**, respectively. |

A run can contribute multiple stage rows: the 80 Block C runs each have fault-window and recovery-probe rows, giving 680 run/stage rows across the 600 runs. Stage rows are not additional runs.

## Material coverage

| Paper element | Artifact location after restoration |
|---|---|
| Read policies and epoch/health boundaries, Sections III-A/C/D | `hindsight/paper_patch/snapshot_fix/repair.py`, `cognee/src/coordinator.py`, `cognee/src/epoch.py`, `cognee/src/native.py` |
| Protocol, Table III and workload schedules | `cognee/PROTOCOL.md`, `cognee/configs/formal/`, `cognee/RUN_MANIFEST.json` |
| Table IV, Table V and all adverse outcomes | `cognee/analysis/final/group_summary.csv`, `unit_metrics.csv`, `read_requests.csv`, `native_attempts.csv` |
| Read/write cost, path counts, CIs | `update_requests.csv`, `update_paths.csv`, `operation_costs.csv`, `paired_comparisons.csv` in the same directory |
| Figure 1 and native evidence | `cognee/selected_raw/`; selection reasons and unit mapping in `cognee/evidence/REVIEW_INVENTORY.json` |
| Figure 2 | `paper_map/figure2_all_200_runs.csv`; `paper_map/plot_figure2.py` plots those data with matplotlib |
| Failure and recovery results | Block C rows and selected failure/control trajectories; all candidates and explicit refusals are retained |
| Hindsight results and the cross-criterion summary | `hindsight/paper_patch/snapshot_fix/results.csv`, `latency.csv`, `RESULT.json`, and `raw/` |
| Source versions and measured environment | `VERSION_PINS.json`, `ENVIRONMENT.json`, `cognee/requirements_recorded.txt` |

The Cognee tables cover **all 600 runs**, not just selected examples. The public raw selection contains all 152 previously selected units and all 2,376 selected logical reads, with every selected native candidate and the corresponding event slices. It retains errors, partial results, rejected requests, refusals and discarded candidates. That selection is directed and is not a new estimate of event frequency. The full per-request/per-attempt tables retain the unselected population.

The Hindsight export contains all **70 accepted runs** underlying the reported comparison: 20 controlled runs (10 pairs), 40 natural runs (20 per variant), and 10 no-write runs (5 pairs). Failed preliminary attempts and unrelated historical studies are outside this paper's released population. The original result tables still include historical order-only labels; **54 baseline and 128 snapshot order-only labels are not certified direct conflicts**. Use `verified_raw_conflicts`, not `solver_TORN`, for the reported natural counts. Terminal C2 checks are a separate settled-state endpoint.

## Interpretation and reproduction limits

- Cognee uses a single owner, one changing source, deterministic extraction and eight-dimensional fixture embeddings. No external model was called in this study. Six-role coverage is a representation contract, not answer quality or a production guarantee.
- Default requests omit `chunk_level_diff`; all 440 actual default updates are incremental. Full rebuild is explicitly requested. Forty failed full mutations keep their absent post-return fields.
- Gate and Epoch remove the measured delivered content failures; Epoch has longer reader tails and shorter writer tails under default bursts. Queue rejections and failed-state refusals remain part of service outcomes.
- Shared read-only B0 runs are counted once. CIs are descriptive 95% intervals over whole paired histories/runs, not request-level independent samples. A uses 90-second controlled guards; ordinary B/C reads and updates use their recorded 10/30-second budgets.
- Hindsight's snapshot mechanism repairs ten controlled conflicts but misses the joint local cost screen: the one-client throughput ratio is approximately 0.790. Its exact historical hardware and Python/asyncpg microversions are unavailable. This artifact cannot supply those missing facts.
- Saved-output verification is immediately supported. A new native execution additionally requires the pinned upstream systems, local backend setup, suitable dependency versions and environment adaptation. The retained runners are research implementations, not a certified one-command cross-platform benchmark. No experiment is launched by the restoration or paper checks.

## Anonymization and integrity

Identifying account strings, local workspace paths and fixture credentials have been replaced in the exported copies; scientific values, event order, timings, seeds and logical IDs are retained. `DATA_MANIFEST.json` and `BUNDLES.json` authenticate the export, while historical source seals and internal hashes refer to the original evidence rather than redacted bytes. `PUBLIC_CHECKS.json` records the release checks and their scope.
