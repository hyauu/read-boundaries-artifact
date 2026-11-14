# Controlled incremental failure and sequential recovery protocol

The fixed 160-row randomized matrix is recorded in MATRIX.csv. Histories 81001–81010 repeat the same synthetic source mechanism; they are not ten independent semantic domains.

## Implementation boundary
Pinned Cognee 1.6.2, commit ba3631f2ed363a6ea50d649c34c56885af6b36fe. The adapter, materialization, gate, epoch, monotonic clock, source observer, receipt scorer and deterministic eight-dimensional fixture are copied byte-for-byte. New fault hooks, stage driver, controller, outcome definitions and analyses are separately frozen. The 600-run study is a separate data set.

One fresh native process and isolated store per unit. Default target and R1 requests omit chunk_level_diff; R2 explicitly supplies False. Fixed ordinary RuntimeError subclass, once only in target_v1. P1 is after acknowledged new derived writes before nonempty old-chunk withdrawal. P2 is after the actual withdrawal returns before canonical publication. Original cleanup, error handling and fallback execute unchanged. Lack of site reach and early fallback are valid observed paths.

## Storage
Temporary native databases use NTFS; the initial exFAT qualification failed before fault injection. The same runtime location policy applies to all conditions. Runtime databases and caches are excluded from this export.

## Timing
After each update's actual quiescence, schedule five reads at offsets 0, 1, 2, 4, 8 seconds. Every probe has a 10-second logical deadline from scheduled arrival. Keep all native candidates, late finishes, refusal and failures. After probes terminate, perform a read-only multi-interface storage sample, explicitly separate from service.

R1 starts no earlier than t0+15 seconds and after all probe/checkpoint/native work has terminated. R2 follows the same rule relative to t1. Both are always attempted, independent of content correctness, on fault and controls alike. Update logical deadline 30 seconds; update plus native drain observation maximum 120 seconds. If unresolved, quarantine the owned scope and mark later phases NOT_EXECUTED. No new write touches a live unresolved store.

Script delay, observer overhead, native body, queue and extra safety waiting are separate fields. This is a predetermined sequential recovery study, not an independent causal comparison of R1 against R2. Whole-unit watchdogs and owned descendant termination, if needed, are recorded interruptions/resource cleanup rather than crash-recovery experiments.

## Runtime and offline semantics
Native runtime success is incremental/full_rebuild/unchanged without error. Runtime READY never depends on gold content or six-role scoring. The setup-only initial check requires a complete bound v0 state.

Offline roles remain passage, summary, graph_city, graph_company, fact_city and fact_company. A response can be locally compatible while stale, while mismatching canonical Data, or while missing roles. Canonical matching, requested v1 matching and source-basis compatibility remain distinct.

Mutually exclusive display priority: non-return service status (including NOT_EXECUTED, DIRTY, error, deadline, exhausted); otherwise DIRECT_CONFLICT; otherwise PARTIAL; otherwise UNKNOWN; otherwise COMPATIBLE_COMPLETE. Independent conflict/partial/unknown flags are also retained. These flags overlap and must not be added as a partition. Usable additionally requires actual delivery within the original deadline. Missing is never imputed as failed service.

Storage checkpoints inventory canonical text and raw graph/vector rows within recorded sampling intervals. They do not prove a cross-store global commit order or count as successful protected reads.

## Statistics and validation
Five probes form one history-level stage rate. Ten planned histories per site, condition, policy and phase; pair only histories with five executed probes in both compared policies. Planned/executed denominators and missing paired histories remain explicit. All six policy contrasts are emitted; Gate versus Unprotected, Epoch versus Unprotected and Epoch versus Gate are primary. 10,000 paired history resamples, NumPy default_rng seed 2026111302, percentile descriptive 95% intervals. Shared controls, sites and probes do not increase independent N.

Four native qualification units (P1/P2 × fault/control, NATIVE) establish real path/ACK prerequisites and one-shot behavior. No effect-size acceptance condition. Local tests cover lifecycle, classification, known bootstrap effects, empty/partial/unbound content, and the separate receipt parser.

All formal units receive mechanical checks. The fixed 32 units in reports/REVIEW_SCOPE.json receive independently implemented offline cross-checks and actual execution-agent public trace review. Cross-checks never call the main checker's final verdict. This is not an independent human/blinded review. Reviews are registered only after reading. Counterexamples across every unit retain a complete index.

## Attempt selection and engineering exclusions
The selected data set contains 160 completed sequences. Eleven superseded formal attempts and five qualification attempts are excluded. A Windows path-length setup failure required replacement of the full eight-cell history 81001/P2 group. A later storage interruption during receipt persistence required replacement of the full eight-cell history 81002/P2 group. The interrupted sequence passed its initial-state check and reached the target fault, but its missing receipts were not reconstructed. Both groups use attempt-002; all other groups use attempt-001. Experimental conditions, timing, scoring, and statistical definitions are unchanged. The attempt inventory records exclusions; the public raw archives contain the selected 160 sequences. Original excluded receipts remain in the local provenance archive.

## Export integrity
Export manifests authenticate sanitized copies. Historical source seals retain their original hashes and do not authenticate redacted bytes. Local paths and environment account identifiers are replaced, while source contents, logical IDs, ordering, timestamps, durations, results and seeds are retained. The content module uses the included pinned upstream facts source. This path adjustment changes no scoring function.
