# Native fault-site map

Actual installed source (read-only): replication/.venv/Lib/site-packages/cognee/api/v1/update/.
The exact installed files are copied into inputs/pinned_sources and hashed in INPUT_INDEX.json / FREEZE.json.

- update.py: normal default request enters incremental_update; only IncrementalUpdateNotPossible is the eligibility fallback exception.
- incremental.py, _run_incremental_update: invokes _write_and_publish and records ordinary exceptions before rethrowing. The experiment preserves this handler.
- incremental.py, _write_and_publish: awaits add_data_points for new derived objects, then delete_chunks_incremental, then publish_updated_data.

P1 wraps the actual imported delete_chunks_incremental symbol: after the awaited new derived write returned, before delegating nonempty deletion. P2 wraps the actual imported publish_updated_data symbol: after the actual nonempty deletion returned, before canonical publish. It does not raise from a read-only observer or manufacture an API result.

Both conditions install the same wrappers. NO_FAULT forwards unchanged. INJECT consumes the one-shot flag before throwing InjectedIncrementalFailure(RuntimeError), and only in operation target_v1. Setup and all reads are outside the hook. After the target returns, injection is disarmed; all original hook targets are restored before closing the owner.

Each site receipt records actual incremental entry, write-body entry, new write ACK, nonempty withdrawal IDs, expected delete state, and canonical-not-started. Backend request/ACK records remain the underlying evidence. Failure to meet prerequisites is a harness error, not a desired scientific outcome; a genuine pre-site fallback is separately retained.

Scope: two ordinary exception points in one-owner synthetic updates. No claim about crash persistence, all failure locations, or source/graph/vector atomic commit.
