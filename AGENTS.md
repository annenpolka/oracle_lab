# Oracle Lab contributor rules

- Read and follow `ORACLE_PRESERVATION.md`; its invariants are normative.
- Read `.stratal/brief.md` before judgment-heavy design or implementation work.
  Treat it as the repo-local working contract for recurring human intent, update
  it when that intent or its validation evidence changes, and keep the change
  compact. Stratal may clarify working defaults but never relax
  `ORACLE_PRESERVATION.md` or rewrite historical records.
- For Host coding-worker changes, follow the living
  `coding_agent_operational_integration_execplan.md` and update its progress,
  decisions, discoveries, and outcomes with the implementation.

- Preserve raw oracle output byte-for-byte. Analysis and rendering are derived data.
- Append events; never repair history in place. Projections must be rebuildable.
- Keep oracle, host, and human decisions distinct in code and event actors.
- Never execute model-authored shell commands on the host. Route them through the tool broker.
- Real shell execution must use the container sandbox with network, credentials, and host mounts disabled.
- Every derived record must retain a source event ID or explicit provenance edge.
- Add or update tests for observable behavior and run `ruff check .` plus `pytest` before handoff.
