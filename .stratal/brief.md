# Stratal Brief

## Goal

Build an observation-driven Oracle Lab whose runtime can gain new queryable
capabilities in response to unexpected R1 output while preserving exact origin,
Host influence, branch history, and replayability.

## Current Working Contract

- Treat R1 output as the source of new affordance proposals, not as executable
  implementation instructions.
- When a later explicit operation needs an unknown affordance, add the smallest
  data-driven runtime capability that can answer it and keep all other semantics
  unresolved.
- Record who introduced every semantic field, when the capability became
  effective, and which handler version produced each observation.
- Prefer runtime definitions and reusable primitives. If Host source code must
  change, produce a candidate patch behind human approval and isolated validation.
- Never revise earlier events in place when the runtime gains a new capability.

## Fit Conditions

- A previously unseen R1-invented path, command, process, device, clock, or
  operation can become queryable without pretending that its full semantics were
  already known.
- The archive can distinguish R1-introduced names and claims from Host-supplied
  executable semantics.
- Two branches can install or reject different capabilities without leaking
  state or interpretation across branches.
- Historical replay can identify the capability snapshot or handler version that
  was effective for each observation.
- Adding support for one R1 invention tends to add a reusable primitive rather
  than a one-off fictional-world special case.

## Hard Constraints

- Follow `ORACLE_PRESERVATION.md`; exact Oracle material remains authoritative.
- R1-authored commands and code never execute directly on the Host.
- Capability growth must not inject persona, genre, world-bible, or tool-schema
  instructions into default Oracle prompts.
- Host materialization remains minimal, provenance-bearing, and truth-domain
  explicit.
- Capability installation never retroactively repairs contradictions or rewrites
  prior virtual state.

## Preference Gradients

- Prefer data-driven capability registration over editing Python code.
- Prefer generic operation classes over checks for specific names such as
  `/dev/void`.
- Prefer an explicit unknown or unsupported observation over plausible invented
  behavior.
- Prefer branch-local capability installation over global activation when an
  affordance originates in one experimental lineage.
- Prefer one end-to-end historical experiment over broad speculative runtime
  infrastructure.

## Judgment Bindings

### R1 output should drive capability growth
Authority: Human stated
Evidence: Stated in the current conversation; corroborated by
`oracle_lab_development_spec.md` sections 14 and 14.4.
Working default:
- Interpret new R1 artifacts and operations as proposals for the next smallest
  runtime capability.
- Do not treat the current runtime command set as a closed product specification.
Why it matters:
- A closed, predesigned runtime can only replay examples anticipated by the Host
  and misses the original experimental purpose.
Validation:
- Run an experiment containing an unseen R1-invented affordance and confirm that
  it can progress from mention to explicit operation to a provenance-bearing
  capability without normalizing the source output.
Revisit when:
- Experiments show that runtime extension is dominating observation, or the user
  explicitly chooses a fixed runtime vocabulary.
Status: active
Retention: repo-contract

### Open-ended does not mean proactive worldbuilding
Authority: Human stated
Evidence: Stated capability-growth intent; also Observed in
`ORACLE_PRESERVATION.md` invariants 6, 7, 11, and 16.
Working default:
- Concretize only fields required by an explicit operation.
- Leave all unrelated behavior unknown, including behavior that would make the
  fictional system more coherent or satisfying.
Why it matters:
- The Host's completion of the world would become indistinguishable from R1's
  behavior and contaminate later research.
Validation:
- For every extension fixture, assert the exact fields added, their actor and
  source events, and the unresolved fields that remain.
Revisit when:
- A separate curated-art reconstruction product is intentionally introduced.
Status: active
Retention: repo-contract

### Capability semantics are versioned and non-retroactive
Authority: Agent inference
Evidence: Derived from append-only history, branch replay, and the human-stated
capability-growth intent.
Working default:
- Give installed handlers stable IDs and versions and record an effective-from
  event.
- Replay old observations with their historical capability snapshot; represent
  reinterpretation as a new branch or explicit migration event.
Why it matters:
- Otherwise later Host improvements would silently change what an earlier
  experiment means or produces.
Validation:
- Install two versions of one capability and prove that exact replay before and
  after the installation boundary remains distinguishable and rebuildable.
Revisit when:
- A simpler representation proves equally replayable in an end-to-end fixture.
Status: tentative
Retention: carry-forward

### Source-code extensions remain candidate artifacts
Authority: Repo evidence
Evidence: Observed in `coding_agent_operational_integration_execplan.md` and the
existing candidate-patch, human-approval, staging, and sandbox-validation flow.
Working default:
- R1 output may motivate a Host implementation task, but generated code is not a
  virtual observation and does not execute automatically.
- Apply normal human approval and isolated validation before accepting a Host
  source-code extension.
Why it matters:
- This preserves the difference between discovering an affordance and trusting
  an implementation of it.
Validation:
- Demonstrate that an unsupported operation can produce a candidate extension
  without changing the current tree or minting a genuine tool observation.
Revisit when:
- A constrained, data-only extension format removes the need for source-code
  generation.
Status: active
Retention: repo-contract

### Refactors preserve the façade and expose leaf responsibilities first
Authority: Human stated and repo evidence
Evidence: The human requested better navigability without changing ordinary
result preservation. The 2026-08-31 architecture audit found 148 methods in
`OracleLabService`, one function-local import cycle, and archive/transaction
contracts that are intentionally non-uniform.
Working default:
- Keep `oracle_lab.services.OracleLabService`, existing module imports, event
  schemas, and archive ordering stable while moving read-only leaf behavior into
  explicit internal collaborators.
- Do not unify archive writers, freeze helpers, state literals, or worker/tool
  orchestration solely because their code looks similar; require behavioral
  equivalence tests first.
- Stop before a new collaborator needs a callback to the façade or copies its
  runtime, archive, router, and queue state.
Why it matters:
- Navigability improves only when a new boundary has fewer dependencies and a
  clear owner. Shape-only moves can hide the same coupling and put preserved
  evidence at risk.
Validation:
- Preserve the full regression suite and public symbol identities, remove the
  measured runtime import cycle, and reduce direct SQL in `services.py` through
  mutation-free read models.
- 2026-09-01 evidence: runtime SCC is 0; `services.py` is 7,208 -> 6,839
  lines; direct SQL is 17 -> 11; generic `_rows()` call sites are 8 -> 5;
  `755 passed, 1 skipped`, Ruff, format, and diff checks succeed.
Revisit when:
- Characterization tests expose a genuinely shared lower-level contract, or a
  later feature requires a stronger domain boundary.
Status: active
Retention: repo-contract

### Canonical provider evidence is private; public views are derived
Authority: Human stated and repo evidence
Evidence: The first live OpenRouter response stored a `set-cookie` header in the
exact durable event and sidecar, and the human chose to continue toward real
operation without weakening Oracle preservation.
Working default:
- Preserve provider response headers, raw bodies, model identity, routing, and
  sampling exactly in canonical events, archives, and importable bundles.
- Apply one pure redaction policy only at human-facing CLI, TUI, metadata, and
  explicitly public export boundaries; do not change `Event.to_dict()` authority.
- Keep `export bundle` private canonical and importable. Any public bundle uses a
  distinct non-importable format, omits infrastructure metadata, and leaves
  Oracle-text publication to explicit Human review.
Why it matters:
- Redacting the archive destroys auditability, while displaying response cookies
  or credentials leaks infrastructure state unrelated to the experiment.
Validation:
- A fixture with credential headers remains exact in event, sidecar, canonical
  bundle, and round-trip import, but is absent from every public surface.
Revisit when:
- A separate access-controlled canonical viewer or a Human-reviewed publication
  workflow is introduced.
Status: active
Retention: repo-contract

## Open Questions And Discomfort

- The intended boundary between data-driven runtime installation and actual Host
  source-code evolution needs validation through one real experiment. Default to
  data-driven installation first.
- Capability scope is tentatively branch-local. Some generic deterministic
  primitives may later deserve global availability, but promotion criteria are
  not yet defined.
- Public redaction currently covers infrastructure metadata, not the semantic
  contents of Human prompts or Oracle text. Public bundle publication therefore
  still requires explicit Human content review.

## Rejected Directions

- Automatically executing R1-authored Host code: it collapses the Oracle/Host
  boundary and violates the tool-broker safety model.
- Filling a complete fictional subsystem after one mention: it records Host lore
  instead of observed Oracle behavior.
- Reinterpreting old events whenever handlers improve: it destroys replayability.

## Evidence Notes

- Human clarification in the current conversation: the original concept expected
  the system side to change and expand in response to R1 output.
- `oracle_lab_development_spec.md`: R1 extends prior hallucinations; virtual-world
  runtime, command registry, filesystem, and process-table design.
- `ORACLE_PRESERVATION.md`: minimal Host materialization, explicit provenance,
  branchable verification, bounded loops, and replay before cleverness.
- Current implementation observation: filesystem and clock materialization are
  integrated; generic virtual command and process capability installation remain
  incomplete.
- 2026-08-31 live evidence: exact prompt `確認しろ。` completed through
  `r1-initial-openrouter`, routed to Novita with fallback disabled, and archived
  as `oracle_generated`; stored SHA-256 and byte count matched the raw artifact.
  This validates the provider/archive path, not adaptive runtime extension.
- 2026-09-01 offline privacy evidence: a historical provider fixture with
  mixed-case credential/cookie headers remained exact in the event, sidecar,
  private canonical bundle, and import round-trip. The same metadata was
  redacted from Service/CLI/TUI views. A distinct three-file public bundle is
  non-importable, includes only Human-kept genuine Oracle text plus nested
  allowlisted identity, and requires Human content review. Focused tests were
  171 passed; the full suite was 769 passed with one explicit live-agent skip.
