# Oracle Lab

Oracle Lab is a local-first, event-sourced environment for exploring historical
language-model behavior. It preserves the exact oracle response, the visible
conversation that produced it, host analyses, tool activity, branches, and
human curation as separate, traceable events.

The implementation follows three non-interchangeable roles:

- **Oracle:** generates raw responses; it is not given an agent framework by default.
- **Host:** stores, analyzes, dispatches, and brokers tools without rewriting oracle text.
- **Human:** decides what to keep, reject, fork, pin, canonize, quarantine, or revisit.

## Quick start

Python 3.13 or newer and `uv` are recommended.

```sh
uv sync --extra dev
cp .env.example .env
chmod 600 .env
# Set OPENROUTER_API_KEY in .env. The file is ignored by Git.
uv run --env-file .env oracle --help
uv run --env-file .env oracle session new --title experiment --model r1-initial-openrouter
uv run --env-file .env oracle ask "確認しろ。" --model r1-initial-openrouter
uv run --env-file .env oracle run --until-human
uv run --env-file .env oracle pause --note "inspect before continuing"
uv run --env-file .env oracle resume
uv run --env-file .env oracle quarantine evt_... --note "needs independent evidence"
uv run --env-file .env oracle revisit evt_... --note "compare against the next branch"
```

Provider credentials are optional for replay, archive, virtual-world, and
deterministic-tool workflows. To call OpenRouter, set `OPENROUTER_API_KEY` and
select the `r1-initial-openrouter` profile from `config/models.toml`.

## Storage

- SQLite in WAL mode is the authoritative append-only event log.
- Projection tables are disposable and rebuildable from events.
- Provider bodies are archived byte-for-byte under
  `archive/raw/YYYY/MM/DD/<event-id>.json`; sidecar metadata and SHA-256 keep the
  provider envelope auditable.
- Rendered Markdown and search indexes are caches, never archival replacements.
- Human/orchestrator prompts, including whitespace, are stored unchanged and can
  be correlated with output attractors using `oracle research prompt-attractors`.

Every `oracle.output` has an immutable material origin. Provider calls are
`oracle_generated`, imported records are `historical_fixture`, and direct
structural test fixtures are `synthetic_fixture`. Unlabelled outputs are rejected;
synthetic lineage is excluded transitively from genuine claims, motifs, curation,
retrieval, transcripts, bundles, and selected corpora. Genuine outputs also require
the requested and returned model identity, routing/fallback state, exact sampling,
context hash, timestamp, API metadata, and committed raw-archive reference.
The production HTTP adapter marks a successful provider response as
`oracle_generated`; replay and custom in-process providers default to
`synthetic_fixture` unless their historical-fixture origin is explicit.

Historical JSON/JSONL logs can become immutable session ancestry:

```sh
uv run oracle session import old-session.json --title "initial R1 experiment"
uv run oracle replay exact --session ses_...
uv run oracle replay host evt_... --host-profile frontier-v2
```

Missing historical provider, sampler, routing, or system-prompt fields remain
explicitly unknown; the importer never guesses them.

A portable research bundle is imported through the same command. The importer
verifies every manifested file and event relationship, rebuilds disposable
projections from the event log, and copies provider bytes into the destination's
write-once archive so the reconstructed session does not depend on the bundle's
location:

```sh
uv run oracle session import bundle-dir
uv run oracle session import curated-bundle-dir --authorize-human-curation
```

Bundles containing keep/star/note or other human curation fail closed unless the
CLI user supplies the explicit authorization flag. Programmatic callers must
also provide an explicit human actor; importing a directory never silently
manufactures one.

## Safety model

Model-authored commands are text until an explicit `tool.request` is approved.
Deterministic arithmetic tools run in-process with restricted parsers. Real
shell commands have no host fallback: they run only in an ephemeral container
with networking, host mounts, credentials, and a writable root disabled. Web
verification and real shell access require explicit policy approval by default.
Structured output from analysis-task coding agents and direct host workers may
append only `analysis.*` proposals. Repository-edit workers use a separate
artifact path and must not emit research-analysis events. Claim lifecycle,
canon, entity, relation, virtual-world, and usage events are emitted only by
trusted deterministic application paths.
Keep/star/canon decisions require a human actor; synthetic fixtures are never
included in the genuine selected-corpus export.

Canon candidates are nominations only. An explicit CLI action records the
claim-specific Human approval and the deterministic canonical promotion as one
atomic, idempotent operation:

```sh
uv run oracle canonize evt_...
```

The candidate event must be `analysis.canon_candidate` on the claim's branch.
Synthetic-fixture and coding-worker lineage cannot be canonized, even by this
Human action.

Every tool result carries one truth domain: `real`, `sandbox`, `virtual`,
`retrieved`, or `synthetic`. Verification is disabled until exact HTTPS hosts are
listed in `config/tools.toml`; approval creates a separate verification branch.
The original exploratory branch is not fact-checked automatically.
An old log's `role=tool` message with no recorded domain remains a historical
context message with `truth_domain_status=unknown_historical`; it is never
promoted to a verified `tool.output` and is never relabeled with a guessed domain.

Tool/oracle loops carry a correlation ID, depth, remaining budget, and equivalent-
event signature. They emit `system.automation_stopped` on human gates, repeated
results, depth/budget limits, provider/tool failure, and respect explicit
`human.pause` / `human.resume` events.

The persistent virtual clock is deliberately sparse. Mentioning a clock or a
time in Oracle text never creates one, and querying an unknown clock returns an
error without consulting the Host wall clock. Only these explicit virtual
operations can create or mutate clock state:

```text
clock create [ID]
clock set [ID] VALUE UNIT
clock advance [ID] DELTA UNIT
clock query [ID]
```

`create` records an unknown value and unit as a Host-originated concretization
event. `set` and `advance` require literal finite decimal values; units are
opaque tokens and are never inferred or converted. Every revision remains in
the event log and rebuildable projection. A differing second `set` preserves
both readings and emits an unresolved virtual-clock contradiction instead of
repairing the earlier reading. Clock tool observations carry
`truth_domain=virtual` and do not claim factual or sandbox truth.

## Coding-agent workers

Codex/OpenCode operational integration is fail-closed and disabled by default.
The intended configuration surface is `config/agents.toml`, with each worker
explicitly declaring `enabled`, adapter, executable, model, timeout, output
limit, sandbox profile, allowed environment-variable names, and any permitted
fallback. Missing configuration, a disabled profile, or a missing Direct API
worker must not silently start a more capable coding agent.

The repository ships a disabled-by-default `config/agents.toml` plus the complete
loader, router, durable task/run archive, candidate-patch, Human gate, persistent
staging, and sandbox-validation pipeline. Every checked-in worker profile remains
disabled, so the default CLI does not launch Codex or OpenCode. Current status and
acceptance criteria are tracked in
[`coding_agent_operational_integration_execplan.md`](coding_agent_operational_integration_execplan.md).
The checked-in router also keeps `isolation_backend="disabled"`. A coding
profile can become active only when the router selects
`docker-sbx-microvm`, the worker selects `sandbox_profile="external-broker"`,
and `bind()` produces a complete conformance attestation. Codex sandbox flags
and OpenCode wrapper paths remain execution settings, not proof of OS
isolation. Deterministic fake adapters are available only through explicit
dependency injection in contract tests.

The staged Docker `sbx` integration has strict, synthetic-fixture coverage for
hashing the resolved broker executable, the reviewed `v0.39.x` protocol, a
digest-pinned template, absence of effective global/org network allows, and a
disposable clone lifecycle. Its parser rejects unknown schemas, identity/policy
drift, cleanup failure, unsafe archive entries, and missing evidence. A bounded
Oracle Lab Workspace Archive is revalidated before any Host materialization.
These mechanisms are necessary plumbing, not yet a production isolation proof.

This backend has deterministic protocol tests, but production `bind()` is
deliberately unavailable and fails before starting `sbx`. A security audit found
that the current probes do not yet prove workspace quiescence after detached
children, guest `.git` integrity, data-plane network/credential behavior, or the
actual template instance used by a created sandbox. The documented `sbx v0.39`
template inventory also does not expose the requested registry digest. Do not
replace any of these checks with a tag, image ID, CLI claim, or synthetic
fixture: the missing evidence must be measured before a production attestation
can be issued.

Synthetic `sbx` protocol doubles remain available only to contract tests. Their
backend, receipt, and every capability check are labeled `synthetic_fixture`.
They never activate the standard service, count as a real tool observation, or
justify a live coding-agent run. Failed synthetic worker runs (timeout, output
limit, or nonzero exit) are cleanup-only and never export a candidate workspace.

### Coding-worker readiness

Inspect the checked-in worker configuration without creating the Oracle Lab
database, binding the isolation broker, starting a sandbox, or invoking an
agent/model:

```sh
uv run oracle worker readiness
```

Use `--agents-config /absolute/path/to/agents.toml` for an operator-owned
configuration outside the target repository. The command emits stable JSON and
intentionally exits non-zero while blocked. It can confirm static prerequisites
such as an exact-host allowlist, a digest-shaped template reference, and the
resolved broker executable hash, but it never issues production attestation or
enables a profile.

The current blocker IDs are `workspace_quiescence`,
`guest_git_control_integrity`,
`data_plane_network_and_credential_enforcement`,
`actual_template_instance_identity`, `sandbox_ownership`, and
`profile_workspace_binding`. They must be cleared by measured production
conformance, not by changing readiness output.

After static readiness, an operator can run an explicitly gated, read-only
control-plane observation. It executes only `sbx version`, `sbx ls --json`, and,
when requested, `sbx inspect NAME --json`. It never creates, enters, stops, or
removes a sandbox and never starts Codex/OpenCode or an Oracle model:

```sh
uv run oracle worker isolation probe \
  --archive-root /absolute/operator-owned/path/sbx-observations \
  --observe-read-only-control-plane
```

Pass `--sandbox-name NAME` to archive two independent name-selected views of an
already-existing sandbox. The report deliberately does not join the UUID from
`ls` to the workspace/image fields from `inspect`; it fixes
`atomic_instance_binding_proven=false`. Exact bounded argv, stdout, and stderr
are committed to a write-once archive with mode `0600`; public JSON contains
hashes, byte counts, and explicit provenance edges instead of raw command data
or archive filesystem paths. A successful probe reports `status=observed`, while
`ready=false`, `safe_to_start_worker=false`, and `attestation_issued=false`
remain fixed.

Automated `sbx create`/`sbx rm` is intentionally absent. In v0.39, mutation is
name-selected and the observed server UUID is not an atomic/CAS selector, so a
pre-removal identity check cannot prevent name reuse in the check-to-use gap.
Sandbox startup can also inherit Docker-managed skills, MCP, policy, or service
credential scope unless those boundaries are independently proven. The six
production blockers therefore remain in force.

On Apple silicon macOS, Docker's standalone CLI is installed and initialized
separately from the legacy `docker sandbox` plugin:

```sh
brew trust docker/tap
brew install docker/tap/sbx
sbx login
sbx secret set openai --oauth
sbx diagnose -o json
```

`sbx login` authenticates the Docker Sandboxes service; the separate
`sbx secret set openai --oauth` flow prepares Codex's proxy-managed OpenAI
credential in the Host keychain. Neither successful login is evidence that the
Oracle Lab isolation contract has passed.

Initialize the global network policy to `deny-all` before a no-model
conformance sandbox if it is not already initialized. Do not treat a successful
`sbx diagnose` as Oracle Lab attestation. Keep the checked-in worker profiles
disabled; static readiness, a real no-model `sbx` conformance run, and a
Human-authorized live-agent smoke are separate gates.

The lightweight `direct` adapter is a separate Host-model path and does not use
the Oracle profiles in `config/models.toml`. To enable it, the operator must set
`router.enabled=true`, `workers.direct.enabled=true`, and explicitly configure
the Host-only `model`, `host_provider_kind`, `host_provider_id`,
`host_base_url`, `host_api_key_env`, and sampling fields in
`config/agents.toml`. The checked-in profile remains disabled. Credential values
are resolved only from the named environment variable and are never put in
events or archives. Response bodies are read as a stream with the profile's
`max_output_bytes` as a hard capture bound. A response within that bound is
preserved byte-for-byte under `archive/workers/`; an oversized response is an
explicit `output_limited` failure whose archive contains only the bounded raw
prefix. If a configured credential value is reflected in a response body, the
body is not persisted at all and the call fails with a recorded credential
quarantine disposition. Response headers and API metadata are redacted both by
sensitive header name and by known credential value.

The Direct Host request explicitly records the requested provider and fallback
policy. When fallback is disabled, an observed provider mismatch or explicit
fallback routing response is a `HostProviderError`, not a completed analysis;
when the provider does not report enough routing identity, fallback remains
`unknown` rather than being guessed. Successful calls record requested and
returned model/provider, routing, sampling, redacted API metadata, and usage,
and emit derived analysis with a Host actor and `host_generated` run provenance.
They never create an `oracle.output` event or silently borrow the R1 model
identity.

Repository editing and prompting the Oracle are separate capabilities:

- `repository-edit` produces only a `worker_generated` candidate patch from an
  isolated repository workspace. Worker prose, code, and diff text never become
  Oracle context, genuine corpus material, claims, motifs, or curation entries.
- `prompt-oracle` must use the configured `OracleProvider`, preserve the exact
  prompt and model identity, and archive the actual provider response. A coding
  agent must never imitate R1 or supply synthetic R1-like text as a substitute.

The required patch lifecycle is:

    candidate patch
      -> deterministic security preflight
      -> explicit human approval or rejection
      -> persistent staging worktree apply
      -> sandbox validation

Security preflight checks the archived patch hash, base commit, target
preconditions, paths, file modes, symlinks, and submodules before a Human gate is
created. Approval only enqueues durable application work; it does not apply the
patch inside the CLI process. Application never targets the user's current
worktree, and neither the agent nor Oracle Lab may automatically commit, push,
merge, keep, star, or canonize a patch.

Worker prompts, command metadata, raw stdout/stderr, and candidate patches are
stored under the write-once `archive/workers/` namespace with SHA-256 and byte
counts. Configured lint/test/type-check results are observations from the
container sandbox and must carry `truth_domain=sandbox`; an agent-written claim
such as "tests passed" is not validation evidence.

The operational CLI flow is:

```sh
uv run oracle worker enqueue repository-edit \
  --source evt_... --goal "Implement the cited change only." \
  --repository /absolute/path/to/repository
uv run oracle run --until-human
uv run oracle worker patch show evt_...
uv run oracle worker patch approve evt_...
uv run oracle run --until-human
uv run oracle worker patch status evt_...
```

`patch reject` records an alternative explicit Human judgment. Approval merely
enqueues the durable apply job; `oracle run` performs staging application and any
configured validation commands.

The live-agent test is operator-only and excluded from normal operation:

```sh
ORACLE_LAB_RUN_LIVE_AGENT_TESTS=1 \
ORACLE_LAB_LIVE_AGENT=codex \
ORACLE_LAB_LIVE_SBX_TEMPLATE='registry.example/codex@sha256:<64-lowercase-hex>' \
ORACLE_LAB_LIVE_ALLOWED_HOSTS='api.example.invalid,auth.example.invalid' \
  uv run pytest -m live_agent tests/test_live_agent_opt_in.py -q
```

Without the opt-in variable, digest-pinned template, and explicit exact-host
list, the test skips before any subprocess can start. `ORACLE_LAB_LIVE_SBX_EXECUTABLE`
may select a non-default `sbx` binary. Once all gates are present, production
broker binding runs first; with the current explicit fail-closed guard the coding
agent still cannot start. After the missing evidence is implemented, the smoke
target will be a newly created fixture repository whose source tree must remain
unchanged while the isolated export produces a candidate patch. The test may
contact an external model and incur cost, so it must never be enabled by a normal
test run. No live agent smoke has been run for the implementation described
above.

## Research and replay commands

```sh
uv run oracle sample --session ses_... --from evt_... -n 20 \
  --temperature 0.6 --top-p 0.95
uv run oracle compare-models --session ses_... --event evt_... \
  r1-initial-openrouter r1-1776-q4-local
uv run oracle research contradiction-mechanisms --session ses_...
uv run oracle research latex-prefixes --session ses_...
uv run oracle research prompt-attractors --phrase 報告書 --session ses_...
uv run oracle export bundle bundle-dir --session ses_...
uv run oracle export transcript transcript.md --session ses_...
uv run oracle export corpus selected.jsonl --session ses_...
```

Research bundles require an absent or empty destination directory; exports
never merge with stale files from an earlier session.

Host branch proposals use the configured `branch_creation` gate. A gated
proposal is materialized only after `oracle session approve-fork <proposal-event>`.
Human keep/star/canon actions can never be inferred from novelty, coherence,
factuality, embeddings, or Host preference.

## Development

```sh
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

The complete design and acceptance criteria live in
[`oracle_lab_development_spec.md`](oracle_lab_development_spec.md). The additional
preservation contract in [`ORACLE_PRESERVATION.md`](ORACLE_PRESERVATION.md) is
normative for implementation, fixtures, imports, tools, and future changes.
