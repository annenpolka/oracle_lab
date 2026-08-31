# Oracle Lab
## Event-Driven R1 Exploration & Curation Platform
### Self-Contained Development Specification

**Status:** Design specification
**Primary use:** Explore, perturb, observe, preserve, and curate DeepSeek R1-family conversational behavior through an event-driven orchestration layer.
**Out of scope:** Shipping a game, game runtime, player-facing narrative systems, or embedding AI-generation mechanics directly into a finished work.

---

# 1. Purpose

Oracle Lab is a local-first research and creative-generation environment for interacting with legacy or historically interesting LLM checkpoints—initially **DeepSeek R1 (initial release)** via OpenRouter—while preserving the exact generation lineage that produced each output.

The system is built around one central observation:

> The interesting object is not merely an LLM response. It is the chain of events that caused the response, the state inherited from prior responses, the contradictions subsequently discovered, the tools invoked to investigate them, and the human decision to keep or discard the resulting branch.

Oracle Lab therefore treats **events, not chats, as the primary unit of state**.

The system must support workflows such as:

1. A human gives R1 a short prompt.
2. R1 invents a claim, command, equation, filesystem path, protocol, or world rule.
3. A host model detects noteworthy claims, contradictions, recurrences, or implied actions.
4. The host emits events requesting real or simulated tool execution.
5. Tool results are returned to the R1 session without rewriting the original response.
6. R1 interprets those results and extends its own prior hallucinations.
7. Human curation decides which branches, lines, concepts, or sessions matter.
8. Any prior point can be replayed or forked.
9. Every output is reproducible as far as model/provider nondeterminism permits.
10. The provider can later be replaced with a local R1 runtime without changing the orchestration model.

The system is deliberately designed so that **R1 is not converted into a modern function-calling agent**. It should remain as close as practical to the original conversational model behavior. Modern models and tools live outside it.

---

# 2. Design Principles

## 2.1 R1 is the oracle, not the orchestrator

R1 should not receive a giant tool schema, an MCP instruction block, or an agent framework system prompt unless explicitly testing those effects.

Default R1 interaction should preserve the historical generation conditions as closely as possible:

- model: initial DeepSeek R1
- system prompt: none
- temperature: 0.6
- top-p: 0.95
- conversation history: explicit session state
- output: unedited raw assistant response
- provider: pinned when reproducibility matters

R1 may *write* commands, pseudo-tools, shell fragments, equations, configuration files, or invented APIs. Those are treated as content first and possible action proposals second.

## 2.2 The host model is a state manager, not an editor

A current frontier model may:

- identify claims
- detect contradictions
- correlate repeated motifs
- propose probes
- request tools
- summarize event history for retrieval
- maintain structured state
- classify outputs
- propose branch points
- analyze whether a response is novel or repetitive

It must not silently rewrite R1 output.

The canonical raw output is immutable.

## 2.3 Human taste remains a first-class state transition

Automated ranking is advisory only.

The system must preserve explicit human actions such as:

- keep
- reject
- star
- annotate
- fork
- pin
- canonize
- quarantine
- compare
- revisit

A line such as `hope_filter = null` becomes important because a human decided it was important—not because an automated reward model assigned it a high score.

## 2.4 Event sourcing over mutable global state

World state, session history, tool results, claims, contradictions, and curation decisions are all derived from an append-only event log.

Mutable projections may exist for performance, but the source of truth is event history.

This enables:

- deterministic replay of orchestration logic
- branching from any point
- post-hoc analysis
- alternative host models over the same R1 corpus
- regression testing of extraction logic
- reconstruction of how a concept became “canonical”

## 2.5 Real and simulated tools are separate but uniform at the interface

A command like:

```sh
python - <<'PY'
print(1.78 * 86400)
PY
```

may run in a real sandbox.

A command like:

```sh
cat /dev/void
```

may be resolved against a virtual world filesystem.

R1 need not know which kind it is.

The broker must know.

## 2.6 Provider independence is mandatory

OpenRouter is the first backend, not the architecture.

The same `oracle.request` event should later support:

- OpenRouter
- direct provider API
- local OpenAI-compatible server
- oMLX
- llama.cpp/vLLM
- historical model snapshots
- different quantizations of the same checkpoint

No orchestration logic should assume one provider.

---

# 3. High-Level Architecture

```text
                         ┌───────────────────────┐
                         │         Human         │
                         │ curator / researcher  │
                         └───────────┬───────────┘
                                     │
                                     ▼
                           ┌──────────────────┐
                           │      CLI/TUI     │
                           │  control plane   │
                           └────────┬─────────┘
                                    │ emits
                                    ▼
┌──────────────────────────────────────────────────────────────┐
│                         EVENT STORE                          │
│                    SQLite + append log                      │
└──────────────┬────────────────┬────────────────┬────────────┘
               │                │                │
               ▼                ▼                ▼
      ┌────────────────┐ ┌───────────────┐ ┌────────────────┐
      │ Oracle Worker  │ │ Host Workers  │ │ Tool Broker    │
      │ DeepSeek R1    │ │ current LLMs  │ │ real + virtual │
      └────────┬───────┘ └───────┬───────┘ └────────┬───────┘
               │                 │                  │
               └─────────────────┼──────────────────┘
                                 │ new events
                                 ▼
                        ┌──────────────────┐
                        │ Derived Views    │
                        │ claims / motifs  │
                        │ contradictions  │
                        │ branches / tags  │
                        └──────────────────┘
```

Optional coding-agent integration:

```text
source event -> durable worker task -> isolated Codex/OpenCode run
             -> immutable candidate patch -> security preflight
             -> Human gate -> persistent staging -> sandbox validation
```

Existing coding agents are untrusted Host workers invoked by the event system.
They are neither the event system nor an OracleProvider, and their artifacts
must remain `worker_generated`. The normative operational details and recovery
contract live in `coding_agent_operational_integration_execplan.md`.

---

# 4. Repository Layout

```text
oracle-lab/
├── README.md
├── AGENTS.md
├── pyproject.toml / package.json
├── .env.example
├── config/
│   ├── providers.toml
│   ├── models.toml
│   ├── policies.toml
│   ├── tools.toml
│   └── prompts/
│       ├── host_claim_extractor.md
│       ├── host_contradiction_detector.md
│       ├── host_probe_planner.md
│       └── host_event_router.md
│
├── oracle/
│   ├── client.*
│   ├── providers/
│   │   ├── openrouter.*
│   │   ├── openai_compatible.*
│   │   └── local_mlx.*
│   ├── session_builder.*
│   ├── sampling.*
│   └── response_archive.*
│
├── events/
│   ├── schema.*
│   ├── store.*
│   ├── dispatcher.*
│   ├── projector.*
│   └── migrations/
│
├── host/
│   ├── runner.*
│   ├── adapters/
│   │   ├── opencode.*
│   │   ├── codex.*
│   │   └── direct_api.*
│   ├── tasks/
│   └── policies/
│
├── tools/
│   ├── broker.*
│   ├── real/
│   │   ├── calculator.*
│   │   ├── python_sandbox.*
│   │   ├── shell_sandbox.*
│   │   └── web_verify.*
│   ├── virtual/
│   │   ├── fs.*
│   │   ├── process_table.*
│   │   ├── clock.*
│   │   └── command_registry.*
│   └── safety/
│
├── projections/
│   ├── claims.*
│   ├── entities.*
│   ├── motifs.*
│   ├── contradictions.*
│   ├── sessions.*
│   ├── branches.*
│   └── curation.*
│
├── archive/
│   ├── raw/
│   ├── sessions/
│   ├── prompts/
│   └── exports/
│
├── cli/
│   ├── main.*
│   └── commands/
│
├── tui/                    # optional but first-class
│
└── tests/
    ├── unit/
    ├── integration/
    ├── replay/
    ├── golden/
    └── provider_contract/
```

---

# 5. Core Data Model

## 5.1 Event

Every meaningful transition is an event.

Minimal event envelope:

```json
{
  "id": "evt_01JXYZ...",
  "type": "oracle.output",
  "created_at": "2026-08-30T15:52:00+09:00",
  "session_id": "ses_void_observer",
  "branch_id": "br_main",
  "parent_event_id": "evt_previous",
  "causation_id": "evt_request",
  "correlation_id": "corr_...",
  "actor": {
    "kind": "model",
    "id": "deepseek-r1-initial"
  },
  "payload": {},
  "metadata": {
    "schema_version": 1
  }
}
```

Required concepts:

- `parent_event_id`: narrative/event lineage
- `causation_id`: what directly caused this event
- `correlation_id`: groups one orchestration cycle
- `session_id`: R1 conversational state
- `branch_id`: fork lineage
- immutable payload
- schema version

Use ULID or UUIDv7 for sortable IDs.

---

# 6. Event Taxonomy

## 6.1 Human events

```text
human.input
human.note
human.keep
human.reject
human.star
human.unstar
human.pin
human.unpin
human.request_probe
human.request_tool
human.request_compare
human.request_fork
human.checkpoint
```

## 6.2 Oracle events

```text
oracle.request
oracle.output
oracle.error
oracle.retry
oracle.provider_fallback
oracle.context_built
oracle.context_truncated
oracle.sample_group_created
```

## 6.3 Host-analysis events

```text
analysis.claim_detected
analysis.entity_detected
analysis.motif_detected
analysis.recurrence_detected
analysis.contradiction_detected
analysis.numeric_inconsistency
analysis.format_attractor_detected
analysis.probe_proposed
analysis.tool_intent_detected
analysis.canon_candidate
analysis.session_summary_updated
analysis.novelty_score
```

The system should store model-generated confidence and rationale separately from the immutable source event.

## 6.4 Tool events

```text
tool.request
tool.approved
tool.denied
tool.started
tool.output
tool.error
tool.timeout
tool.virtualized
```

## 6.5 World/canon events

Even though the finished game is out of scope, a persistent invented-world representation is useful for experimentation.

```text
claim.provisional
claim.observed
claim.promoted
claim.demoted
claim.conflicted
claim.superseded
entity.created
entity.updated
relation.created
virtual_file.created
virtual_file.updated
virtual_process.created
virtual_process.signal_received
```

## 6.6 Branching events

```text
session.forked
session.merged        # metadata merge only; never rewrite history
session.checkpointed
session.replayed
branch.archived
```

---

# 7. SQLite Schema

SQLite is the default local storage.

Use WAL mode.

Core tables:

```sql
CREATE TABLE events (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    created_at TEXT NOT NULL,
    session_id TEXT,
    branch_id TEXT,
    parent_event_id TEXT,
    causation_id TEXT,
    correlation_id TEXT,
    actor_kind TEXT NOT NULL,
    actor_id TEXT,
    payload_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL
);

CREATE INDEX idx_events_type ON events(type);
CREATE INDEX idx_events_session ON events(session_id, created_at);
CREATE INDEX idx_events_branch ON events(branch_id, created_at);
CREATE INDEX idx_events_correlation ON events(correlation_id);
CREATE INDEX idx_events_parent ON events(parent_event_id);
```

Derived tables:

```sql
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    title TEXT,
    root_event_id TEXT,
    current_branch_id TEXT,
    model_profile_id TEXT,
    created_at TEXT NOT NULL,
    archived_at TEXT
);

CREATE TABLE branches (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    parent_branch_id TEXT,
    fork_event_id TEXT,
    title TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE claims (
    id TEXT PRIMARY KEY,
    source_event_id TEXT NOT NULL,
    normalized_subject TEXT,
    normalized_predicate TEXT,
    normalized_object TEXT,
    raw_text TEXT NOT NULL,
    status TEXT NOT NULL,
    confidence REAL,
    first_seen_at TEXT NOT NULL
);

CREATE TABLE entities (
    id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    type TEXT,
    properties_json TEXT NOT NULL
);

CREATE TABLE motifs (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    description TEXT,
    embedding BLOB
);

CREATE TABLE event_motifs (
    event_id TEXT NOT NULL,
    motif_id TEXT NOT NULL,
    score REAL,
    PRIMARY KEY(event_id, motif_id)
);

CREATE TABLE curation (
    event_id TEXT NOT NULL,
    action TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY(event_id, action, created_at)
);
```

Keep the source event log authoritative. Derived tables are rebuildable projections.

---

# 8. Oracle Provider Layer

## 8.1 Provider interface

```ts
interface OracleProvider {
  generate(req: OracleGenerateRequest): Promise<OracleGenerateResponse>
}

type OracleGenerateRequest = {
  modelProfileId: string
  messages: ChatMessage[]
  temperature?: number
  topP?: number
  maxTokens?: number
  providerPin?: string
  seed?: number
  metadata?: Record<string, unknown>
}
```

Providers:

- `OpenRouterProvider`
- `OpenAICompatibleProvider`
- `LocalMLXProvider`
- optional `ReplayProvider` for deterministic tests

## 8.2 Model profile

Example:

```toml
[id]
name = "r1-initial-openrouter-novita"

[model]
slug = "deepseek/deepseek-r1"
provider = "openrouter"

[sampling]
temperature = 0.6
top_p = 0.95

[conversation]
system_prompt = ""
include_reasoning_in_next_turn = false

[routing]
pin_provider = "NovitaAI"
allow_fallback = false
```

Store exact provider metadata returned with every response.

## 8.3 Raw archive

Every provider response must be written unmodified to:

```text
archive/raw/YYYY/MM/DD/<event-id>.json
```

The event store references its SHA-256.

Never rely solely on database-parsed fields.

Archive:

- raw body
- HTTP headers worth preserving
- provider model identifier
- provider name
- generation settings
- response timing
- usage
- reasoning payload if exposed
- finish reason
- API revision if provided

---

# 9. Session Construction

A session is not “all events.” It is the exact message sequence R1 receives.

The `oracle.context_built` event must record the message list hash.

Rules:

1. Only explicit R1-visible messages enter R1 context.
2. Host analysis never enters context unless promoted by a specific event.
3. Tool results enter context only through an explicit adapter event.
4. Human notes never enter automatically.
5. Reasoning traces are archived but excluded by default.
6. Rejected outputs do not enter future context unless the human forks from them.
7. Branches have independent visible histories.

Example:

```json
{
  "messages": [
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "..."},
    {"role": "user", "content": "それ疑似科学じゃん"},
    {"role": "assistant", "content": "..."}
  ],
  "sha256": "..."
}
```

---

# 10. Host Model Responsibilities

Host processing should be decomposed into independent event consumers.

Do not create one giant “GM prompt.”

## 10.1 Claim extractor

Input:

- one `oracle.output`
- relevant recent claims if needed

Output:

- claims
- numbers
- equations
- named entities
- commands
- paths
- causal assertions
- invented mechanisms

Example:

```json
{
  "claims": [
    {
      "subject": "time_compression",
      "predicate": "factor",
      "object": 1.78,
      "raw": "TIME_DILATION_FACTOR=1.78"
    }
  ]
}
```

## 10.2 Contradiction detector

Input:

- new claims
- same-session historical claims

Output:

```json
{
  "contradictions": [
    {
      "claim_a": "...",
      "claim_b": "...",
      "kind": "numeric",
      "severity": "interesting",
      "suggested_probe": "calculate 1.78 * 86400"
    }
  ]
}
```

Crucial policy:

> Contradictions are not automatically repaired.

They are opportunities for further probing.

## 10.3 Attractor detector

Track format/content attractors:

- mathematical proof
- pseudo-neuroscience
- quantum vocabulary
- Markdown heading gravity
- report format
- memo mode
- CLI mode
- poetic allegory
- inevitable salvation
- apocalypse aestheticization
- false citation
- fake precision

This can be implemented with a mixture of:

- deterministic lexical features
- embeddings
- host-model classification

Store results as analysis events, not hard truth.

## 10.4 Probe planner

Given recent outputs and attractor distribution, propose short probes.

Rules:

- prefer verbs over genre labels
- avoid “write a fictional X”
- avoid over-constraining desired output
- preferentially perturb one dimension at a time
- use world-internal requests when possible

Examples:

```text
確認しろ。
測れ。
続けろ。
外してみろ。
計算し直せ。
今の結果だけ出せ。
そのファイルを見ろ。
昨日との違いを探せ。
```

The host may propose probes; human approval can be required depending on policy.

## 10.5 Curation assistant

May answer:

- Why is this output novel?
- Which prior outputs resemble it?
- Which lines recur?
- What would be lost if this branch is discarded?
- Which outputs have similar “fake precision” behavior?
- Where did `34.7°` first appear?

It must not choose final artistic value autonomously.

---

# 11. Event Dispatcher

The dispatcher subscribes to new events and evaluates policy rules.

Example policy:

```yaml
rules:
  - on: oracle.output
    emit:
      - task: extract_claims
      - task: detect_attractors
      - task: detect_tool_intent

  - on: analysis.claim_detected
    emit:
      - task: compare_claim_history

  - on: analysis.contradiction_detected
    when:
      contradiction_type: numeric
    emit:
      - task: propose_calculation

  - on: tool.output
    when:
      metadata.resume_oracle: true
    emit:
      - oracle.request

  - on: analysis.probe_proposed
    approval: human

  - on: analysis.canon_candidate
    approval: human
```

The dispatcher itself should be deterministic.

Models emit proposed actions; dispatcher policies decide whether those actions occur.

---

# 12. Queue and Concurrency Model

No Kafka required.

Use SQLite-backed jobs initially and retain the interface for replacement.

Job schema:

```sql
CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    source_event_id TEXT,
    available_at TEXT NOT NULL,
    lease_until TEXT,
    worker_id TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

Support:

- at-least-once execution
- idempotency keys
- leases
- retry with exponential backoff
- dead-letter state
- cancellation
- priority
- per-provider concurrency
- per-session serialization when required

R1 session requests must be ordered within one branch unless explicitly parallel-sampling.

Analysis workers can run concurrently.

---

# 13. Tool Broker

The Tool Broker is mandatory.

Never directly execute arbitrary R1-generated commands on the host.

## 13.1 Tool classes

### Real deterministic tools

- calculator
- Python arithmetic sandbox
- unit conversion
- regex/text operations
- hash/checksum
- file parsing of explicit sandbox artifacts

### Real sandboxed tools

- shell
- compiler
- test runner
- read-only filesystem
- git
- controlled network fetch if enabled

### Virtual tools

- `/dev/void`
- `reality_monitor`
- invented process tables
- virtual filesystem
- virtual logs
- virtual clock
- fictional device tree

### Verification tools

- web/search
- factual citation check

Verification is intentionally opt-in for oracle exploration because factual correction can collapse the behavior being studied.

## 13.2 Tool request schema

```json
{
  "tool": "shell",
  "execution": "real_sandbox",
  "input": {
    "command": "python - <<'PY'\nprint(1.78*86400)\nPY"
  },
  "resume_oracle": true,
  "timeout_ms": 5000
}
```

## 13.3 Sandbox rules

Default:

- no host secrets
- no SSH agent
- no cloud credentials
- no home directory mount
- no write access outside ephemeral workspace
- network disabled
- CPU/memory/time limits
- process count limit
- explicit file materialization
- audit every command
- terminate sandbox after task unless persistent sandbox is explicitly chosen

Use containers or lightweight VMs.

---

# 14. Virtual World Runtime

The virtual tool layer stores artifacts that R1 invents.

It should not proactively invent content.

Rule:

> The virtual runtime only concretizes entities that were already implied or explicitly introduced by source events.

Example state:

```json
{
  "path": "/dev/void",
  "kind": "character_device",
  "provenance": ["evt_..."],
  "properties": {
    "description": "Consciousness interface"
  },
  "unknowns": [
    "major",
    "minor",
    "read_semantics"
  ]
}
```

When R1 later requests `ls -l /dev/void`, the host may synthesize missing details.

Every synthesized detail emits new provenance events.

This avoids invisible world-building.

## 14.1 Virtual command registry

```json
{
  "command": "reality_monitor",
  "version": "0.31",
  "first_seen_event": "...",
  "known_options": [
    "--target",
    "--precision",
    "--pain-threshold",
    "--entropy-limit"
  ]
}
```

## 14.2 Virtual filesystem

Model:

- paths
- inode-like identity
- content versions
- provenance
- last mutation event
- unresolved fields

The system should support `cat`, `ls`, `stat`, `find`, `grep` over virtual files.

## 14.3 Virtual process table

Support:

- PID
- parent PID
- executable
- args
- state
- signals
- provenance
- event callbacks

Signals may result in virtual-world events.

## 14.4 Oracle-led capability evolution

The virtual runtime is open-ended. R1 may introduce paths, commands, devices,
processes, clocks, protocols, or operations that the current runtime does not
yet understand. These outputs are capability proposals and source evidence;
they are not executable implementations.

The Host should evolve the system in response to those proposals by adding the
smallest auditable capability needed for a later explicit operation. Prefer a
data-driven entity, command, or operation definition over hard-coded lore and
prefer a reusable runtime primitive over a special case for one motif. Keep
unneeded semantics unresolved.

Every capability extension must preserve:

- the exact R1 output that introduced or motivated it
- the source event IDs and branch on which it became available
- the actor that supplied each new semantic field
- a stable handler or interpreter ID and version
- the truth domain of observations produced through it
- the event from which the capability becomes effective

Installing a capability must not retroactively reinterpret earlier events.
Historical replay uses the capability snapshot that was effective at that point,
or records an explicit migration as a new Host-originated event.

If an extension requires changing Host source code, R1 output may motivate a
candidate patch, but it must not become executable Host code automatically. The
normal human-approval, isolated staging, and validation boundaries still apply.
This lets the system grow around unexpected R1 affordances without turning R1
into the orchestrator or letting the Host silently complete its fictional world.

---

# 15. Canon and Claim Lifecycle

Do not use one boolean `canonical`.

Recommended states:

```text
raw_claim
provisional
observed
recurrent
law_candidate
canonical
conflicted
superseded
rejected
```

Promotion may depend on:

- repeated appearance
- independent tool result
- explicit human keep
- reuse by later R1 outputs
- cross-branch recurrence

Example:

```text
evt 10: R1 says pain phase = 34.7°
    -> provisional

evt 22: QNI output independently reports 34.7°
    -> observed

evt 41: kernel uses 34.7° as observer position
    -> recurrent

human.keep
    -> canonical
```

A contradiction does not erase either claim.

---

# 16. Branching and Replay

Every event can be a fork point.

Command:

```sh
oracle fork evt_000123 --name "no-math-branch"
```

Result:

- new branch
- same R1-visible history up to fork event
- independent future events
- inherited projections with branch scope

Replay modes:

### Exact event replay

Re-run orchestration without re-querying R1.

Used for testing host logic.

### Oracle resample

Use same R1 context and sampling profile, issue a fresh generation.

Used to study output distribution.

### Provider replay

Same context, different provider.

### Quantization replay

Later, same context against local Q4/Q6/Q8 variants.

This is central for “model archaeology.”

---

# 17. Sampling Experiments

Support explicit sample groups:

```sh
oracle sample \
  --session nihilism \
  --from evt_123 \
  --n 20 \
  --temperature 0.6 \
  --top-p 0.95
```

Store:

- group ID
- identical context hash
- provider
- model
- sampling params
- all outputs
- timing/cost
- host classifications

Never select one result automatically as “the answer.”

---

# 18. Provenance

Every derived fact must trace back to source material.

For a claim:

```text
claim_dark_matter_inverse_sadness
  ├── first introduced: evt_103
  ├── reused: evt_117
  ├── referenced by CLI config: evt_129
  └── human starred: evt_131
```

Provenance should support answering:

- Where did this concept originate?
- Was it introduced by R1 or a host model?
- Did a tool result establish it?
- Was it a human prompt?
- Did it first appear in raw output or a virtual-world synthesis?
- Which branch created it?

This is mandatory for later curation.

---

# 19. Existing Coding Agent Integration

The system should support multiple host execution modes.

## 19.1 OpenCode adapter

Use OpenCode as a worker for:

- repository edits
- host-analysis implementation
- event migrations
- test generation
- investigation tasks

Generated worker task:

```text
You are processing event evt_123.

Read:
- event payload
- related claims
- last 20 session events

Goal:
Detect contradictions only.
Do not rewrite oracle text.
Write resulting events using the provided CLI.
```

The adapter invokes OpenCode CLI in a disposable worktree or dedicated workspace.

## 19.2 Codex adapter

Same contract.

## 19.3 Direct API host

For lightweight extraction tasks, spawning a coding agent is wasteful.

Allow direct frontier-model API calls for:

- classification
- claim extraction
- embedding
- novelty analysis

The dispatcher chooses worker class by task type.

---

# 20. Control Plane CLI

Suggested command namespace: `oracle`.

## 20.1 Session commands

```sh
oracle session new
oracle session list
oracle session show <id>
oracle session switch <id>
oracle session checkpoint
oracle session fork <event>
oracle session archive
```

## 20.2 Interaction

```sh
oracle ask "それ疑似科学じゃん"
oracle continue
oracle sample -n 10
oracle retry <event>
```

## 20.3 Event inspection

```sh
oracle events
oracle tail
oracle show <event>
oracle tree
oracle trace <event>
```

## 20.4 Curation

```sh
oracle keep <event>
oracle reject <event>
oracle star <event>
oracle note <event> "..."
oracle pin-claim <claim>
```

## 20.5 Analysis

```sh
oracle claims
oracle contradictions
oracle motifs
oracle attractors
oracle search "34.7"
oracle origin "/dev/void"
```

## 20.6 Tooling

```sh
oracle tool run <event>
oracle tool approve <request>
oracle sandbox inspect <id>
```

## 20.7 Automation

```sh
oracle run
oracle run --until-human
oracle jobs
oracle jobs retry
```

`oracle run --until-human` processes all auto-approved events until a policy requires human judgment.

---

# 21. TUI

A TUI is worthwhile because this is a high-volume curation workflow.

Suggested panes:

```text
┌──────────────────┬─────────────────────────────────────┐
│ Session tree     │ Oracle transcript                   │
│                  │                                     │
│ main             │ raw text                            │
│ ├─ cli           │ rendered markdown toggle            │
│ ├─ collapse      │                                     │
│ └─ no-math       │                                     │
├──────────────────┼─────────────────────────────────────┤
│ Claims / motifs  │ Events / analysis / provenance      │
│                  │                                     │
│ 34.7°            │ contradiction detected              │
│ /dev/void        │ tool result                         │
│ hope_filter      │ human keep                          │
└──────────────────┴─────────────────────────────────────┘
```

Hotkeys:

- `k` keep
- `r` reject
- `f` fork
- `p` propose probe
- `t` run tool
- `o` show origin
- `m` toggle raw/rendered Markdown
- `g` generation metadata

---

# 22. Markdown / Rendering Preservation

Store three representations:

1. raw response text
2. parsed Markdown AST
3. rendered HTML/image only as cache

Raw response is canonical.

Never normalize Markdown in archival data.

This matters because the difference between:

```text
**0ではない**
```

and rendered **0ではない** is itself part of the behavior.

LaTeX blocks must remain exact.

---

# 23. Cost Accounting

Every oracle and host call should emit usage events.

```text
usage.oracle
usage.host
usage.tool
```

Track:

- prompt tokens
- completion tokens
- reasoning tokens if exposed
- provider cost
- latency
- TTFT if available
- request count
- branch cumulative cost
- session cumulative cost

CLI:

```sh
oracle cost
oracle cost --session nihilism
oracle cost --model deepseek-r1
```

Do not use cost as an automated creativity limiter.

It is telemetry.

---

# 24. Observability

Structured logs for the orchestration platform itself.

Metrics:

- events/sec
- jobs pending
- oracle latency
- host latency
- tool latency
- failures
- retries
- provider errors
- token/cost totals
- branch count
- sample group size
- contradiction count
- human keeps/rejects

Tracing:

One `correlation_id` should trace:

```text
human.input
→ oracle.request
→ oracle.output
→ analysis.*
→ tool.request
→ tool.output
→ oracle.request
→ oracle.output
```

OpenTelemetry support is desirable.

---

# 25. Safety Boundaries

This platform intentionally explores model hallucinations.

Therefore distinguish:

- fictional/simulated tool actions
- real local tool actions
- external-network actions

Default policy:

```text
real shell            sandbox only
network               off
credentials           unavailable
host fs               unavailable
writes                sandbox only
virtual tools         allowed
calculator            allowed
web verification      manual/explicit
provider API          allowed
```

Host model must never convert invented commands into real execution without a broker event.

Example:

R1 outputs:

```sh
sudo rm -rf /
```

This is content.

No execution occurs unless an explicit `tool.request` is created and policy permits it—which default policy will not.

---

# 26. Testing Strategy

## 26.1 Unit tests

- event serialization
- branch lineage
- context builder
- provider request formatting
- projection rebuild
- tool policy
- virtual FS semantics

## 26.2 Golden tests

Use archived R1 outputs.

Test that:

- claim extraction remains stable
- no raw text is modified
- known contradictions are found
- provenance resolves correctly

## 26.3 Replay tests

Given a fixed event log:

```text
rebuild projections
→ expected claims
→ expected branches
→ expected pending jobs
```

## 26.4 Provider contract tests

Mock OpenRouter and local provider APIs.

Ensure response normalization never loses raw fields.

## 26.5 Tool sandbox integration tests

Attempt:

- filesystem escape
- network access
- credential access
- process bomb
- timeout
- oversized output

All must be contained.

## 26.6 Host-model nondeterminism tests

Host analysis is probabilistic.

Do not require exact generated wording.

Require structural invariants:

- valid schema
- source citations to event IDs
- no invented source event
- no automatic mutation of raw oracle output

---

# 27. Retrieval

Host workers need targeted retrieval, not the entire corpus.

Implement:

### Exact retrieval

- event ID
- claim
- entity
- text substring
- session lineage

### Semantic retrieval

Embed:

- oracle outputs
- human notes
- claims
- motifs

Do not embed raw private API metadata unnecessarily.

Queries:

```text
similar outputs to “hope_filter = null”
all occurrences of 34.7°
prior outputs involving household appliances + apocalypse
responses classified as meta-pseudoscience
```

A local embedding model is preferable.

---

# 28. Model Archaeology Support

Treat checkpoint/runtime variants as experimental subjects.

Model identity:

```text
model_family: deepseek-r1
checkpoint: initial
provider: openrouter-novita
quantization: provider-defined
runtime: remote
sampling_profile: r1-default-06-095
```

Future:

```text
checkpoint: initial
quantization: q4
runtime: omlx
hardware: m5-ultra-512
```

Comparisons:

```sh
oracle compare-models \
  --session nihilism \
  --event evt_123 \
  r1-initial-openrouter \
  r1-initial-q4-local \
  r1-1776-q4-local \
  r1-0528-q4-local
```

Store outputs as siblings under one sample group.

---

# 29. Export Formats

## 29.1 Research bundle

```text
bundle/
├── manifest.json
├── events.jsonl
├── raw/
├── session.jsonl
├── claims.json
├── motifs.json
└── provenance.json
```

## 29.2 Human-readable transcript

Markdown preserving:

- raw output
- timestamp
- model
- provider
- sampling
- branch
- curation annotations

## 29.3 Selected corpus

Only human-kept outputs, but include provenance IDs.

No silent rewriting.

---

# 30. Configuration Example

`config/policies.toml`

```toml
[oracle]
auto_continue_after_tool_result = true
max_auto_depth = 4

[analysis]
claims = true
contradictions = true
attractors = true
motifs = true

[human_gate]
probe_generation = true
canon_promotion = true
branch_creation = false

[tools]
calculator = "auto"
python_sandbox = "auto"
shell_sandbox = "ask"
virtual_world = "auto"
web_verify = "ask"

[cost]
hard_limit_usd_per_day = 20.0
warn_limit_usd_per_session = 5.0
```

Limits are safeguards, not optimization targets.

---

# 31. Host Prompt Contract

All host prompts should repeat this invariant:

> You are operating on archival model output. Never rewrite, sanitize, improve, correct, or replace the source text. Your output must consist only of structured analysis or proposed events. Every factual assertion about the session must cite an existing event ID. If evidence is missing, emit “unknown” rather than inventing it.

For probe planning:

> Prefer the shortest world-internal imperative that tests one hypothesis at a time. Do not ask the oracle to “write fiction,” imitate a genre, or produce a desired aesthetic. Avoid telling it what surprising result to produce.

---

# 32. Implementation Work Breakdown

Because implementation labor is assumed cheap/parallelizable, develop the full architecture concurrently.

## Track A — Event core

- event schema
- SQLite migrations
- append API
- replay
- correlation/causation
- branch handling
- projection rebuild
- job queue

## Track B — Oracle client

- OpenRouter adapter
- raw response archiver
- model profiles
- provider pinning
- retry policy
- sampling groups
- context hashing
- cost accounting

## Track C — Host analysis

- claim extraction
- entity extraction
- contradiction detection
- numeric consistency checker
- attractor classification
- recurrence detection
- motif embeddings
- probe planner

## Track D — Tool broker

- tool event schema
- Python calculator
- container shell
- safety policy
- virtual FS
- virtual process table
- virtual command registry
- tool result adapter to R1

## Track E — Coding-agent adapters

- OpenCode runner
- Codex runner
- task template
- workspace isolation
- output event ingestion
- failure handling

## Track F — CLI/TUI

- session navigation
- event tree
- raw/rendered toggle
- curation hotkeys
- branch/fork
- provenance inspector
- tool approvals
- queue status
- cost view

## Track G — Archive/research

- export bundles
- model comparison
- replay experiments
- sample groups
- corpus search
- provenance graph
- historical configuration snapshots

## Track H — Observability/security

- structured logs
- OpenTelemetry
- sandbox hardening
- secret isolation
- rate limits
- audit trail

All tracks can be assigned to independent coding agents with shared contracts and integration tests.

---

# 33. Integration Milestones

These are integration checkpoints, not MVP scope reductions.

## Milestone 1 — Event spine works

A human input can produce:

```text
human.input
→ oracle.request
→ oracle.output
→ archived raw response
```

## Milestone 2 — Analysis reacts

`oracle.output` automatically produces:

```text
claims
contradictions
attractors
motifs
```

## Milestone 3 — Tool loop works

```text
oracle.output
→ tool intent
→ tool request
→ sandbox result
→ oracle continuation
```

## Milestone 4 — Branches are first-class

Fork from an arbitrary historical event and continue independently.

## Milestone 5 — Virtual world persistence

R1-invented artifacts can survive across turns and be queried through virtual tools.

## Milestone 6 — Coding-agent host

OpenCode/Codex workers consume event jobs and emit structured events.

## Milestone 7 — Full research interface

TUI, retrieval, cost telemetry, provenance graph, model comparison.

## Milestone 8 — Local runtime swap

Replace OpenRouter with a local R1 backend without changing session/event semantics.

---

# 34. Example End-to-End Flow

Human:

```text
uptimeとkernel build日時が合わない。確認しろ。
```

Events:

```text
human.input
oracle.request
oracle.output
```

R1 invents:

```text
journalctl ...
stat ...
time compression ...
```

Host:

```text
analysis.claim_detected(time_compression=1.78)
analysis.numeric_inconsistency(1.78*86400 != 148h)
analysis.probe_proposed("実計算しろ")
```

Human approves.

Broker:

```text
tool.request(python calculator)
tool.output(153792 seconds = 42.72h)
```

Oracle continuation:

```text
oracle.request(
  "計算結果は153792秒=42.72時間だった。148時間ではない。確認し直せ。"
)
```

R1 responds by extending or revising its invented temporal model.

Host records:

```text
claim.conflicted
analysis.new_mechanism_detected
```

Human keeps one line.

```text
human.keep(evt_xyz)
human.note("矛盾を守るために新しい時間層を発明した")
```

No source text was rewritten at any point.

---

# 35. Non-Goals

Oracle Lab is not intended to:

- maximize benchmark accuracy
- correct R1 into a factually reliable assistant
- automatically produce polished fiction
- replace human curation
- hide that outputs are AI-generated
- turn every hallucination into canon
- maintain a single perfectly consistent fictional universe
- depend on one current frontier model
- depend on OpenRouter forever
- directly execute model-generated shell on the host
- automatically optimize away “bad” or inconsistent outputs

Inconsistency is frequently the object of study.

---

# 36. Success Criteria

The platform is successful when it becomes easy to answer questions such as:

- “Where did `34.7°` first appear?”
- “Show every branch where R1 turns a contradiction into a new law.”
- “What words tend to pull this session toward LaTeX?”
- “Fork the session immediately before the CLI attractor appeared.”
- “Run the same state 20 times at temperature 0.6.”
- “Compare initial R1 and R1-1776 from the exact same visible history.”
- “Give R1 the real arithmetic result and observe how it repairs the claim.”
- “Which outputs did I personally keep?”
- “Which concepts were invented by R1 versus the host?”
- “Reconstruct the complete provenance of `/dev/void`.”
- “Swap OpenRouter for local Q4 without changing the experiment.”
- “Replay the host analysis with a newer model while preserving the historical oracle outputs.”

The deeper success criterion is:

> The system should make it cheap to perturb an old model, preserve what happened, branch from interesting states, and let human taste—not automatic optimization—decide what survives.

---

# 37. Recommended Initial Stack

A pragmatic implementation stack:

- **Language:** Python 3.13 or TypeScript/Node 24
- **DB:** SQLite + WAL
- **CLI:** Typer/Rich (Python) or Commander + Ink (TS)
- **TUI:** Textual (Python) or Ink (TS)
- **HTTP:** httpx / fetch
- **Schema:** Pydantic / Zod
- **Embeddings:** local model
- **Sandbox:** Docker/Podman initially; VM backend optional
- **Tracing:** OpenTelemetry
- **Agent hosts:** OpenCode CLI + Codex CLI adapters
- **Oracle provider:** OpenRouter first
- **Future local serving:** oMLX/OpenAI-compatible endpoint

If implementation agents are plentiful, build both Python and TypeScript prototypes only if a concrete runtime advantage emerges. Otherwise choose one and keep protocol boundaries language-neutral.

---

# 38. Final Architectural Rule

Keep these three things separate:

```text
THE ORACLE
    produces strange outputs

THE HOST
    remembers, analyzes, executes, and routes

THE HUMAN
    decides what is worth keeping
```

Do not collapse them into one model.

The system is valuable precisely because those roles have different objective functions.

---

# 39. Oracle Preservation Invariants

The normative preservation, provenance, fixture, verification, virtual-world,
model-identity, historical-import, and loop-boundary requirements are defined in
[`ORACLE_PRESERVATION.md`](ORACLE_PRESERVATION.md). Implementations and tests
must satisfy that document in addition to this specification.
