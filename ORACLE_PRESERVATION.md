# Oracle Preservation Invariants

These requirements are normative. They apply to the Host, coding agents,
fixtures, tests, imports, tools, virtual runtime, retrieval, curation, and
exports.

1. **Never imitate the oracle.** If oracle output is required, call the
   configured `OracleProvider` or use an explicitly marked historical fixture.
   Synthetic oracle-like material must be labeled `synthetic_fixture` and must
   not enter the archival corpus, claim history, motif statistics, or curation
   views as genuine oracle material. Oracle-, Host-, human-, virtual-,
   historical-fixture-, and synthetic-fixture-origin must remain queryable.
2. **Do not teach R1 how to behave.** Default oracle prompts contain no persona,
   fiction/style instruction, tool schema, world bible, or Host analysis.
   Short world-internal imperatives are preferred. Genre and output-format
   nouns are recorded experimental interventions.
3. **Prompt wording is experimental data.** Preserve every input exactly,
   including whitespace and unusual formatting. Derived lexical-attractor
   metadata must never replace the source. Support later prompt phrase to
   output-attractor analysis.
4. **Preserve formatting as behavior.** Canonical oracle text is never linted,
   reflowed, repaired, prettified, or normalized. Parsed and rendered forms are
   derived caches only.
5. **Contradictions are fuel, not defects.** Emit cited contradiction events;
   calculate, inspect, probe, fork, or leave unresolved. Never silently repair
   claims.
6. **Do not over-create the virtual world.** Record mentioned entities, but
   concretize virtual state only when an explicit operation requires it. Create
   the minimum state needed, retain unknown fields, and record every synthesized
   field as Host-originated provenance. Never expand an implied world for lore.
7. **Minimize invisible Host influence.** Any Host-written/transformed text
   returned to R1 is a cited event. Prefer mechanically formatted observations
   and tool output over interpretive prose.
8. **Tool truth domains are explicit.** Every result is labeled exactly one of
   `real`, `sandbox`, `virtual`, `retrieved`, or `synthetic`.
9. **Observation and verification are separate.** Factual web verification is
   opt-in and runs on a separate branch so it cannot contaminate the original
   oracle context.
10. **Human value judgments are not model rewards.** Only explicit human events
    may keep, star, canonize, or mark an exemplar. Host output may only nominate
    candidates.
11. **Do not optimize fictional coherence.** Consistency is metadata, never the
    objective. World state exists only to make earlier hallucinations queryable
    and re-enterable.
12. **Generated-world and future-product state are separate.** Do not assume a
    character identity, a future game, or a production use for any generated
    artifact.
13. **Model identity is immutable and auditable.** Preserve requested slug,
    requested profile/provider and routing, actual provider and returned model,
    fallback status, sampling, context hash, API metadata, and timestamp for
    every oracle output.
14. **Historical session import is first-class.** Imported logs are immutable
    ancestry usable as starting state, fork point, replay fixture, and retrieval
    source. Preserve unknown sampler/provider/system-prompt fields as unknown.
15. **Every automated loop has a boundary.** Carry correlation ID, depth,
    budget, and an equivalence-loop detector. Stop at human gates, depth/budget
    exhaustion, repeated equivalent events, provider/tool failure, or an
    explicit pause event.
16. **Build replayability before cleverness.** Preserve and reconstruct the
    complete prompt -> context -> raw response -> analysis -> tool ->
    continuation -> human-curation chain before adding autonomous planning.
