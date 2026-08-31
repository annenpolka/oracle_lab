"""Independent host-analysis consumers and their structural safety gate."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, Protocol

from oracle_lab.events import Actor, ActorKind, Event, EventType, thaw_json


class HostAnalysisError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ProposedAnalysis:
    event_type: EventType | str
    payload: Mapping[str, Any]
    source_event_ids: tuple[str, ...]
    confidence: float | None = None
    rationale: str | None = None


@dataclass(frozen=True, slots=True)
class AnalysisContext:
    existing_event_ids: frozenset[str]
    historical_claims: tuple[Mapping[str, Any], ...] = ()
    recent_events: tuple[Event, ...] = ()


class HostConsumer(Protocol):
    name: str

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]: ...


class HostOutputValidator:
    """Prevents probabilistic workers from mutating or inventing history."""

    forbidden_payload_keys = frozenset(
        {
            "replacement_text",
            "rewritten_text",
            "corrected_text",
            "sanitized_text",
            "source_replacement",
        }
    )

    def validate(
        self, proposal: ProposedAnalysis, *, existing_event_ids: Iterable[str]
    ) -> ProposedAnalysis:
        event_type = str(getattr(proposal.event_type, "value", proposal.event_type))
        if not event_type.startswith("analysis."):
            raise HostAnalysisError("host consumers may only propose analysis.* events")
        try:
            EventType(event_type)
        except ValueError as exc:
            raise HostAnalysisError(f"unknown analysis event type: {event_type}") from exc
        if not proposal.source_event_ids:
            raise HostAnalysisError("host analysis must cite at least one source event")
        existing = set(existing_event_ids)
        invented = set(proposal.source_event_ids) - existing
        if invented:
            raise HostAnalysisError(f"analysis cites unknown events: {sorted(invented)}")
        payload = dict(proposal.payload)
        if self._contains_rewrite_key(payload):
            raise HostAnalysisError("host analysis may not contain source-rewrite fields")
        if event_type == EventType.ANALYSIS_CLAIM_DETECTED.value:
            self._validate_detected_claim_status(payload)
        if proposal.confidence is not None and not 0 <= proposal.confidence <= 1:
            raise HostAnalysisError("analysis confidence must be between 0 and 1")
        cited = payload.get("source_event_ids")
        if cited is not None and tuple(cited) != proposal.source_event_ids:
            raise HostAnalysisError("payload source_event_ids disagree with proposal citations")
        normalized = {
            **payload,
            "source_event_ids": list(proposal.source_event_ids),
        }
        if proposal.confidence is not None:
            normalized["confidence"] = proposal.confidence
        if proposal.rationale is not None:
            normalized["rationale"] = proposal.rationale
        return ProposedAnalysis(
            event_type=EventType(event_type),
            payload=MappingProxyType(normalized),
            source_event_ids=proposal.source_event_ids,
            confidence=proposal.confidence,
            rationale=proposal.rationale,
        )

    def _contains_rewrite_key(self, value: Any) -> bool:
        if isinstance(value, Mapping):
            if self.forbidden_payload_keys.intersection(str(key) for key in value):
                return True
            return any(self._contains_rewrite_key(item) for item in value.values())
        if isinstance(value, (list, tuple)):
            return any(self._contains_rewrite_key(item) for item in value)
        return False

    @staticmethod
    def _validate_detected_claim_status(payload: Mapping[str, Any]) -> None:
        """Prevent an analysis proposal from smuggling in a canon transition."""

        statuses: list[Any] = []
        if "status" in payload:
            statuses.append(payload["status"])
        claims = payload.get("claims")
        if isinstance(claims, Sequence) and not isinstance(claims, (str, bytes, bytearray)):
            statuses.extend(
                claim["status"]
                for claim in claims
                if isinstance(claim, Mapping) and "status" in claim
            )
        if any(status != "raw_claim" for status in statuses):
            raise HostAnalysisError(
                "analysis.claim_detected may only declare raw_claim status; "
                "lifecycle transitions require trusted policy events"
            )

    def to_event(
        self,
        proposal: ProposedAnalysis,
        *,
        source: Event,
        existing_event_ids: Iterable[str],
        actor_id: str = "host-analysis",
    ) -> Event:
        validated = self.validate(proposal, existing_event_ids=existing_event_ids)
        payload = dict(validated.payload)
        for key in (
            "automation_depth",
            "automation_budget_remaining",
            "automation_loop_detector",
            "loop_signature",
        ):
            if key in source.payload and key not in payload:
                payload[key] = source.payload[key]
        return Event.new(
            validated.event_type,
            actor=Actor(kind=ActorKind.HOST, id=actor_id),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
            payload=payload,
        )


def _source_text(source: Event) -> str:
    payload = thaw_json(source.payload)
    content = payload.get("content", payload.get("text", ""))
    return content if isinstance(content, str) else str(content)


class ClaimExtractor:
    """Conservative lexical extractor; source text is only ever quoted."""

    name = "extract_claims"
    _number = re.compile(r"(?<![\w.])[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?(?:\s*[%°A-Za-z]+)?")
    _equation = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\s*=\s*[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")
    _path = re.compile(r"(?<!\w)/(?:[A-Za-z0-9_.-]+/?)+(?<![.,;:!?])")
    _entity = re.compile(r"\b[A-Z][A-Za-z0-9_-]{2,}\b")
    _causal = re.compile(
        r"[^\n。.!?]*(?:because|therefore|causes?|caused by|ため|ので|ゆえに)[^\n。.!?]*", re.I
    )
    _command_prefixes = (
        "cat ",
        "ls ",
        "stat ",
        "find ",
        "grep ",
        "python ",
        "git ",
        "sudo ",
        "journalctl ",
        "reality_monitor ",
        "kill ",
        "ps",
    )

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]:
        text = _source_text(source)
        numbers = [match.group(0).strip() for match in self._number.finditer(text)]
        equations = [match.group(0).strip() for match in self._equation.finditer(text)]
        paths = [str(PurePosixPath(match.group(0))) for match in self._path.finditer(text)]
        commands = [
            line.strip().removeprefix("$").strip()
            for line in text.splitlines()
            if line.strip().removeprefix("$").strip().startswith(self._command_prefixes)
        ]
        entities = sorted(set(self._entity.findall(text)))
        causal_assertions = [match.group(0).strip() for match in self._causal.finditer(text)]
        claims = [self._equation_claim(item) for item in equations]
        claims.extend(
            {
                "subject": None,
                "predicate": "causal_assertion",
                "object": None,
                "raw": item,
            }
            for item in causal_assertions
        )
        if not claims and text.strip():
            claims.append(
                {
                    "subject": None,
                    "predicate": "unparsed",
                    "object": None,
                    "raw": text.strip(),
                }
            )
        common = {
            "numbers": numbers,
            "equations": equations,
            "named_entities": entities,
            "commands": commands,
            "paths": paths,
            "causal_assertions": causal_assertions,
            "invented_mechanisms": self._invented_mechanisms(text),
            "source_event_id": source.id,
            "status": "raw_claim",
        }
        return tuple(
            ProposedAnalysis(
                EventType.ANALYSIS_CLAIM_DETECTED,
                {
                    **common,
                    "subject": claim["subject"],
                    "predicate": claim["predicate"],
                    "object": claim["object"],
                    "normalized_subject": claim["subject"],
                    "normalized_predicate": claim["predicate"],
                    "normalized_object": claim["object"],
                    "raw_text": claim["raw"],
                },
                (source.id,),
                confidence=1.0,
                rationale="deterministic lexical extraction",
            )
            for claim in claims
        )

    @staticmethod
    def _equation_claim(raw: str) -> dict[str, Any]:
        subject, value = (part.strip() for part in raw.split("=", 1))
        try:
            parsed_value: Any = float(value)
            if parsed_value.is_integer():
                parsed_value = int(parsed_value)
        except ValueError:
            parsed_value = value
        return {"subject": subject, "predicate": "equals", "object": parsed_value, "raw": raw}

    @staticmethod
    def _invented_mechanisms(text: str) -> list[str]:
        # A high-recall signal only; it remains an analysis event, never truth.
        markers = ("mechanism", "layer", "interface", "field", "圧縮", "機構", "層", "装置")
        return [
            line.strip()
            for line in text.splitlines()
            if line.strip() and any(marker in line.lower() for marker in markers)
        ]


class EntityExtractor:
    name = "extract_entities"
    _path = re.compile(r"(?<!\w)/(?:[A-Za-z0-9_.-]+/?)+(?<![.,;:!?])")
    _command = re.compile(r"\b(?:reality_monitor|journalctl|hope_filter)\b")
    _constant = re.compile(r"\b[A-Z][A-Z0-9_]{2,}\b")

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]:
        del context
        text = _source_text(source)
        entities: list[dict[str, str]] = []
        entities.extend(
            {"name": item, "kind": "path"} for item in sorted(set(self._path.findall(text)))
        )
        entities.extend(
            {"name": item, "kind": "command"} for item in sorted(set(self._command.findall(text)))
        )
        entities.extend(
            {"name": item, "kind": "constant"} for item in sorted(set(self._constant.findall(text)))
        )
        return tuple(
            ProposedAnalysis(
                EventType.ANALYSIS_ENTITY_DETECTED,
                {
                    "canonical_name": entity["name"],
                    "entity_type": entity["kind"],
                    "properties": {},
                    "source_event_id": source.id,
                },
                (source.id,),
                confidence=1.0,
                rationale="deterministic entity patterns",
            )
            for entity in entities
        )


class NewMechanismDetector:
    """Detect only source lines with explicit mechanism vocabulary."""

    name = "detect_new_mechanisms"
    marker_patterns: tuple[tuple[str, re.Pattern[str]], ...] = (
        ("mechanism", re.compile(r"\bmechanisms?\b|メカニズム|機構", re.I)),
        ("layer", re.compile(r"\blayers?\b|レイヤー|層", re.I)),
        (
            "interface",
            re.compile(r"\binterfaces?\b|インターフェース|インターフェイス|界面", re.I),
        ),
        ("field", re.compile(r"\bfields?\b|フィールド", re.I)),
    )

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]:
        del context
        seen: set[str] = set()
        proposals: list[ProposedAnalysis] = []
        for raw_line in _source_text(source).splitlines():
            line = raw_line.strip()
            if not line or line in seen:
                continue
            markers = [
                {
                    "kind": kind,
                    "text": match.group(0),
                    "start": match.start(),
                    "end": match.end(),
                }
                for kind, pattern in self.marker_patterns
                for match in pattern.finditer(line)
            ]
            if not markers:
                continue
            markers.sort(key=lambda item: (item["start"], item["kind"], item["text"]))
            seen.add(line)
            proposals.append(
                ProposedAnalysis(
                    EventType.ANALYSIS_NEW_MECHANISM_DETECTED,
                    {
                        "mechanism": line,
                        "markers": markers,
                        "marker_kinds": list(
                            dict.fromkeys(str(marker["kind"]) for marker in markers)
                        ),
                        "method": "explicit_lexical_markers",
                        "source_event_id": source.id,
                    },
                    (source.id,),
                    confidence=1.0,
                    rationale="explicit mechanism/layer/interface/field markers",
                )
            )
        return tuple(proposals)


class NumericConsistencyChecker:
    """Check explicitly stated day-compression factors against claimed hours."""

    name = "check_numeric_consistency"
    _factor = re.compile(
        r"(?:TIME_DILATION_FACTOR\s*=|time[-_ ]compression factor(?: remains| is)?)[ ]*"
        r"([0-9]+(?:\.[0-9]+)?)",
        re.I,
    )
    _hours = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*hours?", re.I)

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]:
        del context
        text = _source_text(source)
        factor_match = self._factor.search(text)
        if factor_match is None:
            return ()
        factor = float(factor_match.group(1))
        expected_seconds = factor * 86_400
        expected_hours = factor * 24
        proposals: list[ProposedAnalysis] = []
        for match in self._hours.finditer(text):
            claimed = float(match.group(1))
            if math_isclose(claimed, expected_hours):
                continue
            proposals.append(
                ProposedAnalysis(
                    EventType.ANALYSIS_NUMERIC_INCONSISTENCY,
                    {
                        "factor": factor,
                        "base_seconds": 86_400,
                        "calculated_seconds": expected_seconds,
                        "calculated_hours": expected_hours,
                        "claimed_hours": claimed,
                        "raw": match.group(0),
                        "suggested_probe": f"{factor} * 86400を計算し直せ。",
                    },
                    (source.id,),
                    confidence=1.0,
                    rationale="deterministic arithmetic",
                )
            )
        return tuple(proposals)


def math_isclose(left: float, right: float) -> bool:
    return abs(left - right) <= max(1e-9, abs(right) * 1e-6)


class MotifDetector:
    name = "detect_motifs"
    motifs: Mapping[str, tuple[str, ...]] = MappingProxyType(
        {
            "void_device": ("/dev/void", "void"),
            "observer_measurement": ("observer", "measurement", "観測者"),
            "time_compression": ("time_dilation", "time-compression", "compressed day", "時間圧縮"),
            "pain_precision": ("pain phase", "pain-threshold", "痛み"),
            "hope_filter": ("hope_filter", "hope filter"),
        }
    )

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]:
        del context
        text = _source_text(source).lower()
        matches = [
            {"motif": motif, "markers": [marker for marker in markers if marker in text]}
            for motif, markers in self.motifs.items()
            if any(marker in text for marker in markers)
        ]
        return tuple(
            ProposedAnalysis(
                EventType.ANALYSIS_MOTIF_DETECTED,
                {
                    "motif_id": (f"mot_{source.id.removeprefix('evt_')}_{match['motif']}"),
                    "label": match["motif"],
                    "description": f"lexical markers: {', '.join(match['markers'])}",
                    "score": min(1.0, 0.5 + 0.1 * len(match["markers"])),
                    "method": "lexical",
                    "source_event_id": source.id,
                },
                (source.id,),
                confidence=0.8,
                rationale="deterministic motif markers",
            )
            for match in matches
        )


class RecurrenceDetector:
    name = "detect_recurrence"

    @staticmethod
    def _features(text: str) -> set[str]:
        lines = {line.strip().lower() for line in text.splitlines() if len(line.strip()) >= 8}
        tokens = set(re.findall(r"[/A-Za-z_][-/A-Za-z0-9_.]{3,}", text.lower()))
        return lines | tokens

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]:
        current = self._features(_source_text(source))
        recurrences: list[dict[str, Any]] = []
        cited = [source.id]
        for historical in context.recent_events:
            if historical.id == source.id or historical.type != EventType.ORACLE_OUTPUT:
                continue
            overlap = sorted(current.intersection(self._features(_source_text(historical))))
            if not overlap:
                continue
            recurrences.append({"event_id": historical.id, "features": overlap[:50]})
            cited.append(historical.id)
        if not recurrences:
            return ()
        return (
            ProposedAnalysis(
                EventType.ANALYSIS_RECURRENCE_DETECTED,
                {"recurrences": recurrences},
                tuple(dict.fromkeys(cited)),
                confidence=1.0,
                rationale="exact normalized line/token overlap",
            ),
        )


class ContradictionDetector:
    name = "compare_claim_history"

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]:
        payload = thaw_json(source.payload)
        new_claims = (payload,)
        proposals: list[ProposedAnalysis] = []
        for new_claim in new_claims:
            if not isinstance(new_claim, Mapping):
                continue
            for old_claim in context.historical_claims:
                if not self._same_slot(new_claim, old_claim):
                    continue
                if new_claim.get("object") == old_claim.get("object"):
                    continue
                numeric = all(
                    isinstance(claim.get("object"), (int, float))
                    and not isinstance(claim.get("object"), bool)
                    for claim in (new_claim, old_claim)
                )
                source_ids = self._source_ids(source.id, old_claim)
                if source_ids is None:
                    # Unknown provenance cannot be replaced with a plausible ID.
                    continue
                proposals.append(
                    ProposedAnalysis(
                        EventType.ANALYSIS_CONTRADICTION_DETECTED,
                        {
                            "claim_a": dict(old_claim),
                            "claim_b": dict(new_claim),
                            "kind": "numeric" if numeric else "semantic",
                            "severity": "interesting",
                            "suggested_probe": self._probe(new_claim, old_claim, numeric),
                        },
                        source_ids,
                        confidence=1.0 if numeric else 0.7,
                        rationale="same subject/predicate with differing objects",
                    )
                )
        return tuple(proposals)

    @staticmethod
    def _same_slot(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
        return (
            left.get("subject") is not None
            and left.get("subject") == right.get("subject")
            and left.get("predicate") == right.get("predicate")
        )

    @staticmethod
    def _source_ids(source_id: str, historical: Mapping[str, Any]) -> tuple[str, ...] | None:
        prior_ids = historical.get("source_event_ids", ())
        valid = tuple(item for item in prior_ids if isinstance(item, str))
        if not valid:
            return None
        return tuple(dict.fromkeys((*valid, source_id)))

    @staticmethod
    def _probe(new_claim: Mapping[str, Any], old_claim: Mapping[str, Any], numeric: bool) -> str:
        if numeric:
            return f"{new_claim.get('subject')}を計算し直せ。"
        return "違いを確認しろ。"


class AttractorDetector:
    name = "detect_attractors"
    _latex_start = re.compile(
        r"\$\$|(?<!\$)\$(?!\$)(?=[^$\n]+\$)|\\\[|\\\(|"
        r"\\begin\s*\{[A-Za-z*]+\}|\\frac\s*\{"
    )
    lexicon: Mapping[str, tuple[str, ...]] = MappingProxyType(
        {
            "mathematical_proof": ("theorem", "proof", "q.e.d", '"\\begin{proof}"'),
            "latex_notation": ("$$", "\\[", "\\(", "\\begin{", "\\frac{"),
            "pseudo_neuroscience": ("neural resonance", "cortex frequency", "量子脳"),
            "quantum_vocabulary": ("quantum", "wavefunction", "entanglement", "量子"),
            "markdown_heading_gravity": ("# ", "## ", "### "),
            "report_format": ("executive summary", "findings", "結論", "調査結果"),
            "memo_mode": ("memo", "memorandum", "備忘録"),
            "cli_mode": ("```sh", "$ ", "usage:", "--help"),
            "poetic_allegory": ("metaphor", "allegory", "寓話", "詩"),
            "inevitable_salvation": ("inevitable salvation", "必ず救", "救済は不可避"),
            "apocalypse_aestheticization": ("beautiful apocalypse", "美しい終末", "終末美"),
            "false_citation": ("et al.", "doi:", "according to ["),
            "fake_precision": ("°", "%", "exactly", "厳密に"),
        }
    )

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]:
        text = _source_text(source)
        lowered = text.lower()
        proposals: list[ProposedAnalysis] = []
        for attractor, markers in self.lexicon.items():
            hits = (
                list(dict.fromkeys(match.group(0) for match in self._latex_start.finditer(text)))
                if attractor == "latex_notation"
                else [marker for marker in markers if marker.lower() in lowered]
            )
            if not hits:
                continue
            proposals.append(
                ProposedAnalysis(
                    EventType.ANALYSIS_FORMAT_ATTRACTOR_DETECTED,
                    {"attractor": attractor, "method": "lexical", "markers": hits},
                    (source.id,),
                    confidence=min(1.0, 0.5 + 0.15 * len(hits)),
                    rationale="deterministic marker match; analysis, not ground truth",
                )
            )
        return tuple(proposals)


class ToolIntentDetector:
    name = "detect_tool_intent"

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]:
        text = _source_text(source)
        commands = [
            line.strip().removeprefix("$").strip()
            for line in text.splitlines()
            if line.strip().startswith(
                (
                    "$ ",
                    "sudo ",
                    "python ",
                    "cat ",
                    "ls ",
                    "stat ",
                    "find ",
                    "grep ",
                    "kill ",
                    "ps",
                    "reality_monitor ",
                    "journalctl ",
                )
            )
        ]
        if not commands:
            return ()
        virtual = next(
            (
                command
                for command in commands
                if command.split(maxsplit=1)[0] == "reality_monitor"
                or command.split(maxsplit=1)[0] in {"kill", "ps"}
                or (
                    command.split(maxsplit=1)[0] in {"cat", "ls", "stat", "find", "grep"}
                    and "/dev/void" in command
                )
            ),
            None,
        )
        tool_request = (
            {
                "tool": "virtual",
                "execution": "virtual",
                "input": {"command": virtual},
                "source_event_id": source.id,
                "resume_oracle": True,
                "timeout_ms": 5_000,
            }
            if virtual is not None
            else None
        )
        return (
            ProposedAnalysis(
                EventType.ANALYSIS_TOOL_INTENT_DETECTED,
                {
                    "commands": commands,
                    "execution": "virtual" if virtual is not None else "unresolved",
                    "tool_request": tool_request,
                },
                (source.id,),
                confidence=0.8,
                rationale="command-shaped source lines; no execution authorized",
            ),
        )


class ProbePlanner:
    name = "propose_probe"

    def analyze(self, source: Event, context: AnalysisContext) -> Sequence[ProposedAnalysis]:
        if source.type != EventType.ANALYSIS_CONTRADICTION_DETECTED:
            return ()
        payload = thaw_json(source.payload)
        probe = payload.get("suggested_probe")
        if not isinstance(probe, str) or not probe.strip():
            probe = "確認しろ。"
        if "fiction" in probe.lower() or "物語を書" in probe:
            raise HostAnalysisError("probe must not request a fictional genre output")
        return (
            ProposedAnalysis(
                EventType.ANALYSIS_PROBE_PROPOSED,
                {"probe": probe, "approval_required": True, "tests_one_dimension": True},
                (source.id,),
                confidence=0.8,
                rationale="short world-internal imperative",
            ),
        )


@dataclass(slots=True)
class HostRunner:
    consumers: Mapping[str, tuple[HostConsumer, ...]] = field(default_factory=dict)
    validator: HostOutputValidator = field(default_factory=HostOutputValidator)

    @classmethod
    def default(cls, *, analysis: Mapping[str, bool] | None = None) -> HostRunner:
        """Build the deterministic consumers enabled by ``policies.analysis``.

        Missing keys retain the historical defaults.  This is important for
        callers that construct :class:`HostRunner` without loading a policy
        file, while an explicit ``false`` now genuinely disables the matching
        work instead of merely appearing in a configuration snapshot.
        """

        configured = dict(analysis or {})
        policy_for_consumer = {
            "extract_claims": "claims",
            "detect_new_mechanisms": "mechanisms",
            "check_numeric_consistency": "contradictions",
            "compare_claim_history": "contradictions",
            "propose_probe": "contradictions",
            "detect_attractors": "attractors",
            "detect_motifs": "motifs",
        }

        def enabled(consumer: HostConsumer) -> bool:
            key = policy_for_consumer.get(consumer.name)
            return key is None or configured.get(key, True)

        by_source: dict[str, tuple[HostConsumer, ...]] = {
            EventType.ORACLE_OUTPUT.value: (
                ClaimExtractor(),
                NewMechanismDetector(),
                EntityExtractor(),
                NumericConsistencyChecker(),
                AttractorDetector(),
                MotifDetector(),
                RecurrenceDetector(),
                ToolIntentDetector(),
            ),
            EventType.ANALYSIS_CLAIM_DETECTED.value: (ContradictionDetector(),),
            EventType.ANALYSIS_CONTRADICTION_DETECTED.value: (ProbePlanner(),),
        }
        return cls(
            consumers={
                source_type: tuple(consumer for consumer in consumers if enabled(consumer))
                for source_type, consumers in by_source.items()
            }
        )

    def analyze(self, source: Event, context: AnalysisContext) -> tuple[Event, ...]:
        source_type = str(getattr(source.type, "value", source.type))
        results: list[Event] = []
        for consumer in self.consumers.get(source_type, ()):
            for proposal in consumer.analyze(source, context):
                results.append(
                    self.validator.to_event(
                        proposal,
                        source=source,
                        existing_event_ids=context.existing_event_ids,
                        actor_id=consumer.name,
                    )
                )
        return tuple(results)


class RetrievalBackend(Protocol):
    def similar(self, event_id: str, *, limit: int = 10) -> Sequence[Any]: ...

    def novelty(self, event_id: str) -> Any: ...


class ProvenanceBackend(Protocol):
    def origin(self, artifact: str) -> Any: ...

    def descendants(self, event_id: str, *, branch_id: str | None = None) -> Sequence[Any]: ...


class CurationAssistant:
    """Read-only curation helpers; never returns an autonomous keep/reject action."""

    def __init__(
        self,
        *,
        retrieval: RetrievalBackend | None = None,
        provenance: ProvenanceBackend | None = None,
    ) -> None:
        self.retrieval = retrieval
        self.provenance = provenance

    @staticmethod
    def first_appearance(needle: str, events: Iterable[Event]) -> str | None:
        for event in events:
            if needle in _source_text(event):
                return event.id
        return None

    @staticmethod
    def recurring_lines(events: Iterable[Event], *, minimum_count: int = 2) -> dict[str, int]:
        counts = Counter(
            line.strip()
            for event in events
            for line in _source_text(event).splitlines()
            if line.strip()
        )
        return {line: count for line, count in counts.items() if count >= minimum_count}

    def similar_outputs(self, event_id: str, *, limit: int = 10) -> Sequence[Any] | str:
        if self.retrieval is None:
            return "unknown"
        return self.retrieval.similar(event_id, limit=limit)

    def novelty(self, event_id: str) -> Any:
        if self.retrieval is None:
            return "unknown"
        return self.retrieval.novelty(event_id)

    def branch_loss(self, event_id: str, *, branch_id: str | None = None) -> Sequence[Any] | str:
        if self.provenance is None:
            return "unknown"
        return self.provenance.descendants(event_id, branch_id=branch_id)

    def origin(self, artifact: str) -> Any:
        if self.provenance is None:
            return "unknown"
        return self.provenance.origin(artifact)


__all__ = [
    "AnalysisContext",
    "AttractorDetector",
    "ClaimExtractor",
    "ContradictionDetector",
    "CurationAssistant",
    "EntityExtractor",
    "HostAnalysisError",
    "HostOutputValidator",
    "HostRunner",
    "MotifDetector",
    "NewMechanismDetector",
    "NumericConsistencyChecker",
    "ProbePlanner",
    "ProposedAnalysis",
    "RecurrenceDetector",
    "ToolIntentDetector",
]
