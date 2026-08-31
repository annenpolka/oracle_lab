"""Deterministic event-to-work policy evaluation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Protocol

from oracle_lab.events import Actor, ActorKind, Event, EventType, thaw_json
from oracle_lab.jsonutil import sha256_json, sha256_text


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


class DispatchError(ValueError):
    pass


class DecisionStatus(StrEnum):
    READY = "ready"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"


@dataclass(frozen=True, slots=True)
class DispatchAction:
    """A policy output, never a model-authored imperative."""

    kind: str  # "task" or "event"
    name: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    priority: int = 0

    def __post_init__(self) -> None:
        if self.kind not in {"task", "event"}:
            raise DispatchError("dispatch action kind must be task or event")
        if not self.name:
            raise DispatchError("dispatch action name must not be empty")
        if self.kind == "event":
            try:
                EventType(self.name)
            except ValueError as exc:
                raise DispatchError(f"unknown event action: {self.name}") from exc
        object.__setattr__(self, "payload", _deep_freeze(self.payload))


@dataclass(frozen=True, slots=True)
class DispatchRule:
    id: str
    on: str
    actions: tuple[DispatchAction, ...]
    when: Mapping[str, Any] = field(default_factory=dict)
    approval: str | None = None

    def __post_init__(self) -> None:
        if not self.id or not self.on:
            raise DispatchError("dispatch rules require id and on")
        try:
            EventType(self.on)
        except ValueError as exc:
            raise DispatchError(f"unknown event type in rule {self.id}: {self.on}") from exc
        if not self.actions:
            raise DispatchError(f"dispatch rule {self.id} must emit at least one action")
        if self.approval not in {None, "human"}:
            raise DispatchError("only human approval gates are supported")
        object.__setattr__(self, "when", _deep_freeze(self.when))


@dataclass(frozen=True, slots=True)
class DispatchDecision:
    source_event_id: str
    rule_id: str
    action: DispatchAction
    idempotency_key: str
    status: DecisionStatus
    approver_event_id: str | None = None


class JobEnqueuer(Protocol):
    def enqueue(self, **kwargs: Any) -> Any: ...


class EventSink(Protocol):
    def append(self, event: Event) -> Event: ...

    def list_events(self, **filters: Any) -> Sequence[Event]: ...


def _nested_get(value: Mapping[str, Any], path: str) -> Any:
    current: Any = value
    for component in path.split("."):
        if not isinstance(current, Mapping) or component not in current:
            return None
        current = current[component]
    return current


def _matches(event: Event, conditions: Mapping[str, Any]) -> bool:
    envelope = event.to_dict()
    payload = thaw_json(event.payload)
    metadata = thaw_json(event.metadata)
    for key, expected in conditions.items():
        if key.startswith(("payload.", "metadata.")):
            actual = _nested_get(envelope, key)
        elif key in payload:
            actual = payload[key]
        elif key in metadata:
            actual = metadata[key]
        else:
            actual = _nested_get(envelope, key)
        if actual != expected:
            return False
    return True


class EventDispatcher:
    """Pure deterministic evaluation plus optional idempotent side effects."""

    def __init__(
        self,
        rules: Iterable[DispatchRule],
        *,
        queue: JobEnqueuer | None = None,
        event_sink: EventSink | None = None,
        model_profile_resolver: Callable[[Event], str | None] | None = None,
        max_auto_depth: int = 4,
        max_auto_budget: int = 16,
    ) -> None:
        materialized = tuple(rules)
        ids = [rule.id for rule in materialized]
        if len(ids) != len(set(ids)):
            raise DispatchError("dispatch rule ids must be unique")
        self.rules = materialized
        self.queue = queue
        self.event_sink = event_sink
        self.model_profile_resolver = model_profile_resolver
        if max_auto_depth < 0 or max_auto_budget < 1:
            raise DispatchError("automation boundaries must be non-negative and positive")
        self.max_auto_depth = max_auto_depth
        self.max_auto_budget = max_auto_budget

    def evaluate(self, event: Event) -> tuple[DispatchDecision, ...]:
        event_type = str(getattr(event.type, "value", event.type))
        decisions: list[DispatchDecision] = []
        for rule in self.rules:
            if rule.on != event_type or not _matches(event, rule.when):
                continue
            for index, action in enumerate(rule.actions):
                identity = {
                    "source_event_id": event.id,
                    "rule_id": rule.id,
                    "action_index": index,
                    "action_kind": action.kind,
                    "action_name": action.name,
                    "action_payload": dict(action.payload),
                }
                decisions.append(
                    DispatchDecision(
                        source_event_id=event.id,
                        rule_id=rule.id,
                        action=action,
                        idempotency_key=f"dispatch:{sha256_json(identity)}",
                        status=(
                            DecisionStatus.PENDING_APPROVAL
                            if rule.approval == "human"
                            else DecisionStatus.READY
                        ),
                    )
                )
        return tuple(decisions)

    def dispatch(self, event: Event) -> tuple[DispatchDecision, ...]:
        decisions = self.evaluate(event)
        cascaded: list[DispatchDecision] = []
        for decision in decisions:
            if decision.status == DecisionStatus.READY:
                result = self._execute(decision, event)
                if isinstance(result, Event):
                    cascaded.extend(self.dispatch(result))
            elif decision.status == DecisionStatus.PENDING_APPROVAL:
                self._persist_pending(decision, event)
        return (*decisions, *cascaded)

    def approve(
        self,
        decision: DispatchDecision,
        *,
        approver_event_id: str,
        source_event: Event,
    ) -> DispatchDecision:
        if decision.status != DecisionStatus.PENDING_APPROVAL:
            raise DispatchError("only a pending decision can be approved")
        if decision.source_event_id != source_event.id:
            raise DispatchError("approval source event does not match decision")
        self._validate_approval_event(decision, source_event, approver_event_id)
        approved = DispatchDecision(
            source_event_id=decision.source_event_id,
            rule_id=decision.rule_id,
            action=decision.action,
            idempotency_key=decision.idempotency_key,
            status=DecisionStatus.APPROVED,
            approver_event_id=approver_event_id,
        )
        self._clear_pending(approved)
        result = self._execute(approved, source_event)
        if isinstance(result, Event):
            self.dispatch(result)
        return approved

    def _validate_approval_event(
        self,
        decision: DispatchDecision,
        source: Event,
        approver_event_id: str,
    ) -> Event:
        """Require a persisted, correctly typed human action citing the gate.

        An ID supplied by a caller is not itself authority.  This validation is
        deliberately performed before cancelling the durable gate or executing
        its action, so forged/missing approvals have no side effects.
        """

        if self.event_sink is None:
            raise DispatchError("approval validation requires a persisted event sink")
        getter = getattr(self.event_sink, "get", None)
        approval = getter(approver_event_id) if callable(getter) else None
        if approval is None:
            try:
                approval = next(
                    (
                        event
                        for event in self.event_sink.list_events()
                        if event.id == approver_event_id
                    ),
                    None,
                )
            except (AttributeError, TypeError):
                approval = None
        if approval is None:
            raise DispatchError(f"approval event is not persisted: {approver_event_id}")
        if approval.actor.kind is not ActorKind.HUMAN:
            raise DispatchError("approval event must have a human actor")

        expected = {
            "human-approve-probe": (
                EventType.ANALYSIS_PROBE_PROPOSED,
                EventType.HUMAN_REQUEST_PROBE,
            ),
            "human-approve-canon": (
                EventType.ANALYSIS_CANON_CANDIDATE,
                EventType.HUMAN_KEEP,
            ),
            "branch-proposal-creation": (
                EventType.ANALYSIS_BRANCH_PROPOSED,
                EventType.HUMAN_REQUEST_FORK,
            ),
            "human-approve-shell-request": (
                EventType.TOOL_REQUEST,
                EventType.TOOL_APPROVED,
            ),
        }.get(decision.rule_id)
        if expected is None:
            raise DispatchError(f"approval contract is undefined for rule: {decision.rule_id}")
        expected_source_type, expected_approval_type = expected
        if source.type is not expected_source_type:
            raise DispatchError(
                f"approval rule {decision.rule_id} cannot authorize {source.type.value}"
            )
        if approval.type is not expected_approval_type:
            raise DispatchError(
                f"approval for {decision.rule_id} must be {expected_approval_type.value}"
            )
        if approval.session_id != source.session_id or approval.branch_id != source.branch_id:
            raise DispatchError("approval event must share the proposal session and branch")

        cited_ids = {
            value
            for key in (
                "proposal_event_id",
                "event_id",
                "target_event_id",
                "request_event_id",
                "request_id",
            )
            if isinstance((value := approval.payload.get(key)), str)
        }
        if source.id not in cited_ids:
            raise DispatchError("approval event does not cite the gated source event")
        if decision.rule_id == "human-approve-canon":
            candidate_claim_id = source.payload.get("claim_id")
            approved_claim_id = approval.payload.get("claim_id")
            if not isinstance(candidate_claim_id, str) or not candidate_claim_id:
                raise DispatchError("canon candidate has no claim_id")
            if approved_claim_id != candidate_claim_id:
                raise DispatchError("canon approval references another claim")
            if (
                approval.payload.get("candidate_event_id") != source.id
                or approval.payload.get("target_event_id") != source.id
                or approval.payload.get("event_id") != source.id
                or approval.parent_event_id != source.id
                or approval.causation_id != source.id
            ):
                raise DispatchError("canon approval does not target its gated candidate")
        return approval

    def _persist_pending(self, decision: DispatchDecision, source: Event) -> Any:
        if self.queue is None:
            return None
        return self.queue.enqueue(
            kind="await_human_approval",
            payload={
                "decision_idempotency_key": decision.idempotency_key,
                "source_event_id": source.id,
                "rule_id": decision.rule_id,
                "action": {
                    "kind": decision.action.kind,
                    "name": decision.action.name,
                    "payload": dict(decision.action.payload),
                },
            },
            source_event_id=source.id,
            idempotency_key=f"approval:{decision.idempotency_key}",
            priority=100,
            session_id=source.session_id,
            branch_id=source.branch_id,
        )

    def _clear_pending(self, decision: DispatchDecision) -> None:
        if self.queue is None:
            return
        list_jobs = getattr(self.queue, "list_jobs", None)
        cancel = getattr(self.queue, "cancel", None)
        if not callable(list_jobs) or not callable(cancel):
            return
        for job in list_jobs(kind="await_human_approval"):
            if getattr(job, "idempotency_key", None) == f"approval:{decision.idempotency_key}":
                cancel(job.id)

    def _execute(self, decision: DispatchDecision, source: Event) -> Any:
        depth, budget = self._automation_state(source)
        signature: str | None = None
        if decision.action.name == EventType.TOOL_RESULT_ADAPTED.value:
            signature = sha256_json(
                {
                    "request_id": source.payload.get("request_id"),
                    "status": source.payload.get("status"),
                    "output": source.payload.get("output"),
                    "error": source.payload.get("error"),
                    "exit_code": source.payload.get("exit_code"),
                    "truth_domain": source.payload.get("truth_domain"),
                }
            )
            if depth >= self.max_auto_depth:
                return self._emit_automation_stop(source, decision, "max_depth", signature)
            if budget <= 0:
                return self._emit_automation_stop(source, decision, "budget_exhausted", signature)
            repeated = self._equivalent_adapter(source, signature)
            if repeated is not None:
                return self._emit_automation_stop(
                    source,
                    decision,
                    "repeated_equivalent_event",
                    signature,
                    equivalent_event_id=repeated.id,
                )
        elif (
            decision.action.name == EventType.ORACLE_REQUEST.value
            and source.type is EventType.TOOL_RESULT_ADAPTED
            and budget <= 0
        ):
            return self._emit_automation_stop(
                source,
                decision,
                "budget_exhausted",
                str(source.payload.get("loop_signature", "")),
            )
        payload = {
            **dict(decision.action.payload),
            "source_event_id": source.id,
            "dispatch_rule_id": decision.rule_id,
        }
        if decision.approver_event_id is not None:
            payload["approver_event_id"] = decision.approver_event_id
        for key in (
            "automation_depth",
            "automation_budget_remaining",
            "automation_loop_detector",
            "loop_signature",
        ):
            if key in source.payload:
                payload.setdefault(key, source.payload[key])
        if decision.action.name in {
            EventType.ORACLE_REQUEST.value,
            "oracle.generate",
        }:
            model_profile_id = self._lineage_payload_value(source, "model_profile_id")
            if model_profile_id is None:
                model_profile_id = self._session_model_profile(source)
            provider_id = self._lineage_payload_value(
                source,
                "provider_id",
                "provider",
                "provider_name",
            )
            if model_profile_id is not None:
                payload.setdefault("model_profile_id", model_profile_id)
            if provider_id is not None:
                payload.setdefault("provider_id", provider_id)
        if decision.action.name == "oracle.generate":
            if not isinstance(payload.get("model_profile_id"), str):
                raise DispatchError(
                    f"cannot resolve model_profile_id for oracle job from {source.id}"
                )
            payload.update(
                {
                    "request_event_id": source.id,
                    "model_profile_id": payload.get("model_profile_id"),
                }
            )
        elif decision.action.name == "tool.execute":
            payload.update(
                {
                    "request_event_id": source.id,
                    "approved": decision.approver_event_id is not None,
                }
            )
        elif decision.action.name == EventType.CLAIM_PROMOTED.value:
            claim_id = source.payload.get("claim_id")
            if not isinstance(claim_id, str) or not claim_id:
                raise DispatchError(f"canon candidate has no claim_id: {source.id}")
            payload["claim_id"] = claim_id
        if decision.action.kind == "task":
            if self.queue is None:
                return None
            queue_options: dict[str, Any] = {
                "session_id": source.session_id,
                "branch_id": source.branch_id,
            }
            if decision.action.name == "oracle.generate":
                queue_options.update(
                    {
                        "provider_id": payload.get("provider_id"),
                        "serialize_branch": not bool(
                            source.payload.get("parallel_sampling", False)
                        ),
                    }
                )
            return self.queue.enqueue(
                kind=decision.action.name,
                payload=payload,
                source_event_id=source.id,
                idempotency_key=decision.idempotency_key,
                priority=decision.action.priority,
                **queue_options,
            )
        if self.event_sink is None:
            return None
        if self._event_already_emitted(decision.idempotency_key, source):
            return None
        metadata = {
            "schema_version": 1,
            "dispatch_idempotency_key": decision.idempotency_key,
        }
        if decision.action.name == EventType.TOOL_RESULT_ADAPTED.value:
            output = source.payload.get("output", "")
            content = str(output)
            payload["message"] = {
                "role": "user",
                "content": content,
            }
            payload.update(
                {
                    "tool_request_id": source.payload.get("request_id"),
                    "source_event_ids": [source.id],
                    "content_sha256": sha256_text(content),
                    "formatter_id": "identity-tool-output",
                    "formatter_version": 1,
                    "truth_domain": source.payload.get(
                        "truth_domain", source.metadata.get("truth_domain")
                    ),
                    "automation_depth": depth + 1,
                    "automation_budget_remaining": max(0, budget - 1),
                    "automation_loop_detector": "sha256-equivalent-event-v1",
                    "loop_signature": signature,
                }
            )
            metadata["r1_visible"] = True
        elif decision.action.name == EventType.ORACLE_CONTEXT_MESSAGE.value:
            content = source.payload.get("probe", source.payload.get("content", ""))
            payload["message"] = {"role": "user", "content": str(content)}
            metadata["r1_visible"] = True
        elif decision.action.name == EventType.CLAIM_PROMOTED.value:
            claim_id = source.payload.get("claim_id")
            if not isinstance(claim_id, str) or not claim_id:
                raise DispatchError(f"canon candidate has no claim_id: {source.id}")
            payload["claim_id"] = claim_id
            payload["candidate_event_id"] = source.id
            payload["source_event_ids"] = list(
                dict.fromkeys(
                    [
                        *(
                            item
                            for item in source.payload.get("source_event_ids", ())
                            if isinstance(item, str)
                        ),
                        source.id,
                    ]
                )
            )
        if (
            decision.action.name == EventType.ORACLE_REQUEST.value
            and source.type is EventType.TOOL_RESULT_ADAPTED
        ):
            payload["automation_budget_remaining"] = max(0, budget - 1)
        if decision.action.name == EventType.ORACLE_REQUEST.value and not isinstance(
            payload.get("model_profile_id"), str
        ):
            raise DispatchError(
                f"cannot resolve model_profile_id for oracle request from {source.id}"
            )
        emitted = Event.new(
            decision.action.name,
            actor=Actor(kind=ActorKind.SYSTEM, id="dispatcher"),
            session_id=source.session_id,
            branch_id=source.branch_id,
            parent_event_id=source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
            payload=payload,
            metadata=metadata,
        )
        return self.event_sink.append(emitted)

    def _automation_state(self, source: Event) -> tuple[int, int]:
        raw_depth = source.payload.get("automation_depth", 0)
        raw_budget = source.payload.get("automation_budget_remaining", self.max_auto_budget)
        depth = raw_depth if isinstance(raw_depth, int) and not isinstance(raw_depth, bool) else 0
        budget = (
            raw_budget
            if isinstance(raw_budget, int) and not isinstance(raw_budget, bool)
            else self.max_auto_budget
        )
        return max(0, depth), max(0, budget)

    def _equivalent_adapter(self, source: Event, signature: str) -> Event | None:
        if self.event_sink is None:
            return None
        try:
            events = self.event_sink.list_events(
                event_type=EventType.TOOL_RESULT_ADAPTED,
                correlation_id=source.correlation_id,
            )
        except (AttributeError, TypeError):
            return None
        return next(
            (event for event in events if event.payload.get("loop_signature") == signature),
            None,
        )

    def _emit_automation_stop(
        self,
        source: Event,
        decision: DispatchDecision,
        reason: str,
        signature: str,
        *,
        equivalent_event_id: str | None = None,
    ) -> Event | None:
        if self.event_sink is None:
            return None
        try:
            prior = self.event_sink.list_events(
                event_type=EventType.SYSTEM_AUTOMATION_STOPPED,
                correlation_id=source.correlation_id,
            )
        except (AttributeError, TypeError):
            prior = ()
        existing = next(
            (
                event
                for event in prior
                if event.payload.get("source_event_id") == source.id
                and event.payload.get("reason") == reason
            ),
            None,
        )
        if existing is not None:
            return existing
        depth, budget = self._automation_state(source)
        payload: dict[str, Any] = {
            "reason": reason,
            "source_event_id": source.id,
            "source_event_ids": [source.id],
            "automation_depth": depth,
            "automation_budget_remaining": budget,
            "automation_loop_detector": "sha256-equivalent-event-v1",
            "loop_signature": signature,
        }
        if equivalent_event_id is not None:
            payload["equivalent_event_id"] = equivalent_event_id
        return self.event_sink.append(
            Event.new(
                EventType.SYSTEM_AUTOMATION_STOPPED,
                actor=Actor(kind=ActorKind.SYSTEM, id="dispatcher-boundary"),
                session_id=source.session_id,
                branch_id=source.branch_id,
                parent_event_id=source.id,
                causation_id=source.id,
                correlation_id=source.correlation_id,
                payload=payload,
                metadata={
                    "schema_version": 1,
                    "dispatch_idempotency_key": (f"stop:{decision.idempotency_key}:{reason}"),
                },
            )
        )

    def _session_model_profile(self, source: Event) -> str | None:
        if self.model_profile_resolver is not None:
            profile = self.model_profile_resolver(source)
            if isinstance(profile, str) and profile.strip():
                return profile
        connection = getattr(self.event_sink, "connection", None)
        if connection is None or source.session_id is None:
            return None
        row = connection.execute(
            "SELECT model_profile_id FROM sessions WHERE id = ?", (source.session_id,)
        ).fetchone()
        profile = None if row is None else row[0]
        return profile if isinstance(profile, str) and profile.strip() else None

    def _lineage_payload_value(self, source: Event, *keys: str) -> Any:
        """Resolve routing data through the persisted narrative ancestry.

        Tool adapters and approved probes deliberately create fresh events, but
        their oracle continuation must retain the originating model/provider.
        The event chain is the durable source of truth; no in-memory decision
        state is needed after a restart.
        """

        current: Event | None = source
        seen: set[str] = set()
        getter = getattr(self.event_sink, "get", None)
        while current is not None and current.id not in seen:
            seen.add(current.id)
            for key in keys:
                value = current.payload.get(key)
                if value is not None and value != "":
                    return value
            if not callable(getter):
                break
            ancestor_id = current.parent_event_id or current.causation_id
            current = getter(ancestor_id) if ancestor_id else None
        return None

    def _event_already_emitted(self, key: str, source: Event) -> bool:
        assert self.event_sink is not None
        try:
            events = self.event_sink.list_events(
                session_id=source.session_id,
                branch_id=source.branch_id,
                ascending=True,
            )
        except (AttributeError, TypeError):
            return False
        return any(event.metadata.get("dispatch_idempotency_key") == key for event in events)


Dispatcher = EventDispatcher


def default_rules(
    *,
    analysis: Mapping[str, bool] | None = None,
    human_gate: Mapping[str, bool] | None = None,
) -> tuple[DispatchRule, ...]:
    """Return built-in rules after applying runtime policy switches.

    Omitted keys remain enabled so programmatic callers retain the original
    rule set.  Explicit ``false`` values remove only the analysis work covered
    by that key, or turn the corresponding human approval into a ready action.
    """

    analysis_policy = dict(analysis or {})
    gate_policy = dict(human_gate or {})
    analysis_actions = (
        ("extract_claims", "claims"),
        ("detect_new_mechanisms", "mechanisms"),
        ("extract_entities", None),
        ("check_numeric_consistency", "contradictions"),
        ("detect_attractors", "attractors"),
        ("detect_motifs", "motifs"),
        ("detect_recurrence", None),
        ("detect_tool_intent", None),
    )

    def analysis_enabled(key: str | None) -> bool:
        return key is None or analysis_policy.get(key, True)

    return (
        DispatchRule(
            "oracle-output-analysis",
            EventType.ORACLE_OUTPUT.value,
            tuple(
                DispatchAction("task", task)
                for task, policy_key in analysis_actions
                if analysis_enabled(policy_key)
            ),
        ),
        DispatchRule(
            "oracle-request-generation",
            EventType.ORACLE_REQUEST.value,
            (DispatchAction("task", "oracle.generate"),),
        ),
        *(
            (
                DispatchRule(
                    "claim-history-comparison",
                    EventType.ANALYSIS_CLAIM_DETECTED.value,
                    (DispatchAction("task", "compare_claim_history"),),
                ),
                DispatchRule(
                    "numeric-contradiction-calculation",
                    EventType.ANALYSIS_CONTRADICTION_DETECTED.value,
                    (DispatchAction("task", "propose_calculation"),),
                    when={"kind": "numeric"},
                ),
                DispatchRule(
                    "numeric-inconsistency-calculation",
                    EventType.ANALYSIS_NUMERIC_INCONSISTENCY.value,
                    (DispatchAction("task", "propose_calculation"),),
                ),
            )
            if analysis_enabled("contradictions")
            else ()
        ),
        DispatchRule(
            "adapt-tool-result-for-oracle",
            EventType.TOOL_OUTPUT.value,
            (DispatchAction("event", EventType.TOOL_RESULT_ADAPTED.value),),
            when={"metadata.resume_oracle": True},
        ),
        DispatchRule(
            "request-oracle-after-tool-adapter",
            EventType.TOOL_RESULT_ADAPTED.value,
            (DispatchAction("event", EventType.ORACLE_REQUEST.value),),
        ),
        DispatchRule(
            "request-oracle-after-context-message",
            EventType.ORACLE_CONTEXT_MESSAGE.value,
            (DispatchAction("event", EventType.ORACLE_REQUEST.value),),
        ),
        DispatchRule(
            "request-oracle-after-analysis-promotion",
            EventType.ANALYSIS_PROMOTED_TO_ORACLE.value,
            (DispatchAction("event", EventType.ORACLE_REQUEST.value),),
        ),
        DispatchRule(
            "human-approve-probe",
            EventType.ANALYSIS_PROBE_PROPOSED.value,
            (DispatchAction("event", EventType.ORACLE_CONTEXT_MESSAGE.value),),
            approval="human" if gate_policy.get("probe_generation", True) else None,
        ),
        DispatchRule(
            "human-approve-canon",
            EventType.ANALYSIS_CANON_CANDIDATE.value,
            (
                DispatchAction(
                    "event",
                    EventType.CLAIM_PROMOTED.value,
                    {"to_status": "canonical"},
                ),
            ),
            # Canon is a human-taste decision, never an automation toggle.
            # Retain the configuration key for snapshot compatibility, but do
            # not let ``false`` authorize a model/host promotion.
            approval="human",
        ),
        DispatchRule(
            "branch-proposal-creation",
            EventType.ANALYSIS_BRANCH_PROPOSED.value,
            (DispatchAction("task", "branch.create"),),
            approval="human" if gate_policy.get("branch_creation", False) else None,
        ),
        DispatchRule(
            "execute-calculator-request",
            EventType.TOOL_REQUEST.value,
            (DispatchAction("task", "tool.execute"),),
            when={"tool": "calculator", "execution": "real_deterministic"},
        ),
        DispatchRule(
            "execute-virtual-request",
            EventType.TOOL_REQUEST.value,
            (DispatchAction("task", "tool.execute"),),
            when={"execution": "virtual"},
        ),
        DispatchRule(
            "human-approve-shell-request",
            EventType.TOOL_REQUEST.value,
            (DispatchAction("task", "tool.execute"),),
            when={"tool": "shell", "execution": "real_sandbox"},
            approval="human",
        ),
    )


__all__ = [
    "DecisionStatus",
    "DispatchAction",
    "DispatchDecision",
    "DispatchError",
    "DispatchRule",
    "Dispatcher",
    "EventDispatcher",
    "default_rules",
]
