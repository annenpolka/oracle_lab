"""Textual curation interface for sessions, transcripts, and provenance."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Protocol

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Grid
from textual.widgets import Footer, Header, Label, Markdown, RichLog, Static, Tree

from oracle_lab.jsonutil import canonical_json


class TUIService(Protocol):
    def list_sessions(self) -> Sequence[Any]: ...

    def list_events(self, session_id: str | None = None) -> Sequence[Any]: ...

    def list_branches(self, session_id: str | None = None) -> Sequence[Any]: ...

    def switch_session(self, session_id: str) -> Any: ...

    def switch_branch(self, session_id: str, branch_id: str) -> Any: ...

    def keep(self, event_id: str) -> Any: ...

    def star(self, event_id: str) -> Any: ...

    def reject(self, event_id: str) -> Any: ...

    def fork(self, event_id: str, title: str | None = None) -> Any: ...

    def propose_probe(self, event_id: str) -> Any: ...

    def request_tool(self, event_id: str) -> Any: ...

    def trace_event(self, event_id: str) -> Any: ...

    def generation_metadata(self, event_id: str) -> Any: ...


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        result = to_dict()
        if isinstance(result, Mapping):
            return dict(result)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        result = model_dump(mode="json")
        if isinstance(result, Mapping):
            return dict(result)
    data = getattr(value, "__dict__", None)
    return dict(data) if isinstance(data, Mapping) else {"value": str(value)}


def _payload(event: Mapping[str, Any]) -> dict[str, Any]:
    value = event.get("payload", {})
    return dict(value) if isinstance(value, Mapping) else {}


def _event_text(event: Mapping[str, Any]) -> str:
    payload = _payload(event)
    for key in ("raw_text", "text", "content", "output", "note"):
        value = payload.get(key)
        if isinstance(value, str):
            return value
    return canonical_json(event)


def _material_origin(event: Mapping[str, Any]) -> str:
    origins = event.get("material_origins")
    if isinstance(origins, Sequence) and not isinstance(origins, (str, bytes, bytearray)):
        labels = [str(value) for value in origins if isinstance(value, str)]
        if labels:
            return ",".join(labels)
    for container_name in ("payload", "metadata"):
        container = event.get(container_name)
        if not isinstance(container, Mapping):
            continue
        origin = container.get("material_origin")
        if isinstance(origin, str):
            return origin
        if container.get("synthetic_fixture") is True:
            return "synthetic_fixture"
    return "synthetic_fixture" if event.get("synthetic_lineage") is True else "unknown"


def _is_synthetic(event: Mapping[str, Any]) -> bool:
    return event.get("synthetic_lineage") is True or "synthetic_fixture" in _material_origin(
        event
    ).split(",")


class OracleLabTUI(App[None]):
    """Four-pane, keyboard-first research and curation application."""

    CSS = """
    Screen {
        layout: vertical;
    }
    #workspace {
        grid-size: 2 2;
        grid-columns: 1fr 2fr;
        grid-rows: 1fr 1fr;
        height: 1fr;
        grid-gutter: 1;
        padding: 0 1;
    }
    .pane {
        border: round $accent;
        padding: 0 1;
        overflow: auto;
    }
    .pane-title {
        text-style: bold;
        color: $accent;
        height: 1;
    }
    #session-tree, #claims-log, #events-log, #transcript-raw, #transcript-rendered {
        height: 1fr;
    }
    """

    BINDINGS: ClassVar[list[Binding]] = [
        Binding("k", "keep", "Keep"),
        Binding("s", "star", "Star"),
        Binding("r", "reject", "Reject"),
        Binding("f", "fork", "Fork"),
        Binding("p", "propose_probe", "Probe"),
        Binding("t", "run_tool", "Tool"),
        Binding("o", "show_origin", "Origin"),
        Binding("m", "toggle_markdown", "Raw/rendered"),
        Binding("g", "generation_metadata", "Generation"),
    ]

    def __init__(
        self,
        service: TUIService | None = None,
        *,
        session_id: str | None = None,
        branch_id: str | None = None,
    ) -> None:
        super().__init__()
        if service is None:
            from oracle_lab.services import OracleLabService

            service = OracleLabService.default()
        self.service = service
        self.session_id = session_id
        self.branch_id = branch_id
        self.selected_event_id: str | None = None
        self.selected_event_text = ""
        self.selected_event_origin = "unknown"
        self.selected_event_synthetic = False
        self.show_rendered = False

    def compose(self) -> ComposeResult:
        yield Header()
        with Grid(id="workspace"):
            with Container(classes="pane", id="sessions-pane"):
                yield Label("Session tree", classes="pane-title")
                yield Tree("sessions", id="session-tree")
            with Container(classes="pane", id="transcript-pane"):
                yield Label("Oracle transcript", classes="pane-title", id="transcript-title")
                yield Static("", id="transcript-raw", markup=False)
                yield Markdown("", id="transcript-rendered")
            with Container(classes="pane", id="claims-pane"):
                yield Label("Claims / motifs", classes="pane-title")
                yield RichLog(id="claims-log", wrap=True, markup=False)
            with Container(classes="pane", id="events-pane"):
                yield Label("Events / analysis / provenance", classes="pane-title")
                yield RichLog(id="events-log", wrap=True, markup=False)
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#transcript-rendered", Markdown).display = False
        self.reload_data()

    def reload_data(self) -> None:
        sessions = [_mapping(session) for session in self.service.list_sessions()]
        tree = self.query_one("#session-tree", Tree)
        tree.clear()
        for session in sessions:
            session_id = str(session.get("id") or session.get("session_id") or "unknown")
            title = str(session.get("title") or session_id)
            session_node = tree.root.add(
                f"{title} [{session_id}]",
                data={"kind": "session", "session_id": session_id},
            )
            branches = [_mapping(branch) for branch in self.service.list_branches(session_id)]
            session_events = [_mapping(event) for event in self.service.list_events(session_id)]
            for branch in branches:
                branch_id = str(branch.get("id") or branch.get("branch_id") or "unknown")
                branch_title = str(branch.get("title") or branch_id)
                branch_node = session_node.add(
                    f"{branch_title} [{branch_id}]",
                    data={
                        "kind": "branch",
                        "session_id": session_id,
                        "branch_id": branch_id,
                    },
                )
                for event in session_events:
                    if event.get("branch_id") != branch_id:
                        continue
                    event_id = str(event.get("id") or event.get("event_id") or "unknown")
                    origin = _material_origin(event)
                    origin_suffix = "" if origin == "unknown" else f" origin={origin}"
                    branch_node.add_leaf(
                        f"{event.get('type', 'event')} [{event_id}]{origin_suffix}",
                        data={
                            "kind": "event",
                            "session_id": session_id,
                            "branch_id": branch_id,
                            "event_id": event_id,
                        },
                    )
                branch_node.expand()
            session_node.expand()
        tree.root.expand()
        if self.session_id is None and sessions:
            active = sessions[-1]
            self.session_id = str(active.get("id") or active.get("session_id"))
            current = active.get("current_branch_id")
            self.branch_id = str(current) if current is not None else self.branch_id
        self._load_events()

    def _load_events(self) -> None:
        events = [
            _mapping(event)
            for event in self.service.list_events(self.session_id)
            if self.branch_id is None or _mapping(event).get("branch_id") == self.branch_id
        ]
        event_log = self.query_one("#events-log", RichLog)
        claims_log = self.query_one("#claims-log", RichLog)
        event_log.clear()
        claims_log.clear()
        for event in events:
            event_id = str(event.get("id") or event.get("event_id") or "unknown")
            event_type = str(event.get("type", "event"))
            origin = _material_origin(event)
            synthetic = _is_synthetic(event)
            origin_note = "" if origin == "unknown" else f"  origin={origin}"
            curation_note = "  CURATION DISABLED" if synthetic else ""
            event_log.write(f"{event_id}  {event_type}{origin_note}{curation_note}")
            if not synthetic and event_type.startswith(("analysis.", "claim.", "entity.")):
                claims_log.write(f"{event_type}: {_event_text(event)}")
        if events:
            selected = next(
                (event for event in reversed(events) if event.get("type") == "oracle.output"),
                events[-1],
            )
            self._select_event(selected)
        else:
            self.selected_event_id = None
            self.selected_event_text = ""
            self.selected_event_origin = "unknown"
            self.selected_event_synthetic = False
        self._refresh_transcript()

    def on_tree_node_selected(self, message: Tree.NodeSelected[Any]) -> None:
        data = message.node.data
        if not isinstance(data, Mapping):
            return
        kind = data.get("kind")
        session_id = data.get("session_id")
        branch_id = data.get("branch_id")
        if isinstance(session_id, str):
            self.session_id = session_id
        if kind == "session" and isinstance(session_id, str):
            session = _mapping(self.service.switch_session(session_id))
            current = session.get("current_branch_id")
            self.branch_id = str(current) if current is not None else None
            self._load_events()
        elif kind == "branch" and isinstance(session_id, str) and isinstance(branch_id, str):
            self.service.switch_branch(session_id, branch_id)
            self.branch_id = branch_id
            self._load_events()
        elif kind == "event":
            event_id = data.get("event_id")
            if not isinstance(event_id, str):
                return
            events = [_mapping(event) for event in self.service.list_events(self.session_id)]
            selected = next((event for event in events if event.get("id") == event_id), None)
            if selected is not None:
                self.branch_id = str(branch_id) if branch_id is not None else self.branch_id
                self._select_event(selected)
                self._refresh_transcript()

    def _select_event(self, event: Mapping[str, Any]) -> None:
        self.selected_event_id = str(event.get("id") or event.get("event_id"))
        self.selected_event_text = _event_text(event)
        self.selected_event_origin = _material_origin(event)
        self.selected_event_synthetic = _is_synthetic(event)

    def _refresh_transcript(self) -> None:
        raw = self.query_one("#transcript-raw", Static)
        rendered = self.query_one("#transcript-rendered", Markdown)
        title = self.query_one("#transcript-title", Label)
        suffix = f" — origin: {self.selected_event_origin}"
        if self.selected_event_synthetic:
            suffix += " — KEEP/STAR DISABLED"
        title.update(f"Oracle transcript{suffix}")
        raw.update(self.selected_event_text)
        rendered.update(self.selected_event_text)
        raw.display = not self.show_rendered
        rendered.display = self.show_rendered

    def _selected_or_notify(self) -> str | None:
        if self.selected_event_id is None:
            self.notify("No event selected", severity="warning")
        return self.selected_event_id

    def _record_action(self, label: str, value: Any) -> None:
        self.query_one("#events-log", RichLog).write(f"{label}: {canonical_json(_mapping(value))}")

    def action_keep(self) -> None:
        if self.selected_event_synthetic:
            self.notify("Synthetic oracle material cannot be kept", severity="warning")
            return
        if event_id := self._selected_or_notify():
            self._record_action("keep", self.service.keep(event_id))

    def action_star(self) -> None:
        if self.selected_event_synthetic:
            self.notify("Synthetic oracle material cannot be starred", severity="warning")
            return
        if event_id := self._selected_or_notify():
            self._record_action("star", self.service.star(event_id))

    def action_reject(self) -> None:
        if event_id := self._selected_or_notify():
            self._record_action("reject", self.service.reject(event_id))

    def action_fork(self) -> None:
        if event_id := self._selected_or_notify():
            self._record_action("fork", self.service.fork(event_id))
            self.reload_data()

    def action_propose_probe(self) -> None:
        if event_id := self._selected_or_notify():
            self._record_action("probe", self.service.propose_probe(event_id))

    def action_run_tool(self) -> None:
        if event_id := self._selected_or_notify():
            # This emits a broker request; it never executes model text here.
            self._record_action("tool request", self.service.request_tool(event_id))

    def action_show_origin(self) -> None:
        if event_id := self._selected_or_notify():
            self._record_action("provenance", self.service.trace_event(event_id))

    def action_toggle_markdown(self) -> None:
        self.show_rendered = not self.show_rendered
        self._refresh_transcript()

    def action_generation_metadata(self) -> None:
        if event_id := self._selected_or_notify():
            self._record_action("generation", self.service.generation_metadata(event_id))


def run_tui(service: TUIService | None = None, *, session_id: str | None = None) -> None:
    OracleLabTUI(service, session_id=session_id).run()


__all__ = ["OracleLabTUI", "TUIService", "run_tui"]
