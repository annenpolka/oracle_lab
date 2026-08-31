from __future__ import annotations

import asyncio
from typing import Any

from textual.widgets import Label, Markdown, Static, Tree

from oracle_lab.tui import OracleLabTUI


class TUIRecordingService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.sessions = [{"id": "ses_1", "title": "Experiment", "current_branch_id": "br_main"}]
        self.branches = [
            {"id": "br_main", "session_id": "ses_1", "title": "main"},
            {"id": "br_child", "session_id": "ses_1", "title": "child"},
        ]
        self.events = [
            {
                "id": "evt_main",
                "type": "oracle.output",
                "session_id": "ses_1",
                "branch_id": "br_main",
                "payload": {"raw_text": "**0ではない**\n\n$$\nx^2\n$$"},
            },
            {
                "id": "evt_child",
                "type": "analysis.claim_detected",
                "session_id": "ses_1",
                "branch_id": "br_child",
                "payload": {"text": "34.7°"},
            },
        ]

    def list_sessions(self) -> list[dict[str, Any]]:
        return self.sessions

    def list_branches(self, session_id: str | None = None) -> list[dict[str, Any]]:
        return [branch for branch in self.branches if branch["session_id"] == session_id]

    def list_events(self, session_id: str | None = None) -> list[dict[str, Any]]:
        return [event for event in self.events if event["session_id"] == session_id]

    def _record(self, name: str, *args: Any) -> dict[str, Any]:
        self.calls.append((name, args))
        return {"action": name, "args": args}

    def switch_session(self, session_id: str) -> dict[str, Any]:
        self._record("switch_session", session_id)
        return self.sessions[0]

    def switch_branch(self, session_id: str, branch_id: str) -> dict[str, Any]:
        return self._record("switch_branch", session_id, branch_id)

    def keep(self, event_id: str) -> dict[str, Any]:
        return self._record("keep", event_id)

    def star(self, event_id: str) -> dict[str, Any]:
        return self._record("star", event_id)

    def reject(self, event_id: str) -> dict[str, Any]:
        return self._record("reject", event_id)

    def fork(self, event_id: str, title: str | None = None) -> dict[str, Any]:
        return self._record("fork", event_id, title)

    def propose_probe(self, event_id: str) -> dict[str, Any]:
        return self._record("propose_probe", event_id)

    def request_tool(self, event_id: str) -> dict[str, Any]:
        return self._record("request_tool", event_id)

    def trace_event(self, event_id: str) -> dict[str, Any]:
        return self._record("trace_event", event_id)

    def generation_metadata(self, event_id: str) -> dict[str, Any]:
        return self._record("generation_metadata", event_id)


def test_four_panes_hotkeys_and_raw_rendered_toggle_work_headlessly() -> None:
    async def exercise() -> None:
        service = TUIRecordingService()
        app = OracleLabTUI(service, session_id="ses_1", branch_id="br_main")
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            assert app.query_one("#sessions-pane")
            assert app.query_one("#transcript-pane")
            assert app.query_one("#claims-pane")
            assert app.query_one("#events-pane")
            assert app.selected_event_id == "evt_main"
            raw = app.query_one("#transcript-raw", Static)
            rendered = app.query_one("#transcript-rendered", Markdown)
            assert raw.display is True
            assert rendered.display is False

            await pilot.press("m")
            assert raw.display is False
            assert rendered.display is True

            for key in ("k", "s", "r", "f", "p", "t", "o", "g"):
                await pilot.press(key)
            names = [name for name, _ in service.calls]
            for expected in (
                "keep",
                "star",
                "reject",
                "fork",
                "propose_probe",
                "request_tool",
                "trace_event",
                "generation_metadata",
            ):
                assert expected in names

    asyncio.run(exercise())


def test_synthetic_origin_is_visible_and_keep_star_are_disabled() -> None:
    async def exercise() -> None:
        service = TUIRecordingService()
        service.events.extend(
            [
                {
                    "id": "evt_synthetic_analysis",
                    "type": "analysis.claim_detected",
                    "session_id": "ses_1",
                    "branch_id": "br_main",
                    "synthetic_lineage": True,
                    "material_origins": ["synthetic_fixture"],
                    "payload": {"text": "SYNTHETIC CLAIM MUST NOT BE CURATED"},
                },
                {
                    "id": "evt_synthetic_oracle",
                    "type": "oracle.output",
                    "session_id": "ses_1",
                    "branch_id": "br_main",
                    "synthetic_lineage": True,
                    "material_origins": ["synthetic_fixture"],
                    "payload": {
                        "raw_text": "fixture only",
                        "material_origin": "synthetic_fixture",
                    },
                },
            ]
        )
        app = OracleLabTUI(service, session_id="ses_1", branch_id="br_main")
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            assert app.selected_event_id == "evt_synthetic_oracle"
            assert app.selected_event_synthetic is True
            title = str(app.query_one("#transcript-title", Label).render())
            assert "origin: synthetic_fixture" in title
            assert "KEEP/STAR DISABLED" in title

            await pilot.press("k")
            await pilot.press("s")
            assert all(name not in {"keep", "star"} for name, _ in service.calls)

    asyncio.run(exercise())


def test_session_tree_contains_branches_and_event_selection_changes_context() -> None:
    async def exercise() -> None:
        service = TUIRecordingService()
        app = OracleLabTUI(service, session_id="ses_1", branch_id="br_main")
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            tree = app.query_one("#session-tree", Tree)
            session_node = tree.root.children[0]
            assert len(session_node.children) == 2
            child_branch = session_node.children[1]
            child_event = child_branch.children[0]

            app.on_tree_node_selected(Tree.NodeSelected(child_branch))
            assert app.branch_id == "br_child"
            assert ("switch_branch", ("ses_1", "br_child")) in service.calls

            app.on_tree_node_selected(Tree.NodeSelected(child_event))
            assert app.selected_event_id == "evt_child"
            assert app.selected_event_text == "34.7°"

    asyncio.run(exercise())
