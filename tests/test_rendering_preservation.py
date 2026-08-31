from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from oracle_lab.rendering import MarkdownArtifact, MarkdownArtifactStore, parse_markdown_ast

FIXTURES = Path(__file__).parent / "fixtures"


def test_raw_markdown_latex_and_trailing_spaces_are_preserved_exactly() -> None:
    raw = (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8")

    artifact = MarkdownArtifact.capture(raw)

    assert artifact.raw_text == raw
    assert "`hope_filter = null`  \n" in artifact.raw_text
    assert "$$\n\\operatorname{hope\\_filter} = \\varnothing\n$$" in artifact.raw_text
    ast_contents = [
        node["content"] for node in artifact.ast if isinstance(node.get("content"), str)
    ]
    assert any("\\operatorname{hope\\_filter}" in content for content in ast_contents)
    assert artifact.rendered_html_cache is not None
    artifact.assert_integrity()


def test_render_cache_can_be_dropped_and_rebuilt_without_touching_raw() -> None:
    raw = "**0ではない**\n\n$$\nx^2\n$$\n"
    captured = MarkdownArtifact.capture(raw)

    uncached = captured.without_rendered_cache()
    rebuilt = uncached.with_rendered_cache()

    assert uncached.rendered_html_cache is None
    assert rebuilt.rendered_html_cache == captured.rendered_html_cache
    assert rebuilt.raw_text == uncached.raw_text == raw
    assert rebuilt.raw_sha256 == uncached.raw_sha256
    assert rebuilt.ast == uncached.ast


def test_artifact_round_trip_validates_raw_integrity() -> None:
    raw = "# exact\n\nLaTeX: $x + y$\n"
    artifact = MarkdownArtifact.capture(raw)

    assert MarkdownArtifact.from_dict(artifact.to_dict()) == artifact
    damaged = artifact.to_dict()
    damaged["raw_text"] = raw + "normalized"
    with pytest.raises(ValueError, match="integrity"):
        MarkdownArtifact.from_dict(damaged)

    with pytest.raises(FrozenInstanceError):
        artifact.raw_text = "mutated"  # type: ignore[misc]


def test_ast_access_returns_defensive_copies() -> None:
    artifact = MarkdownArtifact.capture("**keep me**")
    first = artifact.ast
    first[0]["content"] = "changed"

    assert artifact.ast == parse_markdown_ast("**keep me**")


def test_three_representations_are_persisted_by_source_event_id(tmp_path: Path) -> None:
    raw = (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8")
    store = MarkdownArtifactStore(tmp_path / "rendering")
    artifact = MarkdownArtifact.capture(raw)

    path = store.save("evt_source", artifact)
    restored = store.load("evt_source")
    uncached = store.invalidate_rendered_cache("evt_source")

    assert path.is_file()
    assert restored.raw_text == raw
    assert restored.ast == artifact.ast
    assert restored.rendered_html_cache == artifact.rendered_html_cache
    assert uncached.raw_text == raw
    assert uncached.rendered_html_cache is None
