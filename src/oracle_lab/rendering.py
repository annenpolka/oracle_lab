"""Lossless Markdown capture with derived AST and rendered-cache representations.

The source text is deliberately immutable.  Parsing and rendering are derived
operations and may be repeated without ever replacing the archived response.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from markdown_it import MarkdownIt

from oracle_lab.jsonutil import canonical_json, sha256_text

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$")


def _markdown_parser() -> MarkdownIt:
    # Raw HTML is escaped in the presentation cache.  It remains untouched in
    # ``raw_text`` and in token content, so this is a display safety decision,
    # not archival normalization.
    return MarkdownIt("commonmark", {"html": False})


def _token_dict(token: Any) -> dict[str, Any]:
    """Return a JSON-compatible, recursive markdown-it token representation."""
    children = tuple(_token_dict(child) for child in (token.children or ()))
    attrs = dict(token.attrs or {})
    line_map = list(token.map) if token.map is not None else None
    return {
        "type": token.type,
        "tag": token.tag,
        "nesting": token.nesting,
        "attrs": attrs,
        "map": line_map,
        "level": token.level,
        "children": list(children),
        "content": token.content,
        "markup": token.markup,
        "info": token.info,
        "meta": token.meta,
        "block": token.block,
        "hidden": token.hidden,
    }


def parse_markdown_ast(raw_text: str) -> list[dict[str, Any]]:
    """Parse *raw_text* without modifying it and return a serializable AST."""
    if not isinstance(raw_text, str):
        raise TypeError("raw_text must be a string")
    return [_token_dict(token) for token in _markdown_parser().parse(raw_text)]


def render_markdown_html(raw_text: str) -> str:
    """Render Markdown for presentation; the returned HTML is only a cache."""
    if not isinstance(raw_text, str):
        raise TypeError("raw_text must be a string")
    return _markdown_parser().render(raw_text)


@dataclass(frozen=True, slots=True)
class MarkdownArtifact:
    """The canonical raw response plus immutable derived representations.

    ``ast_json`` is stored rather than a mutable list so a caller cannot mutate
    archival state through a nested dictionary.  The :attr:`ast` property
    returns a fresh parsed value on every access.
    """

    raw_text: str
    raw_sha256: str
    ast_json: str
    rendered_html_cache: str | None = None

    @classmethod
    def capture(cls, raw_text: str, *, cache_rendered: bool = True) -> MarkdownArtifact:
        if not isinstance(raw_text, str):
            raise TypeError("raw_text must be a string")
        ast = parse_markdown_ast(raw_text)
        return cls(
            raw_text=raw_text,
            raw_sha256=sha256_text(raw_text),
            ast_json=canonical_json(ast),
            rendered_html_cache=render_markdown_html(raw_text) if cache_rendered else None,
        )

    @property
    def ast(self) -> list[dict[str, Any]]:
        value = json.loads(self.ast_json)
        if not isinstance(value, list):  # pragma: no cover - constructor integrity guard
            raise ValueError("stored Markdown AST must be a list")
        return value

    def assert_integrity(self) -> None:
        """Raise if the canonical text no longer matches its capture hash."""
        if sha256_text(self.raw_text) != self.raw_sha256:
            raise ValueError("raw Markdown integrity check failed")

    def with_rendered_cache(self) -> MarkdownArtifact:
        """Return a new artifact with a refreshed presentation cache."""
        self.assert_integrity()
        return MarkdownArtifact(
            raw_text=self.raw_text,
            raw_sha256=self.raw_sha256,
            ast_json=self.ast_json,
            rendered_html_cache=render_markdown_html(self.raw_text),
        )

    def without_rendered_cache(self) -> MarkdownArtifact:
        """Drop the disposable cache without touching raw text or the AST."""
        self.assert_integrity()
        return MarkdownArtifact(
            raw_text=self.raw_text,
            raw_sha256=self.raw_sha256,
            ast_json=self.ast_json,
            rendered_html_cache=None,
        )

    def to_dict(self) -> dict[str, Any]:
        self.assert_integrity()
        return {
            "raw_text": self.raw_text,
            "raw_sha256": self.raw_sha256,
            "markdown_ast": self.ast,
            "rendered_html_cache": self.rendered_html_cache,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> MarkdownArtifact:
        raw_text = value["raw_text"]
        if not isinstance(raw_text, str):
            raise TypeError("raw_text must be a string")
        expected_hash = value.get("raw_sha256", sha256_text(raw_text))
        if expected_hash != sha256_text(raw_text):
            raise ValueError("raw Markdown integrity check failed")
        ast = value.get("markdown_ast")
        if ast is None:
            ast = parse_markdown_ast(raw_text)
        if not isinstance(ast, list):
            raise TypeError("markdown_ast must be a list")
        artifact = cls(
            raw_text=raw_text,
            raw_sha256=expected_hash,
            ast_json=canonical_json(ast),
            rendered_html_cache=value.get("rendered_html_cache"),
        )
        artifact.assert_integrity()
        return artifact


class MarkdownArtifactStore:
    """Filesystem-backed derived representation store keyed by source event ID."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def path_for(self, event_id: str) -> Path:
        if not _SAFE_ID.fullmatch(event_id):
            raise ValueError("event_id is not safe for rendering-cache paths")
        return self.root / f"{event_id}.json"

    def save(self, event_id: str, artifact: MarkdownArtifact) -> Path:
        artifact.assert_integrity()
        path = self.path_for(event_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        content = (
            json.dumps(artifact.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        ).encode("utf-8")
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return path

    def load(self, event_id: str) -> MarkdownArtifact:
        path = self.path_for(event_id)
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("rendering artifact must contain a JSON object")
        return MarkdownArtifact.from_dict(value)

    def invalidate_rendered_cache(self, event_id: str) -> MarkdownArtifact:
        artifact = self.load(event_id).without_rendered_cache()
        self.save(event_id, artifact)
        return artifact


__all__ = [
    "MarkdownArtifact",
    "MarkdownArtifactStore",
    "parse_markdown_ast",
    "render_markdown_html",
]
