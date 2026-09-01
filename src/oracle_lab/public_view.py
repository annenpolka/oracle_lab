"""Pure redaction for derived, human-facing Oracle Lab views.

Canonical events and archives deliberately retain exact provider response
metadata.  This module copies JSON-like values for public display and redacts
infrastructure secrets only in explicit metadata fields.  It treats Oracle
prompt, content, reasoning, and context messages as opaque material.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

REDACTED = "[redacted]"

_KEY_SEPARATOR_RE = re.compile(r"[^a-z0-9]+")
_SENSITIVE_KEY_SUFFIXES = (
    "accesskey",
    "apikey",
    "authorization",
    "cookie",
    "credentials",
    "credential",
    "password",
    "passwd",
    "privatekey",
    "secret",
    "secretaccesskey",
)
_SENSITIVE_TOKEN_SUFFIXES = (
    "accesstoken",
    "accesstokens",
    "apitoken",
    "authtoken",
    "authenticationtoken",
    "bearertoken",
    "csrftoken",
    "githubtoken",
    "idtoken",
    "oauthtoken",
    "personalaccesstoken",
    "privatetoken",
    "providertoken",
    "refreshtoken",
    "securitytoken",
    "sessiontoken",
    "xauthtoken",
)
_SENSITIVE_KEY_WRAPPERS = ("", "data", "field", "header", "id", "map", "name", "value")
_PUBLIC_METADATA_KEYS = frozenset(
    {
        "apiresponsemetadata",
        "actualmodelidentifier",
        "actualprovider",
        "effectivesampling",
        "generationidentity",
        "generationsettings",
        "metadata",
        "model",
        "modelidentity",
        "modelprofileid",
        "provider",
        "providermodelid",
        "providername",
        "providerrequestid",
        "providerrouting",
        "requestedsampling",
        "routedprovidername",
        "sampling",
        "usage",
    }
)
_OPAQUE_MATERIAL_KEYS = frozenset(
    {
        "content",
        "messages",
        "note",
        "output",
        "prompt",
        "rawtext",
        "reasoning",
        "text",
    }
)
_AUTH_SCHEME_VALUE_RE = re.compile(r"^\s*(?:bearer|basic)\s+\S+\s*$", re.IGNORECASE)
_SECRET_PREFIX_VALUE_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"sk(?:_(?:live|test))?[_-][A-Za-z0-9][A-Za-z0-9._-]{5,}"
    r"|gh[pousr]_[A-Za-z0-9][A-Za-z0-9._-]{5,}"
    r"|github_pat_[A-Za-z0-9][A-Za-z0-9_]{5,}"
    r"|glpat-[A-Za-z0-9_-]{10,}"
    r"|npm_[A-Za-z0-9]{10,}"
    r"|pypi-[A-Za-z0-9_-]{10,}"
    r"|(?:AKIA|ASIA)[0-9A-Z]{16}"
    r"|AIza[A-Za-z0-9_-]{20,}"
    r"|eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}"
    r"|xox[baprs]-[A-Za-z0-9][A-Za-z0-9-]{5,}"
    r")",
    re.IGNORECASE,
)


def public_view(value: Any) -> Any:
    """Return a non-mutating, idempotent public view of a JSON-like value.

    Secret-value scanning is intentionally scoped to named metadata containers.
    Prompt, content, reasoning, and context fields may themselves discuss or
    hallucinate credential-like strings; those fields remain exact at this
    derived-view boundary.
    """

    return _public_view(value, inside_public_metadata=False)


def _public_view(value: Any, *, inside_public_metadata: bool) -> Any:
    if isinstance(value, Mapping):
        copied: dict[Any, Any] = {}
        for key, item in value.items():
            if not inside_public_metadata and _is_opaque_material_key(key):
                copied[key] = _copy_unredacted(item)
                continue
            item_inside_metadata = inside_public_metadata or _is_public_metadata_key(key)
            if inside_public_metadata and _is_sensitive_metadata_key(key):
                copied[key] = REDACTED
            else:
                copied[key] = _public_view(
                    item,
                    inside_public_metadata=item_inside_metadata,
                )
        return copied
    if isinstance(value, list):
        return [_public_view(item, inside_public_metadata=inside_public_metadata) for item in value]
    if isinstance(value, tuple):
        return tuple(
            _public_view(item, inside_public_metadata=inside_public_metadata) for item in value
        )
    if inside_public_metadata and isinstance(value, str) and _is_secret_like_metadata_value(value):
        return REDACTED
    return value


def _normalized_key(key: object) -> str:
    if not isinstance(key, str):
        return ""
    return _KEY_SEPARATOR_RE.sub("", key.casefold())


def _is_public_metadata_key(key: object) -> bool:
    return _normalized_key(key) in _PUBLIC_METADATA_KEYS


def _is_opaque_material_key(key: object) -> bool:
    return _normalized_key(key) in _OPAQUE_MATERIAL_KEYS


def _copy_unredacted(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _copy_unredacted(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_unredacted(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_copy_unredacted(item) for item in value)
    return value


def _is_sensitive_metadata_key(key: object) -> bool:
    normalized = _normalized_key(key)
    if not normalized:
        return False
    for wrapper in _SENSITIVE_KEY_WRAPPERS:
        if wrapper and not normalized.endswith(wrapper):
            continue
        candidate = normalized[: -len(wrapper)] if wrapper else normalized
        if any(candidate.endswith(suffix) for suffix in _SENSITIVE_KEY_SUFFIXES):
            return True
        if candidate in {"token", "tokens"} or candidate.endswith(_SENSITIVE_TOKEN_SUFFIXES):
            return True
    return False


def _is_secret_like_metadata_value(value: str) -> bool:
    if value == REDACTED:
        return False
    return bool(_AUTH_SCHEME_VALUE_RE.fullmatch(value) or _SECRET_PREFIX_VALUE_RE.search(value))
