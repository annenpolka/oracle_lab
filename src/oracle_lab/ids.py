"""Sortable identifiers without a database or service dependency."""

from __future__ import annotations

import secrets
import threading
import time

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_MONOTONIC_LOCK = threading.Lock()
_LAST_MILLIS = -1
_LAST_RANDOM = -1
_EXPLICIT_RANDOM: dict[int, int] = {}


def _encode_ulid(value: bytes) -> str:
    """Encode 128 bits as a canonical 26-character Crockford Base32 ULID."""
    number = int.from_bytes(value, "big")
    chars = ["0"] * 26
    for index in range(25, -1, -1):
        chars[index] = _CROCKFORD[number & 31]
        number >>= 5
    return "".join(chars)


def new_id(prefix: str = "evt", *, timestamp_ms: int | None = None) -> str:
    """Return a lexicographically sortable, prefixed ULID.

    The first 48 bits are Unix milliseconds and the remaining 80 bits are
    cryptographically random. Prefixes make accidental cross-entity use easy
    to spot while retaining sort order within each entity type.
    """
    if not prefix or not prefix.replace("-", "").isalnum():
        raise ValueError("prefix must contain only letters, numbers, or hyphens")
    global _LAST_MILLIS, _LAST_RANDOM

    millis = int(time.time() * 1000) if timestamp_ms is None else timestamp_ms
    if not 0 <= millis < 2**48:
        raise ValueError("timestamp_ms must fit in 48 bits")
    with _MONOTONIC_LOCK:
        if timestamp_ms is not None:
            previous = _EXPLICIT_RANDOM.get(millis)
            random_part = (
                int.from_bytes(secrets.token_bytes(10), "big") if previous is None else previous + 1
            )
            if random_part >= 2**80:
                raise OverflowError("ULID random component exhausted for timestamp")
            _EXPLICIT_RANDOM[millis] = random_part
            raw = millis.to_bytes(6, "big") + random_part.to_bytes(10, "big")
            return f"{prefix}_{_encode_ulid(raw)}"
        # A real clock can move backwards, and several IDs commonly share one
        # millisecond. For ordinary generation, preserve process-local creation
        # order by holding the previous timestamp and incrementing the random
        # suffix. Explicit historical timestamps remain exact for fixtures.
        if timestamp_ms is None and millis < _LAST_MILLIS:
            millis = _LAST_MILLIS
        if millis == _LAST_MILLIS:
            random_part = _LAST_RANDOM + 1
            if random_part >= 2**80:  # practically unreachable rollover guard
                millis += 1
                random_part = int.from_bytes(secrets.token_bytes(10), "big")
        else:
            random_part = int.from_bytes(secrets.token_bytes(10), "big")
        _LAST_MILLIS = millis
        _LAST_RANDOM = random_part
    raw = millis.to_bytes(6, "big") + random_part.to_bytes(10, "big")
    return f"{prefix}_{_encode_ulid(raw)}"
