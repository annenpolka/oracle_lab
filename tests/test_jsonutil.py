from decimal import Decimal
from types import MappingProxyType

import pytest

from oracle_lab.jsonutil import canonical_json, sha256_json


def test_canonical_json_is_order_independent_and_unicode_preserving() -> None:
    left = {"b": 2, "日本語": "保持", "a": [Decimal("1.0")]}
    right = {"a": [Decimal("1.0")], "日本語": "保持", "b": 2}

    assert canonical_json(left) == canonical_json(right)
    assert "日本語" in canonical_json(left)
    assert sha256_json(left) == sha256_json(right)


def test_non_finite_numbers_are_not_hashable_protocol_values() -> None:
    with pytest.raises(ValueError):
        canonical_json({"value": float("nan")})


def test_immutable_mapping_views_remain_canonical_json_values() -> None:
    frozen = MappingProxyType({"nested": MappingProxyType({"value": 1})})

    assert canonical_json(frozen) == '{"nested":{"value":1}}'
