from oracle_lab.ids import new_id


def test_prefixed_ulids_sort_by_timestamp() -> None:
    earlier = new_id("evt", timestamp_ms=1_000)
    later = new_id("evt", timestamp_ms=1_001)

    assert earlier.startswith("evt_")
    assert len(earlier.removeprefix("evt_")) == 26
    assert earlier < later


def test_ulids_are_monotonic_within_one_millisecond() -> None:
    identifiers = [new_id("job", timestamp_ms=42_000) for _ in range(20)]

    assert identifiers == sorted(identifiers)
    assert len(set(identifiers)) == len(identifiers)


def test_invalid_prefix_is_rejected() -> None:
    try:
        new_id("event/type")
    except ValueError as error:
        assert "prefix" in str(error)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("invalid prefix was accepted")
