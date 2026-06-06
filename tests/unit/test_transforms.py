from __future__ import annotations

from etl_pipeline_python.transforms import cast, derive, drop, filter_by, rename, select


def test_select_keeps_only_listed_keys_in_order() -> None:
    out = select("b", "a")({"a": 1, "b": 2, "c": 3})
    assert out == {"b": 2, "a": 1}
    assert list(out.keys()) == ["b", "a"]


def test_select_ignores_missing_keys() -> None:
    assert select("a", "missing")({"a": 1}) == {"a": 1}


def test_rename_maps_keys_and_passes_through_others() -> None:
    out = rename({"id": "user_id"})({"id": 7, "name": "Ada"})
    assert out == {"user_id": 7, "name": "Ada"}


def test_cast_coerces_only_listed_fields() -> None:
    out = cast({"age": int, "score": float})({"name": "Ada", "age": "36", "score": "9.5"})
    assert out == {"name": "Ada", "age": 36, "score": 9.5}
    assert isinstance(out["age"], int)
    assert isinstance(out["score"], float)


def test_cast_does_not_mutate_input() -> None:
    source = {"age": "36"}
    cast({"age": int})(source)
    assert source == {"age": "36"}


def test_filter_by_keeps_matching_record() -> None:
    assert filter_by(lambda r: r["age"] >= 18)({"age": 21}) == {"age": 21}


def test_filter_by_drops_non_matching_record() -> None:
    assert filter_by(lambda r: r["age"] >= 18)({"age": 12}) is None


def test_derive_adds_computed_field() -> None:
    out = derive("full", lambda r: f"{r['first']} {r['last']}")(
        {"first": "Ada", "last": "Lovelace"}
    )
    assert out == {"first": "Ada", "last": "Lovelace", "full": "Ada Lovelace"}


def test_drop_removes_keys() -> None:
    assert drop("secret")({"id": 1, "secret": "x"}) == {"id": 1}
