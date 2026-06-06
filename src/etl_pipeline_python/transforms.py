"""Reusable record transforms for the ETL pipeline.

Each factory here returns a :data:`~etl_pipeline_python.types.Transform`: a
callable taking one record and returning either a new record or ``None`` to
drop it. Transforms are pure — they never mutate their input record — so they
compose cleanly and are trivial to unit-test.

Part of ETL Pipeline Python by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from etl_pipeline_python.types import Record, Transform


def select(*keys: str) -> Transform:
    """Keep only ``keys`` in each record, preserving their given order.

    Missing keys are silently skipped, so ``select`` doubles as a projection
    that tolerates sparse records.

    >>> select("a", "b")({"a": 1, "b": 2, "c": 3})
    {'a': 1, 'b': 2}
    """

    def _select(record: Record) -> Record:
        return {key: record[key] for key in keys if key in record}

    return _select


def rename(mapping: dict[str, str]) -> Transform:
    """Rename keys according to ``mapping`` (``old -> new``).

    Keys absent from ``mapping`` are passed through unchanged. The relative
    order of fields is preserved.

    >>> rename({"id": "user_id"})({"id": 7, "name": "Ada"})
    {'user_id': 7, 'name': 'Ada'}
    """

    def _rename(record: Record) -> Record:
        return {mapping.get(key, key): value for key, value in record.items()}

    return _rename


def cast(types: dict[str, Callable[[Any], Any]]) -> Transform:
    """Cast selected fields using the given callables (e.g. ``{"age": int}``).

    Only fields present in both the record and ``types`` are cast; everything
    else passes through untouched.

    >>> cast({"age": int})({"name": "Ada", "age": "36"})
    {'name': 'Ada', 'age': 36}
    """

    def _cast(record: Record) -> Record:
        out = dict(record)
        for key, caster in types.items():
            if key in out:
                out[key] = caster(out[key])
        return out

    return _cast


def filter_by(predicate: Callable[[Record], bool]) -> Transform:
    """Keep records for which ``predicate`` is true; drop the rest.

    >>> keep_adults = filter_by(lambda r: r["age"] >= 18)
    >>> keep_adults({"age": 21})
    {'age': 21}
    >>> keep_adults({"age": 12}) is None
    True
    """

    def _filter(record: Record) -> Record | None:
        return record if predicate(record) else None

    return _filter


def derive(field: str, fn: Callable[[Record], Any]) -> Transform:
    """Add (or overwrite) ``field`` with ``fn(record)``.

    The function receives the whole record, so derived fields can combine
    several inputs.

    >>> full = derive("full", lambda r: f"{r['first']} {r['last']}")
    >>> full({"first": "Ada", "last": "Lovelace"})
    {'first': 'Ada', 'last': 'Lovelace', 'full': 'Ada Lovelace'}
    """

    def _derive(record: Record) -> Record:
        out = dict(record)
        out[field] = fn(record)
        return out

    return _derive


def drop(*keys: str) -> Transform:
    """Remove ``keys`` from each record; missing keys are ignored.

    >>> drop("secret")({"id": 1, "secret": "x"})
    {'id': 1}
    """

    def _drop(record: Record) -> Record:
        return {key: value for key, value in record.items() if key not in keys}

    return _drop


__all__ = ["cast", "derive", "drop", "filter_by", "rename", "select"]
