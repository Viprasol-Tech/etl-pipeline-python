"""Core type aliases and protocols for the ETL pipeline.

A *record* is the unit of data flowing through the pipeline: a plain
``dict[str, Any]`` mapping field names to values. Extractors yield records,
transforms map records to records (or drop them), and loaders sink records.

Part of ETL Pipeline Python by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Any, Protocol, runtime_checkable

#: A single data record — a mapping of field name to value.
Record = dict[str, Any]

#: A transform takes one record and returns either a new record or ``None``.
#: Returning ``None`` signals that the record should be filtered out.
Transform = Callable[[Record], "Record | None"]


@runtime_checkable
class Extractor(Protocol):
    """A source of records.

    Implementations yield :data:`Record` objects lazily so that large inputs
    can be streamed without loading everything into memory at once.
    """

    def extract(self) -> Iterator[Record]:
        """Yield records from the underlying source."""
        ...


@runtime_checkable
class Loader(Protocol):
    """A sink for records.

    Implementations consume an iterable of records and persist them somewhere
    (an in-memory list, a CSV string, a database, ...).
    """

    def load(self, records: Iterable[Record]) -> int:
        """Consume ``records`` and return the number loaded."""
        ...


__all__ = ["Extractor", "Loader", "Record", "Transform"]
