"""Built-in record loaders (data sinks) for the ETL pipeline.

These offline loaders cover the common cases: collecting into an in-memory
list and rendering to a CSV string. They implement the
:class:`~etl_pipeline_python.types.Loader` protocol.

Part of ETL Pipeline Python by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Sequence

from etl_pipeline_python.types import Record


class ListLoader:
    """Collect loaded records into an in-memory list.

    The accumulated records are available via :attr:`rows` after the pipeline
    runs — handy for tests and demos.
    """

    def __init__(self) -> None:
        self.rows: list[Record] = []

    def load(self, records: Iterable[Record]) -> int:
        """Append each record to :attr:`rows`; return the count loaded."""
        count = 0
        for record in records:
            self.rows.append(record)
            count += 1
        return count


class CSVStringLoader:
    """Render loaded records to a CSV string held in :attr:`text`.

    Columns are taken from ``fieldnames`` if provided, otherwise inferred from
    the union of keys across all records (first-seen order).
    """

    def __init__(self, fieldnames: Sequence[str] | None = None) -> None:
        self.fieldnames: list[str] | None = list(fieldnames) if fieldnames else None
        self.text: str = ""

    def load(self, records: Iterable[Record]) -> int:
        """Write ``records`` as CSV into :attr:`text`; return the row count."""
        materialised = list(records)
        fieldnames = self.fieldnames
        if fieldnames is None:
            fieldnames = []
            seen: set[str] = set()
            for record in materialised:
                for key in record:
                    if key not in seen:
                        seen.add(key)
                        fieldnames.append(key)
            self.fieldnames = fieldnames

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in materialised:
            writer.writerow(record)
        self.text = buffer.getvalue()
        return len(materialised)


__all__ = ["CSVStringLoader", "ListLoader"]
