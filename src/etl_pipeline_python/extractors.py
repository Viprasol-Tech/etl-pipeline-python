"""Built-in record extractors (data sources) for the ETL pipeline.

These offline extractors cover the common cases: an in-memory iterable and a
CSV string. They implement the :class:`~etl_pipeline_python.types.Extractor`
protocol, so the pipeline never needs to know which source it is reading from.

Part of ETL Pipeline Python by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Iterator

from etl_pipeline_python.types import Record


class MemoryExtractor:
    """Yield records from an in-memory iterable of dicts.

    A defensive shallow copy of each record is yielded so that downstream
    transforms can never mutate the caller's source data.
    """

    def __init__(self, records: Iterable[Record]) -> None:
        self._records = list(records)

    def extract(self) -> Iterator[Record]:
        """Yield a shallow copy of each source record."""
        for record in self._records:
            yield dict(record)


class CSVStringExtractor:
    """Parse a CSV string into records keyed by the header row.

    Every value is read as ``str``; use the :func:`~etl_pipeline_python.transforms.cast`
    transform downstream to coerce numeric or boolean columns.
    """

    def __init__(self, text: str, *, delimiter: str = ",") -> None:
        self._text = text
        self._delimiter = delimiter

    def extract(self) -> Iterator[Record]:
        """Yield one record per CSV data row, keyed by the header."""
        reader = csv.DictReader(io.StringIO(self._text), delimiter=self._delimiter)
        for row in reader:
            yield dict(row)


__all__ = ["CSVStringExtractor", "MemoryExtractor"]
