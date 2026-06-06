"""The composable ETL pipeline.

A :class:`Pipeline` wires one :class:`~etl_pipeline_python.types.Extractor`, an
ordered chain of :data:`~etl_pipeline_python.types.Transform` callables, and one
:class:`~etl_pipeline_python.types.Loader`. Records are streamed lazily:
``extract -> transform_1 -> ... -> transform_n -> load``. A transform that
returns ``None`` drops the record, short-circuiting the rest of the chain for
that record.

Part of ETL Pipeline Python by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field

from etl_pipeline_python.types import Extractor, Loader, Record, Transform


@dataclass(slots=True)
class PipelineResult:
    """Outcome of a single pipeline run.

    Attributes:
        extracted: Number of records produced by the extractor.
        dropped: Number of records filtered out by transforms.
        loaded: Number of records handed to (and accepted by) the loader.
    """

    extracted: int = 0
    dropped: int = 0
    loaded: int = 0


@dataclass(slots=True)
class Pipeline:
    """Compose an extractor, a chain of transforms, and a loader.

    Build a pipeline either by passing ``transforms`` up front or by chaining
    :meth:`transform` calls (each returns ``self`` for fluent composition).
    """

    extractor: Extractor
    loader: Loader
    transforms: list[Transform] = field(default_factory=list)

    def transform(self, fn: Transform) -> Pipeline:
        """Append a transform to the chain and return ``self`` for chaining."""
        self.transforms.append(fn)
        return self

    def _apply(self, record: Record) -> Record | None:
        """Run ``record`` through every transform in order.

        Returns the transformed record, or ``None`` as soon as any transform
        drops it.
        """
        current: Record | None = record
        for fn in self.transforms:
            if current is None:
                return None
            current = fn(current)
        return current

    def _stream(self, result: PipelineResult) -> Iterator[Record]:
        """Yield surviving, transformed records while tallying ``result``."""
        for record in self.extractor.extract():
            result.extracted += 1
            transformed = self._apply(record)
            if transformed is None:
                result.dropped += 1
                continue
            yield transformed

    def run(self) -> PipelineResult:
        """Execute the pipeline end-to-end and return run statistics."""
        result = PipelineResult()
        result.loaded = self.loader.load(self._stream(result))
        return result


__all__ = ["Pipeline", "PipelineResult"]
