from __future__ import annotations

from etl_pipeline_python.extractors import CSVStringExtractor, MemoryExtractor
from etl_pipeline_python.loaders import ListLoader
from etl_pipeline_python.pipeline import Pipeline
from etl_pipeline_python.transforms import cast, derive, filter_by, rename, select


def test_full_pipeline_extract_transform_load() -> None:
    extractor = MemoryExtractor(
        [
            {"id": "1", "first": "Ada", "last": "Lovelace", "age": "36"},
            {"id": "2", "first": "Tim", "last": "Junior", "age": "12"},
        ]
    )
    loader = ListLoader()
    result = (
        Pipeline(extractor=extractor, loader=loader)
        .transform(cast({"age": int}))
        .transform(filter_by(lambda r: r["age"] >= 18))
        .transform(derive("name", lambda r: f"{r['first']} {r['last']}"))
        .transform(select("id", "name", "age"))
        .run()
    )

    assert loader.rows == [{"id": "1", "name": "Ada Lovelace", "age": 36}]
    assert result.extracted == 2
    assert result.dropped == 1
    assert result.loaded == 1


def test_filter_drops_non_matching_rows_from_load() -> None:
    extractor = MemoryExtractor([{"v": 1}, {"v": 2}, {"v": 3}])
    loader = ListLoader()
    result = (
        Pipeline(extractor=extractor, loader=loader)
        .transform(filter_by(lambda r: r["v"] % 2 == 1))
        .run()
    )
    assert loader.rows == [{"v": 1}, {"v": 3}]
    assert result.loaded == 2
    assert result.dropped == 1


def test_chaining_order_is_respected() -> None:
    # rename id->key must happen BEFORE select("key"); reversed order yields nothing.
    extractor = MemoryExtractor([{"id": 7}])
    loader = ListLoader()
    (
        Pipeline(extractor=extractor, loader=loader)
        .transform(rename({"id": "key"}))
        .transform(select("key"))
        .run()
    )
    assert loader.rows == [{"key": 7}]


def test_chaining_wrong_order_loses_field() -> None:
    extractor = MemoryExtractor([{"id": 7}])
    loader = ListLoader()
    (
        Pipeline(extractor=extractor, loader=loader)
        .transform(select("key"))  # "key" does not exist yet
        .transform(rename({"id": "key"}))
        .run()
    )
    assert loader.rows == [{}]


def test_type_casting_through_pipeline() -> None:
    extractor = CSVStringExtractor("n\n10\n20\n")
    loader = ListLoader()
    Pipeline(extractor=extractor, loader=loader).transform(cast({"n": int})).run()
    assert loader.rows == [{"n": 10}, {"n": 20}]
    assert all(isinstance(r["n"], int) for r in loader.rows)


def test_pipeline_with_no_transforms_is_passthrough() -> None:
    extractor = MemoryExtractor([{"a": 1}, {"a": 2}])
    loader = ListLoader()
    result = Pipeline(extractor=extractor, loader=loader).run()
    assert loader.rows == [{"a": 1}, {"a": 2}]
    assert result.extracted == 2
    assert result.dropped == 0
    assert result.loaded == 2
