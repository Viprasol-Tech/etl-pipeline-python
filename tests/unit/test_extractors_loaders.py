from __future__ import annotations

from etl_pipeline_python.extractors import CSVStringExtractor, MemoryExtractor
from etl_pipeline_python.loaders import CSVStringLoader, ListLoader


def test_memory_extractor_yields_copies() -> None:
    source = [{"a": 1}]
    extractor = MemoryExtractor(source)
    out = list(extractor.extract())
    out[0]["a"] = 99
    assert source[0]["a"] == 1  # source untouched


def test_csv_string_extractor_parses_rows() -> None:
    text = "id,name\n1,Ada\n2,Alan\n"
    records = list(CSVStringExtractor(text).extract())
    assert records == [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Alan"}]


def test_list_loader_collects_and_counts() -> None:
    loader = ListLoader()
    n = loader.load([{"a": 1}, {"a": 2}])
    assert n == 2
    assert loader.rows == [{"a": 1}, {"a": 2}]


def test_csv_string_loader_renders_header_and_rows() -> None:
    loader = CSVStringLoader()
    n = loader.load([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Alan"}])
    assert n == 2
    assert loader.text == "id,name\r\n1,Ada\r\n2,Alan\r\n"
    assert loader.fieldnames == ["id", "name"]


def test_csv_string_loader_honours_explicit_fieldnames() -> None:
    loader = CSVStringLoader(fieldnames=["name"])
    loader.load([{"id": 1, "name": "Ada"}])
    assert loader.text == "name\r\nAda\r\n"
