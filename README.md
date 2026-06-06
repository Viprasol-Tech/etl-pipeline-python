<p align="center">
  <img src="docs/assets/logo.png" width="120" alt="Viprasol Tech logo">
</p>

<h1 align="center">ETL Pipeline Python</h1>

<p align="center">
  <strong>A composable extract, transform, and load (ETL) framework — in pure Python.</strong><br>
  Plug in a source, chain a few transforms, point at a sink, then run it all offline.
</p>

<p align="center">
  <em>Built and maintained by <a href="https://viprasol.com">Viprasol Tech</a> — Fintech Experts. Full-Stack Builders.</em>
</p>

<p align="center">
  <a href="https://github.com/Viprasol-Tech/etl-pipeline-python/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Viprasol-Tech/etl-pipeline-python/ci.yml?style=flat-square&logo=githubactions&logoColor=white&label=CI" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/Viprasol-Tech/etl-pipeline-python?style=flat-square&color=blue" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue?style=flat-square&logo=python&logoColor=white" alt="Python">
  <a href="https://t.me/viprasol_help"><img src="https://img.shields.io/badge/Telegram-support-26A5E4?style=flat-square&logo=telegram&logoColor=white" alt="Telegram"></a>
  <a href="https://github.com/Viprasol-Tech/etl-pipeline-python/stargazers"><img src="https://img.shields.io/github/stars/Viprasol-Tech/etl-pipeline-python?style=flat-square&logo=github" alt="Stars"></a>
</p>

---

## ✨ Features

- 🧩 **Composable stages** — one `Extractor`, a chain of `Transform` callables, one `Loader`.
- 🚰 **Streaming by design** — records flow lazily, so large inputs never load all at once.
- 🛠️ **Real reusable transforms** — `select`, `rename`, `cast`, `filter_by`, `derive`, `drop`.
- 🔌 **Pluggable I/O** — in-memory and CSV-string extractors and loaders, all protocol-based.
- 📏 **Run statistics** — every run reports extracted / dropped / loaded counts.
- 🖥️ **CLI** — `etl-pipeline-python demo` runs a full pipeline offline, no files or network.
- ⚙️ **Modern tooling** — ruff, mypy (strict), pytest, GitHub Actions CI.

## 🚀 Quickstart

```bash
git clone https://github.com/Viprasol-Tech/etl-pipeline-python.git
cd etl-pipeline-python
python -m pip install -e ".[dev]"

# Run an offline extract -> transform -> load demo:
etl-pipeline-python demo
```

## 🧩 Build a pipeline

```python
from etl_pipeline_python.extractors import CSVStringExtractor
from etl_pipeline_python.loaders import ListLoader
from etl_pipeline_python.pipeline import Pipeline
from etl_pipeline_python.transforms import cast, derive, filter_by, rename, select

extractor = CSVStringExtractor("id,first,last,age\n1,Ada,Lovelace,36\n2,Tim,Junior,12\n")
loader = ListLoader()

result = (
    Pipeline(extractor=extractor, loader=loader)
    .transform(cast({"age": int}))
    .transform(filter_by(lambda r: r["age"] >= 18))
    .transform(derive("name", lambda r: f"{r['first']} {r['last']}"))
    .transform(rename({"id": "user_id"}))
    .transform(select("user_id", "name", "age"))
    .run()
)

print(loader.rows)   # [{'user_id': '1', 'name': 'Ada Lovelace', 'age': 36}]
print(result)        # PipelineResult(extracted=2, dropped=1, loaded=1)
```

## 🏗️ Architecture

```mermaid
flowchart LR
    SRC[Extractor: memory / CSV] --> T1[cast]
    T1 --> T2[filter_by]
    T2 --> T3[derive / rename / select]
    T3 --> SINK[Loader: list / CSV]
```

## 🗺️ Roadmap

- [x] Composable Pipeline with streaming extract -> transform -> load
- [x] Reusable transforms: select, rename, cast, filter_by, derive, drop
- [x] In-memory + CSV-string extractors and loaders
- [ ] JSON / JSONL and SQLite extractors and loaders
- [ ] Batch + parallel execution and pipeline-level error handling
- [ ] Schema validation stage backed by pydantic

## 🤝 Contributing

PRs welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) and our [Code of Conduct](CODE_OF_CONDUCT.md).

## Contact — Viprasol Tech Private Limited

- Website: [viprasol.com](https://viprasol.com)
- Email: [support@viprasol.com](mailto:support@viprasol.com)
- Telegram: [t.me/viprasol_help](https://t.me/viprasol_help) | WhatsApp: +91 96336 52112
- GitHub: [@Viprasol-Tech](https://github.com/Viprasol-Tech) | [LinkedIn](https://www.linkedin.com/in/viprasol/) | X [@viprasol](https://twitter.com/viprasol)

## License

[MIT](LICENSE) (c) 2025 Viprasol Tech Private Limited
