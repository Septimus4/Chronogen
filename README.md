# Chronogen

Chronogen creates date-based wordlists that can be used for penetration testing, QA fixtures, or any workflow that needs exhaustive date coverage. The project now ships as a Python package with a modern command line interface and documentation.

## Features

- Symbolic format templates (`YYYYMMDD`, `YYMMDD`, `MM`, `MMDD`, etc.)
- Optional custom `strftime` patterns
- Prefix, suffix, casing, and separator controls
- Month/day filtering, leap-day aware
- Reverse chronological generation for targeting the latest dates first
- CLI, `python -m chronogen`, or importable library

## Installation

```bash
pip install .
```

For development extras (linting + tests):

```bash
pip install -e .[dev]
```

## Command line usage

```bash
chronogen --start 1990 --end 1995 --format DDMMYY --separator "/" --prefix corp-
```

Use `chronogen --help` or consult the [CLI documentation](docs/cli.md) for all options. The tool can also be executed with `python -m chronogen` (module execution) during development.

## Library usage

```python
from chronogen import DateGenerator

generator = DateGenerator(start_year=2020, end_year=2021, format="YYYYMMDD", separator="-")
for value in generator.generate():
    print(value)
```

More examples are available in the [library guide](docs/library.md).

## Development

See [docs/development.md](docs/development.md) for linting, testing, and release workflows. Continuous integration is provided through GitHub Actions and enforces lint + coverage on pull requests.

## License

This project is distributed under the terms of the GNU General Public License v3.0. See [LICENSE](LICENSE).
