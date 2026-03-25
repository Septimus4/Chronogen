# Development Guide

This project follows a standard `src/` layout with packaging metadata defined in `pyproject.toml`.

## Requirements

- Python 3.8+
- `pip` for dependency management

Install dev dependencies:

```bash
pip install -e .[dev]
```

## Building distributions

Install the [PyPA `build` frontend](https://pypi.org/project/build/) and create source and wheel archives with:

```bash
python -m build
```

The command writes artifacts to the `dist/` directory.

## Code quality

Run Ruff to lint the code base:

```bash
ruff check .
```

## Testing & coverage

Tests are written with `pytest` and collect coverage information automatically.

```bash
pytest
```

Coverage reports are printed to the terminal and should remain near 80% for new contributions.

## Continuous integration

GitHub Actions executes linting and test jobs on every push and pull request (`.github/workflows/ci.yml`). Ensure the workflow passes locally before submitting changes.

## Releasing

Chronogen uses [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/) to release from
GitHub Actions. Complete these steps the first time to finish wiring PyPI to the repository:

1. Log in to PyPI, open the project, and visit **Publishing** under **Project settings**.
2. In the **Pending publishers** table, locate the row for `Septimus4/Chronogen` and click **Approve**.
   PyPI now trusts the `publish.yml` workflow for releases.

For every release:

1. Update the version in `pyproject.toml` and `docs/readme.md` if necessary.
2. Ensure the GitHub environment named `pypi` contains a `PYPI_API_TOKEN` secret with an API token that
   has *Maintain* or *Owner* permissions for the project.
3. Commit and push your changes, then draft a GitHub release. Publishing the release triggers the
   `Publish to PyPI` workflow, which runs tests, builds wheels and source distributions, and uploads
   them to PyPI automatically.
