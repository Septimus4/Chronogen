"""High level API for generating date wordlists and iterators."""

from importlib import metadata

from .core import (
    DateGenerator,
    DateGeneratorConfig,
    DateGeneratorError,
    generate_dates,
    parse_format_spec,
)

__all__ = [
    "DateGenerator",
    "DateGeneratorConfig",
    "DateGeneratorError",
    "generate_dates",
    "parse_format_spec",
]

try:
    __version__ = metadata.version("chronogen")
except metadata.PackageNotFoundError:
    # Fallback for development (not installed)
    __version__ = "dev"
