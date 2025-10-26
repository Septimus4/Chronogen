"""Core date generation utilities.

Simplified implementation:
* Removed ``FormatChunk`` indirection; format specs now compile to simple tokens.
* ``parse_format_spec`` accepts any component order (Y/M/D groups) without positional
    restrictions so long as each component appears at most once.
* Lazy compilation of the formatter the first time formatting is required.
* Iteration works over raw integers (year, month, day) and only materialises a
    ``date`` object when a custom ``strftime`` pattern is used.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Iterator, Sequence

__all__ = [
    "DateGenerator",
    "DateGeneratorConfig",
    "DateGeneratorError",
    "generate_dates",
    "parse_format_spec",
]


class DateGeneratorError(ValueError):
    """Base error raised for invalid configuration."""


def parse_format_spec(spec: str) -> tuple[str, ...]:
    """Parse a symbolic date format specification into ordered component tokens.

    Accepted characters are ``Y``, ``M``, and ``D`` grouped contiguously.
    Rules:
    * At most one group for each letter.
    * ``Y`` group length must be 2 (short year) or 4 (full year).
    * ``M`` and ``D`` groups must be exactly length 2.
    * Any order is accepted (e.g. ``DDMMYYYY`` or ``MMYYYY`` or ``YYMMDD``).

    The returned tuple contains tokens from: ``'Y4'``, ``'Y2'``, ``'M'``, ``'D'``.
    """

    if not spec:
        raise DateGeneratorError("format string must not be empty")
    text = spec.upper().strip()
    invalid = set(text) - {"Y", "M", "D"}
    if invalid:
        joined = ", ".join(sorted(invalid))
        raise DateGeneratorError(f"format may only contain Y, M, and D characters (found: {joined})")

    # Group contiguous identical characters using a simple scan (clearer & faster than itertools.groupby here)
    groups: list[tuple[str, int]] = []
    current = text[0]
    count = 1
    for char in text[1:]:
        if char == current:
            count += 1
        else:
            groups.append((current, count))
            current = char
            count = 1
    groups.append((current, count))

    seen: set[str] = set()
    tokens: list[str] = []
    for letter, length in groups:
        if letter in seen:
            raise DateGeneratorError(f"{letter} appears multiple times; combine into a single group")
        seen.add(letter)
        if letter == "Y":
            if length not in {2, 4}:
                raise DateGeneratorError("Y groups must be either 'YY' or 'YYYY'")
            tokens.append("Y4" if length == 4 else "Y2")
        elif letter == "M":
            if length != 2:
                raise DateGeneratorError("M groups must be exactly 'MM'")
            tokens.append("M")
        elif letter == "D":
            if length != 2:
                raise DateGeneratorError("D groups must be exactly 'DD'")
            tokens.append("D")

    if not tokens:
        raise DateGeneratorError("format must include at least one component (Y, M, or D)")
    return tuple(tokens)


@dataclass
class DateGeneratorConfig:
    """Configuration object for :class:`DateGenerator`."""

    start_year: int
    end_year: int
    format: str = "YYYYMMDD"
    separator: str = ""
    custom_pattern: str | None = None
    months: Sequence[int] | None = None
    days: Sequence[int] | None = None
    prefix: str = ""
    suffix: str = ""
    case: str | None = None
    reverse: bool = False

    def normalized(self) -> DateGeneratorConfig:
        """Return a validated, normalized copy of this configuration."""

        if self.start_year > self.end_year:
            raise DateGeneratorError("start_year must be less than or equal to end_year")

        case_normalized = None
        if self.case:
            lowered = self.case.lower()
            if lowered not in {"lower", "upper"}:
                raise DateGeneratorError("case must be 'lower' or 'upper' when provided")
            case_normalized = lowered

        months = self._normalize_int_sequence(self.months, 1, 12, "months")
        days = self._normalize_int_sequence(self.days, 1, 31, "days")

        custom_pattern = self.custom_pattern
        format_normalized = self.format.upper().strip() if self.format else ""
        if custom_pattern:
            self._validate_pattern(custom_pattern)
        else:
            # Eager validation so construction errors fast (tests rely on this)
            parse_format_spec(format_normalized or "YYYYMMDD")

        config = DateGeneratorConfig(
            start_year=self.start_year,
            end_year=self.end_year,
            format=format_normalized or "YYYYMMDD",
            separator=self.separator,
            custom_pattern=custom_pattern,
            months=months,
            days=days,
            prefix=self.prefix,
            suffix=self.suffix,
            case=case_normalized,
            reverse=self.reverse,
        )
        return config

    @staticmethod
    def _normalize_int_sequence(
        values: Sequence[int] | None, minimum: int, maximum: int, field: str
    ) -> tuple[int, ...] | None:
        if values is None:
            return None
        normalized: list[int] = []
        for value in values:
            if not minimum <= int(value) <= maximum:
                raise DateGeneratorError(f"{field} must be between {minimum} and {maximum}")
            if int(value) not in normalized:
                normalized.append(int(value))
        return tuple(sorted(normalized))

    @staticmethod
    def _validate_pattern(pattern: str) -> None:
        try:
            datetime(2000, 1, 1).strftime(pattern)
        except ValueError as exc:  # pragma: no cover - only triggered with invalid patterns
            raise DateGeneratorError(f"Invalid custom pattern: {pattern}") from exc


class DateGenerator:
    """Generate formatted date strings for wordlists and automation scripts."""

    def __init__(self, config: DateGeneratorConfig | None = None, /, **kwargs) -> None:
        if config and kwargs:
            raise DateGeneratorError("Provide either a config object or keyword arguments, not both")
        if config is None:
            config = DateGeneratorConfig(**kwargs)
        self.config = config.normalized()
        # Lazy compiled spec tokens & formatter cache
        self._spec_tokens: tuple[str, ...] | None = None
        self._compile_cache: list[str] | None = None  # used only to remember token order

    def __iter__(self) -> Iterator[str]:
        return self.generate()

    def generate(self) -> Iterator[str]:
        """Yield formatted date strings according to the configuration."""

        if self.config.custom_pattern:
            # Need full date objects for strftime
            for y, m, d in self._iter_ymd():
                yield self._format_custom(y, m, d)
        else:
            for y, m, d in self._iter_ymd():
                yield self._format_spec(y, m, d)

    def generate_to_list(self) -> list[str]:
        """Return all generated date strings in a list."""

        return list(self.generate())

    def write(self, destination: Path | str, newline: str = "\n", chunk_size: int = 1000) -> Path:
        """Write generated date strings to ``destination`` and return the path."""

        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as stream:
            chunk = []
            for value in self.generate():
                chunk.append(value)
                if len(chunk) >= chunk_size:
                    stream.write(newline.join(chunk) + newline)
                    chunk = []
            if chunk:
                stream.write(newline.join(chunk) + newline)
        return path

    # --- Iteration helpers -------------------------------------------------
    def _iter_ymd(self) -> Iterator[tuple[int, int, int]]:
        """Iterate over (year, month, day) tuples according to configuration."""

        months: Sequence[int] = self.config.months or tuple(range(1, 13))
        reverse = self.config.reverse
        years: Iterable[int]
        if reverse:
            years = range(self.config.end_year, self.config.start_year - 1, -1)
            months_iter_factory = lambda: reversed(months)  # noqa: E731 - concise inline
        else:
            years = range(self.config.start_year, self.config.end_year + 1)
            months_iter_factory = lambda: months  # noqa: E731

        days_filter = self.config.days

        for year in years:
            for month in months_iter_factory():
                last_day = calendar.monthrange(year, month)[1]
                if days_filter is None:
                    if reverse:
                        day_iter: Iterable[int] = range(last_day, 0, -1)
                    else:
                        day_iter = range(1, last_day + 1)
                else:
                    valid_days = [d for d in days_filter if d <= last_day]
                    day_iter = reversed(valid_days) if reverse else valid_days
                for day in day_iter:
                    yield year, month, day

    # --- Formatting helpers ------------------------------------------------
    def _format_custom(self, year: int, month: int, day: int) -> str:
        dt = date(year, month, day)
        formatted = dt.strftime(self.config.custom_pattern or "%Y%m%d")  # custom_pattern validated already
        return self._apply_affixes_and_case(formatted)

    def _format_spec(self, year: int, month: int, day: int) -> str:
        if self._spec_tokens is None:
            self._spec_tokens = parse_format_spec(self.config.format)
        parts: list[str] = []
        for token in self._spec_tokens:
            if token == "Y4":
                parts.append(f"{year:04d}")
            elif token == "Y2":
                parts.append(f"{year % 100:02d}")
            elif token == "M":
                parts.append(f"{month:02d}")
            elif token == "D":
                parts.append(f"{day:02d}")
            else:  # pragma: no cover - defensive programming
                raise DateGeneratorError(f"Unsupported format token: {token}")
        formatted = self.config.separator.join(parts)
        return self._apply_affixes_and_case(formatted)

    def _apply_affixes_and_case(self, text: str) -> str:
        if self.config.case == "lower":
            text = text.lower()
        elif self.config.case == "upper":
            text = text.upper()
        return f"{self.config.prefix}{text}{self.config.suffix}"


def generate_dates(**kwargs) -> list[str]:
    """Convenience wrapper returning generated date strings as a list."""

    return DateGenerator(**kwargs).generate_to_list()
