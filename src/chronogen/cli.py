"""Command line interface for the Chronogen package."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from textwrap import dedent
from typing import Sequence

import chronogen

from .core import DateGenerator, DateGeneratorConfig, DateGeneratorError, parse_format_spec


class FriendlyFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    """Help formatter that preserves line breaks and shows readable defaults."""

    def _get_help_string(self, action: argparse.Action) -> str | None:  # type: ignore[override]
        help_text = action.help or ""
        if "%(default)" in help_text:
            return help_text
        default = action.default
        if default in (None, argparse.SUPPRESS):
            return help_text
        defaulting_nargs = [argparse.OPTIONAL, argparse.ZERO_OR_MORE]
        if not action.option_strings and action.nargs not in defaulting_nargs:
            return help_text
        if isinstance(default, str):
            if not default:
                default_display = "''"
            else:
                escaped = default.encode("unicode_escape").decode("ascii")
                default_display = f"'{escaped}'"
        else:
            default_display = default
        suffix = f" (default: {default_display})"
        return f"{help_text}{suffix}" if help_text else suffix


def parse_newline(value: str) -> str:
    """Normalise newline choices for CLI usage."""

    normalized = value.strip().lower()
    mapping = {
        "lf": "\n",
        "crlf": "\r\n",
        "\\n": "\n",
        "\\r\\n": "\r\n",
    }
    if value in ("\n", "\r\n"):
        return value
    if normalized in mapping:
        return mapping[normalized]
    if value in mapping:
        return mapping[value]
    raise argparse.ArgumentTypeError("newline must be one of 'lf', 'crlf', '\\n', or '\\r\\n'")


class PositiveIntAction(argparse.Action):
    """Argparse action that converts arguments to positive integers."""

    def __init__(
        self,
        option_strings: Sequence[str],
        dest: str,
        nargs: str | None = None,
        **kwargs,
    ) -> None:
        if nargs is None:
            nargs = "+"
        super().__init__(option_strings, dest, nargs=nargs, **kwargs)

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Sequence[str] | str,
        option_string: str | None = None,
    ) -> None:
        if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
            converted = [self._convert(value, parser, option_string) for value in values]
        else:
            converted = self._convert(values, parser, option_string)
        setattr(namespace, self.dest, converted)

    def _convert(self, value: str, parser: argparse.ArgumentParser, option_string: str | None) -> int:
        try:
            value_int = int(value)
        except (TypeError, ValueError):
            parser.error(f"{option_string or ''} expects integer values")
        if value_int <= 0:
            parser.error(f"{option_string or ''} values must be positive integers")
        return value_int


FORMAT_SAMPLES: dict[str, str] = {
    "YYYYMMDD": "Year-Month-Day (default ISO style)",
    "YYMMDD": "Short year, month, day",
    "YYYYMM": "Year-Month",
    "DDMMYYYY": "Day-Month-Year",
    "DDMMYY": "Day-Month-Short year",
    "MMDDYYYY": "Month-Day-Year",
    "MMDD": "Month-Day",
    "MM": "Month",
    "DD": "Day",
}


def parse_format(value: str) -> str:
    try:
        parse_format_spec(value)
    except DateGeneratorError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return value.upper()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chronogen",
        description=dedent(
            """
            Generate date-based wordlists for pentesting and automation.
            Provide --start/--end to define the year range or list formats to explore suggested templates.
            """
        ),
        epilog=dedent(
            """
            Example usage:
              chronogen --start 2000 --end 2001 --format YYYYMMDD
              chronogen --list-formats
            """
        ),
        formatter_class=FriendlyFormatter,
    )
    parser.add_argument("-s", "--start", type=int, help="Starting year (inclusive)")
    parser.add_argument("-e", "--end", type=int, help="Ending year (inclusive)")
    parser.add_argument(
        "-f",
        "--format",
        default="YYYYMMDD",
        type=parse_format,
        help="Format template using contiguous Y, M, and D groups (e.g. 'YYYYMMDD', 'YYMMDD', 'DDMMYYYY')",
    )
    parser.add_argument(
        "-S",
        "--separator",
        default="",
        help="String inserted between date parts (ignored for custom patterns)",
    )
    parser.add_argument(
        "-p",
        "--pattern",
        dest="custom_pattern",
        help="Custom strftime pattern overriding format template (e.g. '%%d%%b%%Y')",
    )
    parser.add_argument(
        "-m",
        "--months",
        nargs="+",
        action=PositiveIntAction,
        metavar="MONTH",
        help="Restrict generation to specific months (1-12)",
    )
    parser.add_argument(
        "-d",
        "--days",
        nargs="+",
        action=PositiveIntAction,
        metavar="DAY",
        help="Restrict generation to specific days of the month (1-31)",
    )
    parser.add_argument("-P", "--prefix", default="", help="Prefix to prepend to every value")
    parser.add_argument("-X", "--suffix", default="", help="Suffix appended to every value")
    parser.add_argument(
        "-c",
        "--case",
        choices=["lower", "upper"],
        help="Apply lower or upper casing to the generated values",
    )
    parser.add_argument(
        "-r",
        "--reverse",
        action="store_true",
        help="Generate dates in reverse chronological order",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Optional file path where the values will be written",
    )
    parser.add_argument(
        "-n",
        "--newline",
        default="\n",
        type=parse_newline,
        metavar="STYLE",
        help="Line ending when writing to a file (accepts 'lf', 'crlf', '\\n', or '\\r\\n')",
    )
    parser.add_argument(
        "-l",
        "--list-formats",
        action="store_true",
        help="Show suggested format templates and exit",
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"chronogen {chronogen.__version__}"
    )
    return parser


def run_from_args(args: Sequence[str] | None = None) -> int:
    parser = build_parser()
    namespace = parser.parse_args(args=args)
    if namespace.list_formats:
        for key, description in FORMAT_SAMPLES.items():
            print(f"{key}: {description}")
        return 0
    if namespace.start is None or namespace.end is None:
        parser.error("--start and --end are required unless --list-formats is provided")
    config = DateGeneratorConfig(
        start_year=namespace.start,
        end_year=namespace.end,
        format=namespace.format,
        separator=namespace.separator,
        custom_pattern=namespace.custom_pattern,
        months=namespace.months,
        days=namespace.days,
        prefix=namespace.prefix,
        suffix=namespace.suffix,
        case=namespace.case,
        reverse=namespace.reverse,
    )
    generator = DateGenerator(config)

    if namespace.output:
        generator.write(namespace.output, newline=namespace.newline)
    else:
        for value in generator.generate():
            print(value)
    return 0


def main() -> int:
    try:
        return run_from_args()
    except DateGeneratorError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
