# Library Usage

The Python API is built around the :class:`chronogen.DateGenerator` class and a small configuration dataclass. This section covers common scenarios and advanced options.

## Generating values lazily

```python
from chronogen import DateGenerator

generator = DateGenerator(start_year=2020, end_year=2021, format="YYYYMMDD", separator="-")
for value in generator.generate():
    print(value)
```

Iteration is lazy, making it safe to stream large ranges without exhausting memory.

## Working with configuration objects

`DateGenerator` accepts either keyword arguments or a :class:`chronogen.DateGeneratorConfig` instance. The latter is helpful when building reusable format templates:

```python
from chronogen import DateGenerator, DateGeneratorConfig

config = DateGeneratorConfig(
    start_year=1980,
    end_year=1990,
    format="DDMMYY",
    separator="/",
    prefix="corp-",
    suffix="!",
    case="lower",
)

generator = DateGenerator(config)
results = generator.generate_to_list()
```

`generate_to_list` is a convenience helper that materialises all values.

## Writing wordlists

Use the `write` method to save outputs to disk:

```python
from pathlib import Path
from chronogen import DateGenerator

# Basic usage
DateGenerator(
    start_year=2000,
    end_year=2000,
    format="YYYYMMDD"
).write(Path("wordlists/2000.txt"))

# With optional chunk_size for large datasets
DateGenerator(
    start_year=2000,
    end_year=2020,
    format="YYYYMMDD"
).write(Path("wordlists/2000-2020.txt"), chunk_size=10000)
```

Parameters:
- `destination` (`Path` or `str`): The path where the output file will be saved.
- `newline` (`str`, optional): The newline character to use (default: `"\n"`).
- `chunk_size` (`int`, optional): The number of lines to write at once. Use this for large datasets to optimize performance (default: `1000`).

The parent directory is created automatically if required.

## Custom strftime patterns

To unlock month names or other locale-aware tokens, provide `custom_pattern`:

```python
from chronogen import generate_dates

dates = generate_dates(
    start_year=2023,
    end_year=2023,
    custom_pattern="%d%b%Y",
    case="lower",
)
print(dates[:3])  # ['01jan2023', '02jan2023', '03jan2023']
```

When `custom_pattern` is set, the `format` and `separator` options are ignored.

## Filtering by month or day

Restrict the generator to specific months or days. Values exceeding the number of days in the month are discarded automatically, so you can safely include 31 even when February is present.

```python
from chronogen import generate_dates

dates = generate_dates(
    start_year=2022,
    end_year=2022,
    months=[1, 12],
    days=[1, 15, 31],
)
```

## Handling invalid input

Most validation errors raise :class:`chronogen.core.DateGeneratorError`. Catch this exception to provide user-friendly feedback in downstream tools.
