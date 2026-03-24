from pathlib import Path

import pytest

from chronogen.core import DateGenerator, DateGeneratorConfig, DateGeneratorError


def test_write_batch_small_dataset(tmp_path: Path):
    config = DateGeneratorConfig(
        start_year=2023,
        end_year=2023,
        months=[1],
        days=[1, 2],
        format="YYYYMMDD",
    )
    generator = DateGenerator(config)
    result_path = generator.write(tmp_path / "test_output.txt")

    assert result_path.exists()
    assert result_path.read_text(encoding="utf-8") == "20230101\n20230102\n"


def test_write_chunked_large_dataset(tmp_path: Path):
    config = DateGeneratorConfig(
        start_year=2023,
        end_year=2023,
        months=[1],
        days=list(range(1, 32)),
        format="YYYYMMDD",
    )
    generator = DateGenerator(config)
    result_path = generator.write(tmp_path / "test_output.txt", chunk_size=10)

    assert result_path.exists()
    assert len(result_path.read_text(encoding="utf-8").splitlines()) == 31


def test_write_empty_dataset(tmp_path: Path):
    config = DateGeneratorConfig(
        start_year=2023,
        end_year=2023,
        months=[1],
        days=[],
        format="YYYYMMDD",
    )
    generator = DateGenerator(config)
    result_path = generator.write(tmp_path / "test_output.txt")

    assert result_path.exists()
    assert result_path.read_text(encoding="utf-8") == ""


def test_write_custom_pattern(tmp_path: Path):
    config = DateGeneratorConfig(
        start_year=2023,
        end_year=2023,
        months=[1],
        days=[1],
        custom_pattern="%d/%m/%Y",
    )
    generator = DateGenerator(config)
    result_path = generator.write(tmp_path / "test_output.txt")

    assert result_path.exists()
    assert result_path.read_text(encoding="utf-8") == "01/01/2023\n"


@pytest.mark.parametrize("chunk_size", [0, -1, 1.5, True])
def test_write_rejects_invalid_chunk_size(tmp_path: Path, chunk_size: object):
    generator = DateGenerator(start_year=2023, end_year=2023, format="YYYYMMDD", months=[1], days=[1])

    with pytest.raises(DateGeneratorError, match="chunk_size must be an integer greater than or equal to 1"):
        generator.write(tmp_path / "test_output.txt", chunk_size=chunk_size)  # type: ignore[arg-type]


def test_write_preserves_exact_crlf_newline(tmp_path: Path):
    config = DateGeneratorConfig(
        start_year=2023,
        end_year=2023,
        months=[1],
        days=[1, 2],
        format="YYYYMMDD",
    )
    generator = DateGenerator(config)
    result_path = generator.write(tmp_path / "test_output.txt", newline="\r\n")

    assert result_path.read_bytes() == b"20230101\r\n20230102\r\n"
