import os
import tempfile
import unittest
from pathlib import Path

from chronogen.core import DateGenerator, DateGeneratorConfig


class TestDateGeneratorWrite(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_file = Path(self.test_dir) / "test_output.txt"

    def tearDown(self):
        for root, dirs, files in os.walk(self.test_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.test_dir)

    def test_write_batch_small_dataset(self):
        config = DateGeneratorConfig(
            start_year=2023,
            end_year=2023,
            months=[1],
            days=[1, 2],
            format="YYYYMMDD",
        )
        generator = DateGenerator(config)
        result_path = generator.write(self.test_file)

        assert result_path.exists()

        with open(result_path, encoding="utf-8") as f:
            content = f.read()
            assert content == "20230101\n20230102\n"

    def test_write_chunked_large_dataset(self):
        config = DateGeneratorConfig(
            start_year=2023,
            end_year=2023,
            months=[1],
            days=list(range(1, 32)),
            format="YYYYMMDD",
        )
        generator = DateGenerator(config)
        result_path = generator.write(self.test_file, chunk_size=10)

        assert result_path.exists()

        with open(result_path, encoding="utf-8") as f:
            lines = f.readlines()
            assert len(lines) == 31

    def test_write_empty_dataset(self):
        config = DateGeneratorConfig(
            start_year=2023,
            end_year=2023,
            months=[1],
            days=[],
            format="YYYYMMDD",
        )
        generator = DateGenerator(config)
        result_path = generator.write(self.test_file)

        assert result_path.exists()
        with open(result_path, encoding="utf-8") as f:
            content = f.read()
            assert content == ""

    def test_write_custom_pattern(self):
        config = DateGeneratorConfig(
            start_year=2023,
            end_year=2023,
            months=[1],
            days=[1],
            custom_pattern="%d/%m/%Y",
        )
        generator = DateGenerator(config)
        result_path = generator.write(self.test_file)

        assert result_path.exists()
        with open(result_path, encoding="utf-8") as f:
            content = f.read()
            assert content == "01/01/2023\n"
