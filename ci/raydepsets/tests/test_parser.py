import sys
import tempfile
from pathlib import Path

import pytest

from ci.raydepsets.parser import Parser
from ci.raydepsets.tests.utils import (
    copy_data_to_tmpdir,
    replace_in_file,
    write_to_file,
)


def test_parser():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        parser = Parser(Path(tmpdir) / "test_python_depset.lock")
        parsed_deps = parser.parse()
        assert len(parsed_deps) == 11
        assert parsed_deps[0].name == "aiohappyeyeballs"
        assert parsed_deps[0].version == "2.6.1"
        assert parsed_deps[0].required_by == ["aiohttp"]
        assert parsed_deps[1].name == "aiohttp"
        assert parsed_deps[1].version == "3.11.16"
        assert parsed_deps[1].required_by == []


def test_parser_w_annotations():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        replace_in_file(
            Path(tmpdir) / "test_python_depset.lock",
            "frozenlist==1.8.0 \n",
            'frozenlist==1.8.0 ; python_version >= "3.11"\n',
        )
        parser = Parser(Path(tmpdir) / "test_python_depset.lock")
        parsed_deps = parser.parse()
        assert len(parsed_deps) == 11
        assert parsed_deps[0].name == "aiohappyeyeballs"
        assert parsed_deps[0].version == "2.6.1"
        assert parsed_deps[0].required_by == ["aiohttp"]
        assert parsed_deps[1].name == "aiohttp"
        assert parsed_deps[1].version == "3.11.16"
        assert parsed_deps[1].required_by == []
        assert parsed_deps[5].name == "frozenlist"
        assert parsed_deps[5].version == "1.8.0"


def test_parser_empty_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        write_to_file(Path(tmpdir) / "empty.lock", "")
        parser = Parser(Path(tmpdir) / "empty.lock")
        parsed_deps = parser.parse()
        assert len(parsed_deps) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
