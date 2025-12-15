import sys
import tempfile
from pathlib import Path

import pytest

from ci.raydepsets.parser import Parser
from ci.raydepsets.tests.utils import copy_data_to_tmpdir


def test_parser():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        parser = Parser(Path(tmpdir) / "test_python_depset.lock")
        parsed_deps = parser.parse()
        assert len(parsed_deps) == 10
        assert parsed_deps[0].name == "aiohappyeyeballs"
        assert parsed_deps[0].version == "2.6.1"
        assert parsed_deps[0].required_by == ["aiohttp"]
        assert parsed_deps[1].name == "aiohttp"
        assert parsed_deps[1].version == "3.11.16"
        assert parsed_deps[1].required_by == ["-r in.lock"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
