import sys
import tempfile
from pathlib import Path

import pytest

from ci.raydepsets.parser import parse_lock_file
from ci.raydepsets.tests.utils import (
    _REPO_NAME,
    _runfiles,
    copy_data_to_tmpdir,
    replace_in_file,
)


def test_parser():
    lock_file_path = _runfiles.Rlocation(
        f"{_REPO_NAME}/ci/raydepsets/tests/test_data/test_python_depset.lock"
    )
    parsed_deps = parse_lock_file(lock_file_path)
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
            "frozenlist==1.8.0 \\\n",
            'frozenlist==1.8.0 ; python_version >= "3.11" \\\n',
        )
        parsed_deps = parse_lock_file(Path(tmpdir) / "test_python_depset.lock")
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
        filepath = Path(tmpdir) / "empty.lock"
        with open(filepath, "w") as f:
            f.write("")
        parsed_deps = parse_lock_file(filepath)
        assert len(parsed_deps) == 0


def test_parser_package_with_dots():
    with tempfile.TemporaryDirectory() as tmpdir:
        lock_file = Path(tmpdir) / "test.lock"
        with open(lock_file, "w") as f:
            f.write("zope.interface==5.4.0\n")
        parsed_deps = parse_lock_file(lock_file)
        assert len(parsed_deps) == 1
        assert parsed_deps[0].name == "zope.interface"
        assert parsed_deps[0].version == "5.4.0"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
