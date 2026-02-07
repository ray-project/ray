import sys
import tempfile
from pathlib import Path

import pytest

from ci.raydepsets.parser import parse_lock_file, write_lock_file


def test_parse_lock_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        lock_file = Path(tmpdir) / "requirements.txt"
        lock_file.write_text(
            "emoji==2.9.0 \\\n"
            "    --hash=sha256:abc123\n"
            "pyperclip==1.6.0 \\\n"
            "    --hash=sha256:def456\n"
        )
        reqs = parse_lock_file(str(lock_file))
        names = [req.name for req in reqs]
        assert "emoji" in names
        assert "pyperclip" in names
        assert len(reqs) == 2


def test_parse_lock_file_with_index_url():
    with tempfile.TemporaryDirectory() as tmpdir:
        lock_file = Path(tmpdir) / "requirements.txt"
        lock_file.write_text(
            "--index-url https://pypi.org/simple\n"
            "\n"
            "emoji==2.9.0 \\\n"
            "    --hash=sha256:abc123\n"
        )
        reqs = parse_lock_file(str(lock_file))
        assert len(reqs) == 1
        assert reqs[0].name == "emoji"


def test_parse_lock_file_empty():
    with tempfile.TemporaryDirectory() as tmpdir:
        lock_file = Path(tmpdir) / "requirements.txt"
        lock_file.write_text("")
        reqs = parse_lock_file(str(lock_file))
        assert len(reqs) == 0


def test_parse_lock_file_comments_only():
    with tempfile.TemporaryDirectory() as tmpdir:
        lock_file = Path(tmpdir) / "requirements.txt"
        lock_file.write_text("# This is a comment\n" "# Another comment\n")
        reqs = parse_lock_file(str(lock_file))
        assert len(reqs) == 0


def test_write_requirements_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        lock_file = Path(tmpdir) / "requirements.txt"
        lock_file.write_text(
            "emoji==2.9.0 \\\n"
            "    --hash=sha256:abc123\n"
            "pyperclip==1.6.0 \\\n"
            "    --hash=sha256:def456\n"
        )
        reqs = parse_lock_file(str(lock_file))

        output_file = Path(tmpdir) / "output.txt"
        write_lock_file(reqs, str(output_file))

        output_text = output_file.read_text()
        assert "emoji==2.9.0" in output_text
        assert "pyperclip==1.6.0" in output_text


def test_write_requirements_file_empty():
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = Path(tmpdir) / "output.txt"
        write_lock_file([], str(output_file))
        assert output_file.read_text() == ""


def test_roundtrip_preserves_packages():
    with tempfile.TemporaryDirectory() as tmpdir:
        lock_file = Path(tmpdir) / "requirements.txt"
        lock_file.write_text(
            "emoji==2.9.0 \\\n"
            "    --hash=sha256:abc123\n"
            "pyperclip==1.6.0 \\\n"
            "    --hash=sha256:def456\n"
        )
        reqs = parse_lock_file(str(lock_file))

        output_file = Path(tmpdir) / "output.txt"
        write_lock_file(reqs, str(output_file))

        reqs2 = parse_lock_file(str(output_file))
        assert len(reqs) == len(reqs2)
        assert [r.name for r in reqs] == [r.name for r in reqs2]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
