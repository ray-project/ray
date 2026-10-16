"""Unit tests for go/check_pkghash.py (D4).

The tests depend on a real `raygo` binary (data dependency, cgo-free after the
thin-layer rework) and use the readelf procedure documented in CLAUDE.md.
"""

import os

import pytest

import go.check_pkghash as c


def _find_raygo():
    root = os.environ.get("TEST_SRCDIR", "")
    if not root:
        pytest.skip("TEST_SRCDIR not set (not running under bazel)")
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f == "raygo":
                return os.path.join(dirpath, f)
    pytest.skip("raygo data dependency not found in runfiles")


def test_package_hash_extracts_symbol():
    raygo = _find_raygo()
    value, offset = c.package_hash(raygo)
    assert isinstance(value, str) and len(value) == 16, value
    assert offset >= 0


def test_verify_binaries_positive():
    raygo = _find_raygo()
    c.verify_binaries([raygo, raygo])  # same binary -> same hash, no raise


def test_verify_binaries_negative():
    raygo = _find_raygo()
    import shutil

    tmp = os.path.join(os.environ["TEST_TMPDIR"], "raygo_corrupt")
    shutil.copyfile(raygo, tmp)
    _, offset = c.package_hash(tmp)
    with open(tmp, "r+b") as f:
        f.seek(offset)
        b = f.read(1)
        f.seek(offset)
        f.write(bytes([b[0] ^ 0xFF]))
    with pytest.raises(RuntimeError, match="mismatch"):
        c.verify_binaries([raygo, tmp])


_SECTION_OUT = """\
  [Nr] Name              Type            Address          Off    Size   ES Flg Lk Inf Al
  [ 0]                   NULL            0000000000000000 000000 000000 00      0   0  0
  [ 1] .interp           PROGBITS        0000000000400270 000270 00001c 00   A  0   0  1
  [ 9] .gnu.version      VERSYM          0000000000c725d8 8725d8 03a456 02   A  5   0  2
  [10] .gnu.version_r    VERNEED         0000000000caca30 8aca30 0000a0 00   A  6   4  4
  [17] .rodata           PROGBITS        0000000001cb9900 18b9900 752b80 00   A  0   0 32
"""


def test_parse_sections_single_and_double_digit():
    # Single-digit sections ("[ 1]") must not be dropped, and the NULL
    # section (empty name) must be skipped rather than mis-parsed.
    sections = c._parse_sections(_SECTION_OUT)
    names = [s[0] for s in sections]
    assert ".interp" in names
    assert ".gnu.version" in names
    assert ".gnu.version_r" in names
    assert ".rodata" in names
    assert "0]" not in names
    assert "NULL" not in names


def test_parse_sections_hex_offsets():
    sections = c._parse_sections(_SECTION_OUT)
    rodata = [s for s in sections if s[0] == ".rodata"][0]
    assert rodata[1] == 0x1CB9900  # address
    assert rodata[2] == 0x18B9900  # file offset
    assert rodata[3] == 0x752B80  # size
