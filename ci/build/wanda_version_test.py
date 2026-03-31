"""Verify the wanda binary version in bazel/wanda.bzl matches .rayciversion."""

import re
import sys
from pathlib import Path

import runfiles


def test_wanda_bzl_version_matches_rayciversion():
    """Ensure bazel/wanda.bzl downloads wanda at the version pinned in .rayciversion."""
    r = runfiles.Create()
    expected = Path(r.Rlocation("io_ray/.rayciversion")).read_text().strip()
    wanda_bzl = Path(r.Rlocation("io_ray/bazel/wanda.bzl")).read_text()

    match = re.search(r'WANDA_VERSION\s*=\s*"([^"]+)"', wanda_bzl)
    assert match, "WANDA_VERSION not found in bazel/wanda.bzl"
    actual = match.group(1)
    assert actual == expected, (
        f"bazel/wanda.bzl has WANDA_VERSION = {actual!r} but "
        f".rayciversion specifies {expected!r}"
    )


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main([__file__, "-v"]))
