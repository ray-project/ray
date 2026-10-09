#!/usr/bin/env python3
"""Tests for build_common.py"""

import tempfile
import unittest
from pathlib import Path

from ci.build.build_common import BuildError, parse_file


class TestParseFile(unittest.TestCase):
    """Test parse_file() extracts values from config files."""

    def test_parses_quoted_value(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            self.addCleanup(Path(f.name).unlink, missing_ok=True)
            f.write('MANYLINUX_VERSION="260128.221a193"\n')
            f.flush()
            result = parse_file(Path(f.name), r'MANYLINUX_VERSION=["\']?([^"\'\s]+)')
        self.assertEqual(result, "260128.221a193")

    def test_missing_file_raises(self):
        with self.assertRaises(BuildError) as ctx:
            parse_file(Path("/nonexistent"), r".*")
        self.assertIn("Missing", str(ctx.exception))

    def test_missing_pattern_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            self.addCleanup(Path(f.name).unlink, missing_ok=True)
            f.write("OTHER_VAR=value\n")
            f.flush()
            with self.assertRaises(BuildError) as ctx:
                parse_file(Path(f.name), r"MISSING_VAR=(\w+)")
        self.assertIn("not found in", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
