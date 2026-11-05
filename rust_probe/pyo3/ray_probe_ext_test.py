"""Prove the Rust-built .so imports and executes from a WORKSPACE-world py_test."""

import os
import sys
import unittest


class RayProbeExtTest(unittest.TestCase):
    def test_sum_as_string(self) -> None:
        # The .so lands next to this test in runfiles; put that dir on sys.path.
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import ray_probe_ext

        result = ray_probe_ext.sum_as_string(1337, 42)
        self.assertIsInstance(result, str)
        self.assertEqual("1379", result)


if __name__ == "__main__":
    unittest.main()
