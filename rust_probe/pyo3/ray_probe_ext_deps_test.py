"""Import the PyO3 extension through a normal py_test deps edge (no sys.path tricks)."""

import unittest

from rust_probe.pyo3 import ray_probe_ext


class RayProbeExtDepsTest(unittest.TestCase):
    def test_sum_as_string(self) -> None:
        self.assertEqual("1379", ray_probe_ext.sum_as_string(1337, 42))


if __name__ == "__main__":
    unittest.main()
