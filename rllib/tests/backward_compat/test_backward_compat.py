import unittest


class TestBackwardCompatibility(unittest.TestCase):
    # Leaving this class in-tact as we will add new backward-compat tests in
    # an upcoming PR.
    pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
