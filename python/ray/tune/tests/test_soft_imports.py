import sys
import unittest


class TestSoftImports(unittest.TestCase):
    """Tests whether it's possible to use Ray Tune without soft dependencies
    """

    def testSoftImports(self):
        import ray.tune.schedulers  # noqa: F401
        from ray.tune.suggest import SEARCH_ALG_IMPORT
        for import_func in SEARCH_ALG_IMPORT.values():
            import_func()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
