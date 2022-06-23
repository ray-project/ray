import sys
import unittest


class TestSoftImports(unittest.TestCase):
    """Tests whether it's possible to use Ray Tune without soft dependencies"""

    def testSoftImports(self):
        import ray.tune.schedulers  # noqa: F401
        from ray.tune.suggest import SEARCH_ALG_IMPORT

        for name, import_func in SEARCH_ALG_IMPORT.items():
            print(f"testing searcher {name}")
            searcher = import_func()

            # ensure that the dependencies aren't actually installed
            if searcher and name not in ("variant_generator", "random"):
                with self.assertRaises((AssertionError, ImportError)):
                    searcher()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
