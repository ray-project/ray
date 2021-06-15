import sys
import unittest
import pathlib
import importlib.util


class TestSoftImports(unittest.TestCase):
    """Tests whether it's possible to use Ray Tune without soft dependencies
    """

    def _testSoftImports(self, folder):
        searcher_modules = pathlib.Path(__file__).parents[1].joinpath(
            folder).glob("*.py")
        for module_path in searcher_modules:
            module_name = f"ray.tune.{folder}.{module_path.name}"
            print(f"loading {module_name} in {module_path}")
            try:
                spec = importlib.util.spec_from_file_location(
                    module_name, module_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
            except ImportError as e:
                raise e

    def testSoftImports(self):
        self._testSoftImports("suggest")
        self._testSoftImports("schedulers")


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
