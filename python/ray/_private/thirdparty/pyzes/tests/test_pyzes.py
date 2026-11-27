"""
Tests for the Linux loader search in the vendored pyzes.

Ray's copy differs from upstream pyzes 0.1.3 only in how it finds
libze_loader on Linux (oneapi-src/level-zero#496). These tests mock dlopen,
so they need no Intel GPU:
    python3 -m unittest discover \
        -s python/ray/_private/thirdparty/pyzes/tests \
        -p "test_pyzes.py" -v
"""

import importlib.util
import pathlib
import unittest
from unittest.mock import Mock, patch

_mod_path = pathlib.Path(__file__).parent.parent / "pyzes.py"

SONAME = "libze_loader.so.1"
SEARCH_ORDER = [
    SONAME,
    "/usr/lib/x86_64-linux-gnu/" + SONAME,
    "/usr/lib64/" + SONAME,
    "/usr/lib/" + SONAME,
]


def _import_pyzes(loadable):
    """Import a fresh copy of pyzes on Linux, where only ``loadable`` dlopens.

    Returns the module, the paths passed to dlopen in order, and the library
    handle returned for each path that loaded.
    """
    tried = []
    handles = {}

    def cdll(path, *args, **kwargs):
        tried.append(path)
        if path in loadable:
            return handles.setdefault(path, Mock())
        raise OSError(f"{path}: cannot open shared object file")

    spec = importlib.util.spec_from_file_location("pyzes", _mod_path)
    module = importlib.util.module_from_spec(spec)
    with patch("ctypes.CDLL", side_effect=cdll), patch("sys.platform", "linux"):
        spec.loader.exec_module(module)
    return module, tried, handles


class TestLinuxLoaderSearch(unittest.TestCase):
    def test_soname_is_tried_first(self):
        module, tried, handles = _import_pyzes({SONAME})

        self.assertEqual(tried, [SONAME])
        self.assertIs(module.gpuLib, handles[SONAME])

    def test_falls_back_to_lib64(self):
        """RHEL/SUSE layout: the loader is only in /usr/lib64."""
        module, tried, handles = _import_pyzes({"/usr/lib64/" + SONAME})

        self.assertEqual(tried, SEARCH_ORDER[:3])
        self.assertIs(module.gpuLib, handles["/usr/lib64/" + SONAME])

    def test_missing_loader_raises_oserror_listing_every_path(self):
        with self.assertRaises(OSError) as ctx:
            _import_pyzes(set())

        for path in SEARCH_ORDER:
            self.assertIn(path, str(ctx.exception))
        self.assertIsInstance(ctx.exception.__cause__, OSError)


if __name__ == "__main__":
    unittest.main()
