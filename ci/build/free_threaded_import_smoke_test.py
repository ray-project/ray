import sys
import sysconfig

import pytest
from free_threaded_import_smoke import check_import, find_gil_enabling_module

# Verbatim from CPython 3.15t when importing an extension that does not declare
# free-threading support.
GIL_WARNING = (
    "<frozen importlib._bootstrap>:549: RuntimeWarning: The global interpreter lock "
    "(GIL) has been enabled to load module 'ray._raylet', which has not declared "
    "that it can run safely without the GIL. To override this behavior and keep the "
    "GIL disabled (at your own risk), run with PYTHON_GIL=0 or -Xgil=0.\n"
)


@pytest.mark.parametrize(
    "stderr, expected",
    [(GIL_WARNING, "ray._raylet"), ("some other warning\n", None), ("", None)],
)
def test_find_gil_enabling_module(stderr, expected):
    assert find_gil_enabling_module(stderr) == expected


def test_reports_gil_state_of_a_successful_import():
    result = check_import(sys.executable, "json")
    assert result.returncode == 0
    # A pure-Python stdlib module never re-enables the GIL on a free-threaded build.
    assert result.gil_enabled == (not sysconfig.get_config_var("Py_GIL_DISABLED"))
    assert result.gil_enabling_module is None


def test_reports_failed_import():
    result = check_import(sys.executable, "no_such_module_for_smoke_test")
    assert result.returncode != 0
    assert result.gil_enabled is None
    assert "no_such_module_for_smoke_test" in result.stderr


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
