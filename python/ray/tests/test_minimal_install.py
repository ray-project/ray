# coding: utf-8
"""
Tests that are specific to minimal installations.
"""

import itertools
import os
import pathlib
import subprocess
import sys
import textwrap
import unittest.mock as mock
from typing import Dict

import packaging
import pytest


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL", "0") != "1",
    reason="Skip unless running in a minimal install.",
)
def test_correct_python_version():
    """
    Validate that Bazel uses the correct Python version in our minimal tests.
    """
    expected_python_version = os.environ.get("EXPECTED_PYTHON_VERSION", "").strip()
    assert (
        expected_python_version
    ), f"EXPECTED_PYTHON_VERSION is {expected_python_version}"

    actual_major, actual_minor = sys.version_info[:2]
    actual_version = f"{actual_major}.{actual_minor}"
    assert actual_version == expected_python_version, (
        f"expected_python_version={expected_python_version}"
        f"actual_version={actual_version}"
    )


class MockBaseModel:
    model_fields = {}

    def __init__(self, *args, **kwargs):
        pass

    def __init_subclass__(self, *args, **kwargs):
        pass


def _make_mock_pydantic_modules(pydantic_version: str) -> Dict:
    """Make a mock for the `pydantic` module.

    This module requires special handling to:
        - Make `BaseModel` a class object so type hints work.
        - Set the `__version__` attribute appropriately.
        - Also mock `pydantic.dataclasses`.

    Returns a dict of mocked modules.
    """
    mock_modules = {
        "pydantic": mock.MagicMock(),
        "pydantic.dataclasses": mock.MagicMock(),
    }
    mock_modules["pydantic"].BaseModel = MockBaseModel
    if packaging.version.parse(pydantic_version) >= packaging.version.parse("1.9.0"):
        mock_modules["pydantic"].__version__ = pydantic_version

    return mock_modules


@pytest.mark.parametrize("pydantic_version", ["2.0.0"])
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL", "0") != "1",
    reason="Skip unless running in a minimal install.",
)
def test_module_import_with_various_non_minimal_deps(pydantic_version: str):
    optional_modules = [
        "opencensus",
        "prometheus_client",
        "aiohttp",
        "aiohttp_cors",
        "pydantic",
        "grpc",
    ]
    for i in range(len(optional_modules)):
        for install_modules in itertools.combinations(optional_modules, i):
            print(install_modules)
            mock_modules = {}
            for mod in install_modules:
                if mod == "pydantic":
                    mock_modules.update(**_make_mock_pydantic_modules(pydantic_version))
                else:
                    mock_modules[mod] = mock.MagicMock()

            with mock.patch.dict("sys.modules", mock_modules):
                from ray.dashboard.utils import DashboardHeadModule, get_all_modules

                get_all_modules(DashboardHeadModule)


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL", "0") != "1",
    reason="Skip unless running in a minimal install.",
)
def test_pydantic_v1_is_unsupported():
    mock_modules = _make_mock_pydantic_modules("1.9.0")

    with mock.patch.dict("sys.modules", mock_modules):
        from ray.util import serialization_addons

        with pytest.raises(ImportError, match="Pydantic v1 is no longer supported"):
            serialization_addons.apply(mock.MagicMock())


def _run_without_pydantic(code: str):
    """Run `code` in a new interpreter where importing pydantic fails.

    This works whether or not pydantic is installed, because a `None` entry in
    `sys.modules` makes `import pydantic` raise `ModuleNotFoundError`.
    """
    script = "import sys\nsys.modules['pydantic'] = None\n" + textwrap.dedent(code)
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_state_and_serialization_work_without_pydantic():
    _run_without_pydantic(
        """
        import dataclasses
        from unittest import mock

        from ray.dashboard.modules.job.pydantic_models import (
            DriverInfo,
            JobDetails,
            JobType,
        )
        from ray.dashboard.modules.reporter.reporter_models import StatsPayload
        from ray.util import serialization_addons
        from ray.util.state.common import GetLogOptions, JobState, ListApiOptions

        assert (DriverInfo, JobDetails, JobType, StatsPayload) == (None,) * 4

        # The state dataclasses fall back to the standard library, which has to
        # generate __init__ and call __post_init__.
        assert dataclasses.is_dataclass(ListApiOptions)
        assert not hasattr(ListApiOptions, "__pydantic_fields__")
        assert ListApiOptions(limit=3, timeout=10).limit == 3
        # __post_init__ scales the timeout down.
        assert ListApiOptions(limit=3, timeout=10).timeout == 8
        assert GetLogOptions(timeout=1, node_id="a", filename="f").lines == 1000
        assert JobState.list_columns() == []

        context = mock.MagicMock()
        serialization_addons.apply(context)
        for call in context._register_cloudpickle_serializer.call_args_list:
            assert not call.args[0].__module__.startswith("pydantic")
        """
    )


def test_dashboard_modules_load_without_pydantic():
    """`get_all_modules` only tolerates `ModuleNotFoundError` from a module.

    A module that needs pydantic must raise it, whichever optional dependencies are
    installed, or the dashboard fails to start.
    """
    _run_without_pydantic(
        """
        from ray.dashboard.utils import DashboardHeadModule, get_all_modules

        modules = {m.__name__ for m in get_all_modules(DashboardHeadModule)}
        assert "UsageStatsHead" in modules, modules
        assert "JobHead" not in modules, modules
        """
    )


@pytest.mark.parametrize(
    "module",
    [
        "ray.dashboard.modules.job.job_head",
        "ray.dashboard.modules.log.log_manager",
    ],
)
def test_modules_requiring_pydantic_raise_module_not_found(module: str):
    _run_without_pydantic(
        f"""
        import importlib

        try:
            importlib.import_module("{module}")
        except ModuleNotFoundError:
            pass
        else:
            raise RuntimeError("{module} imported without pydantic")
        """
    )


def test_pydantic_compat_is_removed():
    import ray

    package_root = pathlib.Path(ray.__file__).parent
    assert not (package_root / "_common" / "pydantic_compat.py").exists()

    references = [
        str(path.relative_to(package_root))
        for path in package_root.rglob("*.py")
        if path.name != "test_minimal_install.py"
        and "pydantic_compat" in path.read_text(encoding="utf-8", errors="ignore")
    ]
    assert references == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
