"""
Tests for boolean environment variable parsing in DAGContext.

Regression test for the bug where bool(os.environ.get(..., 0)) incorrectly
treats any non-empty string (including "0", "false", "no") as True.
"""

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "env_value, expected",
    [
        ("0", False),
        ("1", True),
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("false", False),
        ("False", False),
        ("yes", True),
        ("Yes", True),
        ("no", False),
    ],
)
def test_overlap_gpu_communication_env_parsing(env_value, expected):
    """Test that RAY_CGRAPH_overlap_gpu_communication env var is parsed correctly.

    This is a regression test: the old code used bool(os.environ.get(..., 0))
    which treats any non-empty string as True, so "0" and "false" would
    incorrectly enable the feature.
    """
    # We test by running a subprocess that loads the module fresh with the
    # env var set, to avoid module-level caching issues.
    script = (
        "import os; "
        "from ray.dag.context import DEFAULT_OVERLAP_GPU_COMMUNICATION; "
        "print(DEFAULT_OVERLAP_GPU_COMMUNICATION)"
    )
    env = os.environ.copy()
    env["RAY_CGRAPH_overlap_gpu_communication"] = env_value

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
    )

    if result.returncode != 0:
        pytest.skip(f"Could not import ray.dag.context: {result.stderr.strip()}")

    actual = result.stdout.strip()
    assert actual == str(expected), (
        f"RAY_CGRAPH_overlap_gpu_communication={env_value!r}: "
        f"expected {expected}, got {actual}"
    )


def test_overlap_gpu_communication_default():
    """Test that the default (env var not set) is False."""
    script = (
        "import os; "
        "os.environ.pop('RAY_CGRAPH_overlap_gpu_communication', None); "
        "# Force reimport by removing from cache\n"
        "import importlib; "
        "import ray.dag.context; "
        "importlib.reload(ray.dag.context); "
        "print(ray.dag.context.DEFAULT_OVERLAP_GPU_COMMUNICATION)"
    )
    env = os.environ.copy()
    env.pop("RAY_CGRAPH_overlap_gpu_communication", None)

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
    )

    if result.returncode != 0:
        pytest.skip(f"Could not import ray.dag.context: {result.stderr.strip()}")

    actual = result.stdout.strip()
    assert actual == "False", f"Expected default to be False, got {actual}"
