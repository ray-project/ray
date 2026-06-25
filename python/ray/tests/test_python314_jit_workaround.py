"""Tests for Python 3.14 JIT workaround.

The experimental Python JIT compiler on 3.14+ with the tail-call interpreter
causes SIGSEGV crashes in _Py_Executors_InvalidateDependency during module
import. Ray auto-disables JIT to prevent this.
"""

import os
import sys

import pytest


@pytest.mark.skipif(
    sys.version_info < (3, 14),
    reason="JIT workaround only applies to Python 3.14+",
)
class TestPython314JitWorkaround:
    """Tests that verify JIT is disabled on Python 3.14+."""

    def test_jit_disabled_after_ray_import(self):
        """Verify PYTHON_JIT=0 is set after Ray is imported on 3.14+."""
        import subprocess

        cmd = [
            sys.executable,
            "-c",
            "import os; import ray; print(os.environ.get('PYTHON_JIT', 'None'))",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        assert result.stdout.strip() == "0", (
            "Expected PYTHON_JIT=0 to be set by Ray on Python 3.14+, "
            f"but got PYTHON_JIT={result.stdout.strip()}"
        )

    def test_worker_inherits_jit_disabled(self):
        """Verify Ray worker processes have PYTHON_JIT=0 in their env."""
        import ray

        ray.init(num_cpus=1)
        try:

            @ray.remote
            def get_jit_env():
                return os.environ.get("PYTHON_JIT", "not_set")

            result = ray.get(get_jit_env.remote())
            assert (
                result == "0"
            ), f"Expected worker to have PYTHON_JIT=0, got '{result}'"
        finally:
            ray.shutdown()


@pytest.mark.skipif(
    sys.version_info >= (3, 14),
    reason="This test validates behavior on pre-3.14 Python",
)
class TestPrePython314NoWarning:
    """Tests that verify no JIT interference on older Python versions."""

    def test_jit_env_not_forced_on_old_python(self):
        """On Python < 3.14, Ray should NOT force PYTHON_JIT=0 via __init__.py."""
        import subprocess

        cmd = [
            sys.executable,
            "-c",
            "import os; import ray; print(os.environ.get('PYTHON_JIT', 'None'))",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        assert result.stdout.strip() == "None", (
            "Expected PYTHON_JIT to not be set by Ray on Python < 3.14, "
            f"but got PYTHON_JIT={result.stdout.strip()}"
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
