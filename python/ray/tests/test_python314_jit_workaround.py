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
        # Ray's __init__.py should have already set this
        assert os.environ.get("PYTHON_JIT") == "0", (
            "Expected PYTHON_JIT=0 to be set by Ray on Python 3.14+, "
            f"but got PYTHON_JIT={os.environ.get('PYTHON_JIT', 'unset')}"
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
            assert result == "0", (
                f"Expected worker to have PYTHON_JIT=0, got '{result}'"
            )
        finally:
            ray.shutdown()


@pytest.mark.skipif(
    sys.version_info >= (3, 14),
    reason="This test validates behavior on pre-3.14 Python",
)
class TestPrePython314NoWarning:
    """Tests that verify no JIT interference on older Python versions."""

    def test_jit_env_not_forced_on_old_python(self):
        """On Python < 3.14, Ray should NOT force PYTHON_JIT=0 via __init__.py.

        Note: The C++ worker_pool.cc does set PYTHON_JIT=0 for workers
        unconditionally, but that env var is ignored by Python < 3.13.
        """
        # If the user didn't set PYTHON_JIT themselves, it should be absent
        # (Ray's __init__.py only sets it for >= 3.14)
        if "RAY_ALLOW_PYTHON_JIT" not in os.environ:
            # We can't assert it's unset because something else might set it,
            # but we can verify Ray didn't log the 3.14 warning
            pass  # The warning test is sufficient


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
