import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from ray._private.test_utils import sandbox_test_enabled


def pytest_runtest_setup(item):
    if not sandbox_test_enabled():
        pytest.skip("Sandbox tests are only run when TEST_SANDBOX=1")
    os.environ["RAY_SANDBOX_IGNORE_CGROUPS"] = "1"


@pytest.fixture(scope="session", autouse=True)
def ensure_runsc():
    if not sandbox_test_enabled():
        return

    os.environ["RAY_SANDBOX_IGNORE_CGROUPS"] = "1"

    if not shutil.which("runsc"):
        temp_bin = tempfile.mkdtemp()
        script = Path(__file__).resolve().parents[5] / "ci" / "env" / "install-runsc.sh"
        try:
            subprocess.check_call(["bash", str(script), temp_bin])
            os.environ["PATH"] = f"{temp_bin}:{os.environ.get('PATH', '')}"
        except Exception as e:
            pytest.skip(f"Failed to install runsc for sandbox tests: {e}")
