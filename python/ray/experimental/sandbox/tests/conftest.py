import os
import shutil
import stat
import tempfile
import urllib.request

import pytest

from ray._private.test_utils import sandbox_test_enabled


def pytest_runtest_setup(item):
    if not sandbox_test_enabled():
        pytest.skip("Sandbox tests are only run when TEST_SANDBOX=1")


@pytest.fixture(scope="session", autouse=True)
def ensure_runsc():
    if not sandbox_test_enabled():
        return

    if not shutil.which("runsc"):
        temp_bin = tempfile.mkdtemp()
        runsc_path = os.path.join(temp_bin, "runsc")
        url = (
            "https://storage.googleapis.com/gvisor/releases/release/latest/x86_64/runsc"
        )
        try:
            urllib.request.urlretrieve(url, runsc_path)
            os.chmod(runsc_path, os.stat(runsc_path).st_mode | stat.S_IEXEC)
            os.environ["PATH"] = f"{temp_bin}:{os.environ.get('PATH', '')}"
        except Exception as e:
            pytest.skip(f"Failed to install runsc for sandbox tests: {e}")
