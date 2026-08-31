import os
import platform
import shutil
import tempfile
import urllib.request

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
        os.chmod(temp_bin, 0o755)
        runsc_path = os.path.join(temp_bin, "runsc")
        arch = (
            "aarch64"
            if platform.machine().lower() in ("aarch64", "arm64")
            else "x86_64"
        )
        url = f"https://storage.googleapis.com/gvisor/releases/release/latest/{arch}/runsc"
        try:
            urllib.request.urlretrieve(url, runsc_path)
            os.chmod(runsc_path, 0o755)
            os.environ["PATH"] = f"{temp_bin}:{os.environ.get('PATH', '')}"
        except Exception as e:
            pytest.skip(f"Failed to install runsc for sandbox tests: {e}")


@pytest.fixture(scope="session")
def ensure_pasta():
    """Provide pasta for the per-sandbox-netns tests (network="public").

    Requested (not autouse) so only tests that exercise the pasta wrapper
    skip when a static build cannot be fetched.
    """
    if shutil.which("pasta"):
        return

    temp_bin = tempfile.mkdtemp()
    os.chmod(temp_bin, 0o755)
    pasta_path = os.path.join(temp_bin, "pasta")
    arch = "aarch64" if platform.machine().lower() in ("aarch64", "arm64") else "x86_64"
    url = f"https://passt.top/builds/latest/{arch}/pasta"
    try:
        urllib.request.urlretrieve(url, pasta_path)
        os.chmod(pasta_path, 0o755)
        os.environ["PATH"] = f"{temp_bin}:{os.environ.get('PATH', '')}"
    except Exception as e:
        pytest.skip(f"Failed to install pasta for netns tests: {e}")
