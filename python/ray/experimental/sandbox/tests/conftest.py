import os
import platform
import shutil
import tempfile
import urllib.request

import pytest

from ray._private.test_utils import sandbox_test_enabled

_RUNSC_URL = (
    "https://storage.googleapis.com/gvisor/releases/release/latest/{arch}/runsc"
)
_PASTA_URL = "https://passt.top/builds/latest/x86_64/pasta"


def pytest_runtest_setup(item):
    if not sandbox_test_enabled():
        pytest.skip("Sandbox tests are only run when TEST_SANDBOX=1")
    os.environ["RAY_SANDBOX_IGNORE_CGROUPS"] = "1"


def _install_on_path(name: str, url: str) -> None:
    """Fetch a static binary into a temp dir prepended to PATH, or skip."""
    if shutil.which(name):
        return
    bin_dir = tempfile.mkdtemp()
    os.chmod(bin_dir, 0o755)
    binary = os.path.join(bin_dir, name)
    try:
        urllib.request.urlretrieve(url, binary)
    except Exception as e:
        pytest.skip(f"Failed to install {name} for sandbox tests: {e}")
    os.chmod(binary, 0o755)
    os.environ["PATH"] = f"{bin_dir}:{os.environ.get('PATH', '')}"


@pytest.fixture(scope="session", autouse=True)
def ensure_runsc():
    if not sandbox_test_enabled():
        return
    os.environ["RAY_SANDBOX_IGNORE_CGROUPS"] = "1"
    arch = "aarch64" if platform.machine().lower() in ("aarch64", "arm64") else "x86_64"
    _install_on_path("runsc", _RUNSC_URL.format(arch=arch))


def _public_netns_supported() -> bool:
    """Whether this host can actually bring a network="public" sandbox up.

    The path parks a sandbox's network+user namespaces in an
    ``unshare --user --net`` holder and has pasta and a non-rootless runsc
    re-enter them via ``nsenter -U -n``. Some sandboxed CI environments permit
    a single unprivileged user namespace (enough for the rootless sandbox
    tests) and even entering another process's, yet still forbid the *nested*
    user namespace runsc opens when it drops the sandbox process to ``nobody``
    -- which only surfaces once the sandbox boots, as ``Started as root, will
    change to nobody. Couldn't open user namespace ...: Permission denied``. A
    namespace-entry probe passes there and the tests then fail, so instead
    bring a throwaway busybox sandbox all the way up and tear it down: only the
    real path exercises that nested open. runsc and pasta must already be on
    PATH.
    """
    from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
    from ray.experimental.sandbox.config import GVisorSandboxConfig
    from ray.experimental.sandbox.exceptions import SandboxError

    backend = GVisorSandboxBackend()
    try:
        sandbox_id = backend.create_sandbox(
            GVisorSandboxConfig(
                image="busybox:latest", shell="/bin/sh", network="public"
            )
        )
    except SandboxError:
        return False
    backend.delete_sandbox(sandbox_id)
    return True


@pytest.fixture(scope="session")
def ensure_pasta(ensure_runsc):
    """pasta plus a host that can actually run the network="public" path.

    Requested rather than autouse, so only the netns tests skip when no static
    pasta build exists for the arch or the environment forbids the per-sandbox
    user+network namespace the path relies on. pasta and runsc are installed
    first because the support probe boots a real sandbox.
    """
    if not shutil.which("pasta") and platform.machine().lower() not in (
        "x86_64",
        "amd64",
    ):
        pytest.skip("no static pasta build for this arch; install passt")
    _install_on_path("pasta", _PASTA_URL)
    if not _public_netns_supported():
        pytest.skip(
            'network="public" needs a per-sandbox user+network namespace this '
            "environment forbids (nested user namespace denied at sandbox boot)"
        )
