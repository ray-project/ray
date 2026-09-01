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


def _public_netns_supported() -> bool:
    """Whether this host can actually bring a network="public" sandbox up.

    The path parks a sandbox's network+user namespaces in an
    ``unshare --user --net`` holder and has pasta and a non-rootless runsc
    re-enter them via ``nsenter -U -n``. Some sandboxed CI environments permit
    a single unprivileged user namespace (enough for the rootless sandbox
    tests) and even entering another process's, yet still forbid the *nested*
    user namespace runsc opens when it drops the sandbox process to ``nobody``
    -- which only surfaces once the sandbox boots, as ``Started as root, will
    change to nobody. Couldn't open user namespace ...: Permission denied``.
    Bring a throwaway busybox sandbox all the way up and tear it down: only the
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
    """Provide pasta for the per-sandbox-netns tests (network="public").

    Requested (not autouse) so only tests that exercise the pasta wrapper skip
    when a static build cannot be fetched or the environment cannot boot the
    sandbox. pasta and runsc are installed before the support probe, which
    boots a real sandbox.
    """
    if not shutil.which("pasta"):
        if platform.machine().lower() not in ("x86_64", "amd64"):
            pytest.skip(
                "no static pasta build for this arch; install passt to run "
                "the netns tests"
            )
        temp_bin = tempfile.mkdtemp()
        os.chmod(temp_bin, 0o755)
        pasta_path = os.path.join(temp_bin, "pasta")
        url = "https://passt.top/builds/latest/x86_64/pasta"
        try:
            urllib.request.urlretrieve(url, pasta_path)
            os.chmod(pasta_path, 0o755)
            os.environ["PATH"] = f"{temp_bin}:{os.environ.get('PATH', '')}"
        except Exception as e:
            pytest.skip(f"Failed to install pasta for netns tests: {e}")

    if not _public_netns_supported():
        pytest.skip(
            'network="public" needs a per-sandbox user+network namespace this '
            "environment forbids (nested user namespace denied at sandbox boot)"
        )


@pytest.fixture(scope="session")
def ensure_idmap_node():
    """The node's multi-uid mapping; skips when the node cannot map one.

    Multi-uid tests need the setuid newuidmap/newgidmap helpers (uidmap
    package) and /etc/subuid + /etc/subgid ranges for the test user.
    """
    from ray.experimental.sandbox._internal.idmap import detect_idmap

    detect_idmap.cache_clear()
    idmap = detect_idmap()
    if idmap is None:
        pytest.skip(
            "node lacks newuidmap/newgidmap or usable /etc/subuid ranges; "
            "multi-uid tests skipped"
        )
    return idmap
