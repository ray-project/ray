import os
import platform
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from ray._private.test_utils import sandbox_test_enabled

_SLIRP4NETNS_URL = (
    "https://github.com/rootless-containers/slirp4netns/releases/download/"
    "v1.3.5/slirp4netns-{arch}"
)


def pytest_runtest_setup(item):
    if not sandbox_test_enabled():
        pytest.skip("Sandbox tests are only run when TEST_SANDBOX=1")
    os.environ["RAY_SANDBOX_IGNORE_CGROUPS"] = "1"


@pytest.fixture
def fake_mkfs_erofs(tmp_path, monkeypatch):
    """A stand-in ``mkfs.erofs`` first on PATH, for tests that need no gVisor.

    It advertises ``--tar`` and copies the flattened tar it is handed to the
    image path, so a cached ``rootfs.erofs`` is a tar the test can open and
    inspect, with the owners the real build would store. Its argv lands in
    ``mkfs.args`` next to the script. Yields the directory holding both.
    """
    from ray.experimental.sandbox._internal.image_utils import mkfs_erofs_path

    bin_dir = tmp_path / "fake-mkfs-bin"
    bin_dir.mkdir()
    script = bin_dir / "mkfs.erofs"
    script.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = "--help" ]; then echo "  --tar=MODE  build from tarball"; exit 0; fi\n'
        'echo "$@" > "$(dirname "$0")/mkfs.args"\n'
        "# args: --tar=f -b<page size> -E^inline_data OUT TAR\n"
        'cp "$5" "$4"\n'
    )
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ.get('PATH', '')}")
    mkfs_erofs_path.cache_clear()
    yield bin_dir
    mkfs_erofs_path.cache_clear()


def _install_on_path(name: str, url: str) -> None:
    """Fetch a static binary into a temp dir prepended to PATH, or skip."""
    if shutil.which(name):
        return
    bin_dir = tempfile.mkdtemp()
    os.chmod(bin_dir, 0o755)
    binary = os.path.join(bin_dir, name)
    try:
        import urllib.request

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
    if not shutil.which("runsc"):
        temp_bin = tempfile.mkdtemp()
        script = Path(__file__).resolve().parents[5] / "ci" / "env" / "install-runsc.sh"
        try:
            subprocess.check_call(["bash", str(script), temp_bin])
            os.environ["PATH"] = f"{temp_bin}:{os.environ.get('PATH', '')}"
        except Exception as e:
            pytest.skip(f"Failed to install runsc for sandbox tests: {e}")


def _public_netns_supported() -> bool:
    """Whether this host can actually bring a network="public" sandbox up.

    The path parks a sandbox's network+user namespaces in an
    ``unshare --user --net`` holder and has slirp4netns and a non-rootless runsc
    re-enter them via ``nsenter -U -n``. Some sandboxed CI environments permit
    a single unprivileged user namespace (enough for the rootless sandbox
    tests) and even entering another process's, yet still forbid the *nested*
    user namespace runsc opens when it drops the sandbox process to ``nobody``
    -- which only surfaces once the sandbox boots, as ``Started as root, will
    change to nobody. Couldn't open user namespace ...: Permission denied``. A
    namespace-entry probe passes there and the tests then fail, so instead
    bring a throwaway busybox sandbox all the way up and tear it down: only the
    real path exercises that nested open. runsc and slirp4netns must already be
    on PATH.
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


@pytest.fixture(autouse=True)
def _clear_overlayfs_detect_caches():
    """Each test decides an overlayfs sandbox's user namespace, image mount
    mode and subordinate ids afresh, so its stubs take effect."""
    from ray.experimental.sandbox._internal import overlayfs

    caches = (
        overlayfs.UserNamespaceType.detect,
        overlayfs.ImageMountMode.detect,
        overlayfs.IdMaps.detect,
    )
    for cache in caches:
        cache.cache_clear()
    yield
    for cache in caches:
        cache.cache_clear()


@pytest.fixture(scope="session")
def ensure_overlayfs_mount(ensure_runsc):
    """A host that can mount an overlayfs sandbox's kernel overlay (see
    ``overlayfs.UserNamespaceType.detect`` and
    ``overlayfs.ImageMountMode.detect``). Requested rather than autouse, so
    only the overlayfs tests skip when the host can't."""
    from ray.experimental.sandbox._internal import overlayfs
    from ray.experimental.sandbox.exceptions import SandboxCreationError

    try:
        overlayfs.ImageMountMode.detect(overlayfs.UserNamespaceType.detect())
    except SandboxCreationError as err:
        pytest.skip(str(err))


def _skip_unless_runsc_runs_in_private_userns(tmp_path):
    """Skip unless runsc can start in the private user namespace an overlayfs
    sandbox gets. For a worker running as a user other than root, that
    namespace only maps the worker's own ids, so a runsc the worker reaches
    only through a group, or through a directory owned by an unmapped user,
    can't start inside it."""
    from ray.experimental.sandbox._internal import overlayfs

    runsc = shutil.which("runsc") or "runsc"
    id_maps = overlayfs.IdMaps.detect()
    overlay = overlayfs.RootfsOverlay(
        image="",
        image_mount_mode=overlayfs.ImageMountMode.FUSE,
        userns=overlayfs.UserNamespaceType.PRIVATE,
        id_maps=id_maps,
        mountpoint=str(tmp_path / "mnt"),
        tmpfs_dir=str(tmp_path / "tmpfs"),
    )
    res = subprocess.run(
        overlay._wrap_in_private_userns([runsc, "--version"]),
        capture_output=True,
        text=True,
        timeout=60,
    )
    if res.returncode != 0:
        mapped = "only root" if id_maps is None else "its id maps"
        pytest.skip(
            f"{runsc} can't start in this worker's private user namespace, "
            f"which maps {mapped}, so an overlayfs sandbox in "
            f"one can't start here either: {res.stderr.strip()}"
        )


@pytest.fixture(
    params=[
        ("HOST", "KERNEL"),
        ("HOST", "FUSE"),
        ("PRIVATE", "FUSE"),
    ],
    ids=["host-kernel", "host-fuse", "private-fuse"],
)
def overlay_mount(request, monkeypatch, tmp_path, ensure_overlayfs_mount):
    """Runs a test through each way of mounting an overlayfs sandbox's rootfs
    this worker supports, as a (user namespace, image mount) pair."""
    from ray.experimental.sandbox._internal import overlayfs

    userns = overlayfs.UserNamespaceType[request.param[0]]
    image_mount_mode = overlayfs.ImageMountMode[request.param[1]]
    if userns is overlayfs.UserNamespaceType.HOST and not (
        overlayfs._is_host_userns() and overlayfs._has_mount_privileges()
    ):
        pytest.skip("this worker can't mount in the host's user namespace")
    mounted, errstr = overlayfs._probe_mount(userns, image_mount_mode)
    if not mounted:
        pytest.skip(f"this worker can't mount this way: {errstr}")
    if userns is overlayfs.UserNamespaceType.PRIVATE:
        _skip_unless_runsc_runs_in_private_userns(tmp_path)
    monkeypatch.setattr(overlayfs.UserNamespaceType, "detect", lambda: userns)
    monkeypatch.setattr(
        overlayfs.ImageMountMode, "detect", lambda userns: image_mount_mode
    )
    return userns, image_mount_mode


@pytest.fixture(scope="session")
def ensure_slirp4netns(ensure_runsc):
    """slirp4netns plus a host that can actually run the network="public" path.

    Requested rather than autouse, so only the netns tests skip when the
    environment forbids the per-sandbox user+network namespace the path
    relies on. slirp4netns and runsc are installed first because the support
    probe boots a real sandbox.
    """
    arch = "aarch64" if platform.machine().lower() in ("aarch64", "arm64") else "x86_64"
    _install_on_path("slirp4netns", _SLIRP4NETNS_URL.format(arch=arch))
    if not _public_netns_supported():
        pytest.skip(
            'network="public" needs a per-sandbox user+network namespace this '
            "environment forbids (nested user namespace denied at sandbox boot)"
        )
