import fnmatch
import io
import os
import platform
import shutil
import tarfile
import tempfile
import textwrap
import urllib.request
import uuid

import pytest

from ray._private.test_utils import sandbox_test_enabled

_RUNSC_URL = (
    "https://storage.googleapis.com/gvisor/releases/release/latest/{arch}/runsc"
)
_SLIRP4NETNS_URL = (
    "https://github.com/rootless-containers/slirp4netns/releases/download/"
    "v1.3.5/slirp4netns-{arch}"
)
_NVIDIA_CTK_URL = (
    "https://github.com/NVIDIA/nvidia-container-toolkit/releases/download/"
    "v1.20.1/nvidia-container-toolkit_1.20.1_deb_{arch}.tar.gz"
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


@pytest.fixture
def fake_fsck_erofs(fake_mkfs_erofs, monkeypatch):
    """A stand-in ``fsck.erofs`` next to ``fake_mkfs_erofs``'s.

    It unpacks the fake image (a tar) into the ``--extract`` directory with
    its modes. It keeps owners only for ``--preserve-owner`` when run as
    root. It appends each argv to ``fsck.args`` beside the script. Yields the
    directory holding it.

    The fake image has no EROFS superblock, so its UUID is a random one kept
    in a ``rootfs.erofs.uuid`` file beside it, made on first read. A re-pull
    builds the image in a new directory, so it gets a new UUID.
    """
    from ray.experimental.sandbox._internal import image_utils

    def fake_erofs_image_uuid(erofs_image):
        # A missing image raises FileNotFoundError, like the real one.
        os.stat(erofs_image)
        uuid_file = f"{erofs_image}.uuid"
        if not os.path.exists(uuid_file):
            with open(uuid_file, "w", encoding="utf-8") as f:
                f.write(str(uuid.uuid4()))
        with open(uuid_file, encoding="utf-8") as f:
            return f.read()

    monkeypatch.setattr(image_utils, "_erofs_image_uuid", fake_erofs_image_uuid)

    script = fake_mkfs_erofs / "fsck.erofs"
    script.write_text(
        textwrap.dedent(
            """\
            #!/bin/sh
            if [ "$1" = "--help" ]; then echo "  --extract[=X]  extract"; exit 0; fi
            echo "$@" >> "$(dirname "$0")/fsck.args"
            owner=--no-same-owner
            for a in "$@"; do
              case "$a" in
                --extract=*) dir="${a#--extract=}" ;;
                --preserve-owner) owner=--same-owner ;;
              esac
              # The last argument is the image.
              image="$a"
            done
            [ "$(id -u)" = 0 ] || owner=--no-same-owner
            mkdir -p "$dir" && tar -xpf "$image" -C "$dir" "$owner"
            """
        )
    )
    script.chmod(0o755)
    image_utils.fsck_erofs_path.cache_clear()
    yield fake_mkfs_erofs
    image_utils.fsck_erofs_path.cache_clear()


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


@pytest.fixture(scope="session")
def ensure_overlayfs_mount(ensure_runsc):
    """A host that can mount an overlayfs sandbox's kernel overlay (see
    ``overlayfs.mount_mode``). Requested rather than autouse, so
    only the overlayfs tests skip when the host can't."""
    from ray.experimental.sandbox._internal import overlayfs
    from ray.experimental.sandbox.exceptions import SandboxCreationError

    try:
        overlayfs.mount_mode()
    except SandboxCreationError as err:
        pytest.skip(str(err))


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


def _gpus_available() -> bool:
    """Whether this host has a real NVIDIA GPU, so it's worth installing
    nvidia-ctk for at all. Unlike CI (see test_gvisor_gpu.py's own
    comment on why it skips this check), a dev machine running these
    tests directly isn't guaranteed to have one."""
    from ray._private.accelerators import NvidiaGPUAcceleratorManager

    try:
        return NvidiaGPUAcceleratorManager.get_current_node_num_accelerators() > 0
    except Exception:
        return False


def _extract_data_tar(deb_path: str) -> tuple:
    """Return (name, bytes) for the data.tar.* member of a .deb package.
    Hand-rolled instead of shelling out to `ar`, which isn't guaranteed
    to be on PATH. Format: https://man.freebsd.org/cgi/man.cgi?query=ar&sektion=5
    """
    with open(deb_path, "rb") as f:
        if f.read(8) != b"!<arch>\n":
            raise ValueError(f"{deb_path} is not a valid .deb package")
        while True:
            header = f.read(60)
            if len(header) < 60:
                raise ValueError(f"no data.tar.* member found in {deb_path}")
            name = header[0:16].decode("ascii").strip().rstrip("/")
            size = int(header[48:58].decode("ascii").strip())
            data = f.read(size)
            if size % 2:
                f.read(1)  # ar pads each member to an even offset
            if name.startswith("data.tar"):
                return name, data


def _install_nvidia_ctk(arch: str) -> None:
    """Extract nvidia-ctk and nvidia-cdi-hook from the pinned upstream
    release's .deb into a temp dir on PATH; skips if that or anything
    else fails. Both binaries have to land in the same directory:
    nvidia-ctk resolves nvidia-cdi-hook's path (baked into the CDI spec
    it generates) relative to its own location."""
    if shutil.which("nvidia-ctk"):
        return
    try:
        with tempfile.TemporaryDirectory() as work_dir:
            tarball = os.path.join(work_dir, "release.tar.gz")
            urllib.request.urlretrieve(_NVIDIA_CTK_URL.format(arch=arch), tarball)
            with tarfile.open(tarball) as tf:
                deb_member = next(
                    m
                    for m in tf.getmembers()
                    if fnmatch.fnmatch(
                        os.path.basename(m.name),
                        f"nvidia-container-toolkit-base_*_{arch}.deb",
                    )
                )
                tf.extract(deb_member, work_dir)

            tar_name, tar_bytes = _extract_data_tar(
                os.path.join(work_dir, deb_member.name)
            )

            bin_dir = tempfile.mkdtemp()
            os.chmod(bin_dir, 0o755)
            with tarfile.open(
                fileobj=io.BytesIO(tar_bytes), mode="r:*", name=tar_name
            ) as dtf:
                for name in ("nvidia-ctk", "nvidia-cdi-hook"):
                    bin_member = dtf.getmember(f"./usr/bin/{name}")
                    bin_member.name = name
                    dtf.extract(bin_member, bin_dir)
                    os.chmod(os.path.join(bin_dir, name), 0o755)
            os.environ["PATH"] = f"{bin_dir}:{os.environ.get('PATH', '')}"
    except Exception as e:
        pytest.skip(f"Failed to install nvidia-ctk: {e}")


@pytest.fixture(scope="session")
def ensure_nvidia_ctk():
    """nvidia-ctk plus a host that actually has a GPU to use it on.

    Requested rather than autouse, so only the two real-GPU sandbox test
    files need it. A dev machine running these tests directly isn't
    guaranteed to have a GPU, unlike CI's tag-based scheduling.
    """
    if not _gpus_available():
        pytest.skip("No NVIDIA GPU on this host")
    arch = "arm64" if platform.machine().lower() in ("aarch64", "arm64") else "amd64"
    _install_nvidia_ctk(arch)
