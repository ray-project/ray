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


# -- bucket mounts ---------------------------------------------------------------

# The mount-s3 release the bucket-mount code is tested against, with the
# sha256 of each architecture's tarball (each also verified against AWS's
# release signature when pinned).
_MOUNT_S3_VERSION = "1.24.0"
_MOUNT_S3_URL = (
    "https://s3.amazonaws.com/mountpoint-s3-release/{version}/{arch}/"
    "mount-s3-{version}-{arch}.tar.gz"
)
_MOUNT_S3_SHA256 = {
    "x86_64": "a99bea20510eaabaf9d7cbfe95ab221a11005cacaa77cfc311a2912bbbdbfd72",
    "arm64": "9f97026c9feabd0e4d128206f1846ee317e6c4f21846914dfe4268d110c892c4",
}


def _skip_or_fail_bucket_tests(reason: str):
    """Skip, unless the job says bucket-mount coverage is required."""
    if os.environ.get("RAY_SANDBOX_REQUIRE_BUCKET_TESTS") == "1":
        pytest.fail(reason)
    pytest.skip(reason)


@pytest.fixture(scope="session")
def ensure_mount_s3(ensure_runsc):
    """mount-s3 on PATH, plus the FUSE device and helper it needs."""
    import hashlib
    import tarfile
    import urllib.request

    if not shutil.which("mount-s3"):
        arch = (
            "arm64" if platform.machine().lower() in ("aarch64", "arm64") else "x86_64"
        )
        url = _MOUNT_S3_URL.format(version=_MOUNT_S3_VERSION, arch=arch)
        target = tempfile.mkdtemp()
        tarball = os.path.join(target, "mount-s3.tar.gz")
        try:
            urllib.request.urlretrieve(url, tarball)
            with open(tarball, "rb") as f:
                digest = hashlib.sha256(f.read()).hexdigest()
            if digest != _MOUNT_S3_SHA256[arch]:
                raise RuntimeError(f"sha256 mismatch for {url}: {digest}")
            with tarfile.open(tarball) as tar:
                tar.extractall(target)
        except Exception as e:
            _skip_or_fail_bucket_tests(f"Failed to install mount-s3: {e}")
        os.environ["PATH"] = f"{os.path.join(target, 'bin')}:{os.environ['PATH']}"

    from ray.experimental.sandbox._internal import bucket_mount
    from ray.experimental.sandbox.exceptions import SandboxCreationError

    try:
        bucket_mount.check_prerequisites()
    except SandboxCreationError as e:
        _skip_or_fail_bucket_tests(str(e))


def _patch_moto_for_mount_s3():
    """Make moto answer the way S3 does where mount-s3 depends on it.

    * A ``max-keys=0`` listing: moto renders IsTruncated as Python's ``True``
      with no continuation token, which mount-s3 rejects as invalid XML; S3
      answers ``false``.
    * ``PUT ?renameObject`` (mount-s3 >= 1.19 renames through it): S3 refuses it
      on a general-purpose bucket, while moto stores an empty object at the
      destination and reports success.
    """
    from moto.s3.exceptions import S3ClientError
    from moto.s3.models import S3Backend
    from moto.s3.responses import S3Response

    if getattr(S3Backend, "_ray_patched_for_mount_s3", False):
        return
    truncate = S3Backend._truncate_result

    def _truncate_result(self, result_keys, max_keys):
        keys, truncated, token = truncate(self, result_keys, max_keys)
        if max_keys == 0:
            truncated = "false"
        return keys, truncated, token

    class _RenameNotImplemented(S3ClientError):
        code = 501

        def __init__(self):
            super().__init__(
                "NotImplemented",
                "RenameObject is only supported for directory buckets.",
            )

    put = S3Response._key_response_put

    def _key_response_put(self, query, key_name):
        if "renameObject" in query:
            raise _RenameNotImplemented()
        return put(self, query, key_name)

    S3Backend._truncate_result = _truncate_result
    S3Response._key_response_put = _key_response_put
    S3Backend._ray_patched_for_mount_s3 = True


@pytest.fixture(scope="session")
def moto_s3(ensure_mount_s3):
    """A moto S3 server in this process. Yields ``(endpoint, boto3 client)``;
    the client signs with moto's dummy credentials, to set buckets up."""
    import socket

    try:
        import boto3
        from moto.server import ThreadedMotoServer
    except ImportError as e:
        _skip_or_fail_bucket_tests(f"moto is needed for bucket-mount tests: {e}")
    _patch_moto_for_mount_s3()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    endpoint = f"http://127.0.0.1:{port}"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    yield endpoint, s3
    server.stop()


def _create_public_bucket(s3, bucket: str, allow, deny=()) -> None:
    """A bucket with ``seed/hello.txt`` in it, whose policy allows ``allow``
    to anyone and denies ``deny`` to everyone.

    moto enforces only an explicit Deny on object requests, so a bucket that
    must refuse anonymous writes, as S3 does without an Allow, says so. The
    seed goes in first: the Deny binds signed requests too.
    """
    import json

    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="seed/hello.txt", Body=b"hello from the bucket\n")
    resources = [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]
    statements = [
        {"Effect": "Allow", "Principal": "*", "Action": allow, "Resource": resources}
    ]
    if deny:
        statements.append(
            {
                "Effect": "Deny",
                "Principal": "*",
                "Action": list(deny),
                "Resource": resources,
            }
        )
    s3.put_bucket_policy(
        Bucket=bucket,
        Policy=json.dumps({"Version": "2012-10-17", "Statement": statements}),
    )


@pytest.fixture(scope="session")
def public_bucket(moto_s3):
    """A bucket anyone may read and write, as an anonymous (public) mount needs.
    Yields ``(endpoint, bucket)``.

    Empty objects are not asserted on against moto: mount-s3 expects a part
    checksum moto does not return, so an empty upload fails there.
    """
    endpoint, s3 = moto_s3
    _create_public_bucket(s3, "ray-sandbox-public", "s3:*")
    yield endpoint, "ray-sandbox-public"


@pytest.fixture(scope="session")
def read_only_public_bucket(moto_s3):
    """A bucket anyone may list and read but not write, like most public
    buckets. Yields ``(endpoint, bucket)``."""
    endpoint, s3 = moto_s3
    _create_public_bucket(
        s3,
        "ray-sandbox-public-ro",
        allow=["s3:GetObject", "s3:ListBucket"],
        # moto names each object request after its method: a multipart upload
        # is POSTs and PUTs.
        deny=["s3:PutObject", "s3:PostObject", "s3:DeleteObject"],
    )
    yield endpoint, "ray-sandbox-public-ro"
