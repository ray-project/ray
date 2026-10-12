"""``modal.CloudBucketMount`` through the Modal layer, end to end.

Errors are the ones measured on Modal: a write, directory creation or removal a
bucket refuses is ``SandboxFilesystemError("I/O error")``, and a recursive
removal in a writable bucket is ``InvalidError``. Needs TEST_SANDBOX=1, runsc,
/dev/fuse, mount-s3, and a Ray cluster.
"""

import asyncio
import glob
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

import pytest

import ray
from ray.experimental.sandbox import modal
from ray.experimental.sandbox._internal import bucket_mount
from ray.experimental.sandbox.backend import gvisor
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.modal.exception import (
    InvalidError,
    SandboxFilesystemError,
    SandboxFilesystemNotFoundError,
)

pytestmark = pytest.mark.usefixtures("ensure_mount_s3", "ensure_slirp4netns")


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield


def _mount(public_bucket, **kwargs):
    endpoint, bucket = public_bucket
    return modal.CloudBucketMount(
        bucket, bucket_endpoint_url=endpoint, force_path_style=True, **kwargs
    )


@pytest.fixture(scope="module")
def sandbox(public_bucket):
    sandbox = modal.Sandbox.create(
        image="busybox:latest",
        timeout=300,
        volumes={
            "/data": _mount(public_bucket),
            "/ro": _mount(public_bucket, read_only=True),
        },
    )
    try:
        yield sandbox
    finally:
        sandbox.terminate()


def test_files_look_as_on_modal(sandbox):
    info = sandbox.filesystem.stat("/data/seed/hello.txt")
    assert (info.owner, info.group, info.permissions, info.size) == (
        "root",
        "root",
        "0644",
        22,
    )
    assert sandbox.filesystem.stat("/data").permissions == "0755"
    assert [f.name for f in sandbox.filesystem.list_files("/data/seed")] == [
        "hello.txt"
    ]
    assert (
        sandbox.filesystem.read_text("/data/seed/hello.txt")
        == "hello from the bucket\n"
    )


def test_writes_reach_the_bucket(sandbox, tmp_path):
    name = f"/data/w-{uuid.uuid4().hex[:8]}"
    sandbox.filesystem.write_bytes(b"first", f"{name}/a.bin")
    sandbox.filesystem.write_text("second", f"{name}/a.bin")
    assert sandbox.filesystem.read_text(f"{name}/a.bin") == "second"
    local = tmp_path / "local.txt"
    local.write_text("from local")
    sandbox.filesystem.copy_from_local(str(local), f"{name}/b.txt")
    process = sandbox.exec("cat", f"{name}/b.txt")
    assert process.stdout.read() == "from local"
    # No temporary file was left beside the destination.
    assert sorted(f.name for f in sandbox.filesystem.list_files(name)) == [
        "a.bin",
        "b.txt",
    ]
    sandbox.filesystem.remove(f"{name}/a.bin")
    with pytest.raises(SandboxFilesystemNotFoundError):
        sandbox.filesystem.stat(f"{name}/a.bin")


def test_rename_fails_as_on_modal(sandbox):
    name = f"/data/mv-{uuid.uuid4().hex[:8]}"
    sandbox.filesystem.write_text("x", name)
    process = sandbox.exec("mv", name, name + ".2")
    assert process.wait() != 0
    assert "Function not implemented" in process.stderr.read()


@pytest.mark.parametrize(
    "call",
    [
        lambda fs: fs.write_bytes(b"x", "/ro/new.txt"),
        lambda fs: fs.write_bytes(b"x", "/ro/seed/hello.txt"),
        lambda fs: fs.make_directory("/ro/newdir"),
        lambda fs: fs.remove("/ro/seed/hello.txt"),
        lambda fs: fs.remove("/ro/seed", recursive=True),
    ],
)
def test_a_read_only_bucket_refuses_with_modals_error(sandbox, call):
    with pytest.raises(SandboxFilesystemError) as err:
        call(sandbox.filesystem)
    assert type(err.value) is SandboxFilesystemError
    assert str(err.value) == "I/O error"
    assert (
        sandbox.filesystem.read_text("/ro/seed/hello.txt") == "hello from the bucket\n"
    )


def _object(public_bucket, key):
    """``(status, body)`` of a GET of ``key``, straight from the bucket."""
    endpoint, bucket = public_bucket
    try:
        with urllib.request.urlopen(f"{endpoint}/{bucket}/{key}", timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as err:
        return err.code, b""


def _send(public_bucket, method, key, body=None):
    """A ``method`` request for ``key``, as another client of the bucket."""
    endpoint, bucket = public_bucket
    # Without a type, urllib sends a body as a form, which moto then stores
    # as an empty object.
    request = urllib.request.Request(
        f"{endpoint}/{bucket}/{key}",
        data=body,
        method=method,
        headers={"Content-Type": "application/octet-stream"},
    )
    with urllib.request.urlopen(request, timeout=10) as r:
        assert r.status in (200, 204), r.status


def _eventually(check, what, timeout=5, interval=0.1):
    deadline = time.monotonic() + timeout
    while not check():
        assert time.monotonic() < deadline, what
        time.sleep(interval)


def test_an_upload_the_bucket_refuses_is_an_io_error(read_only_public_bucket):
    """A writable mount of a bucket that refuses anonymous writes: Modal's
    default ``read_only=False`` on a typical public bucket. However the
    refusal arrives -- on the first write, or only when the upload completes,
    which for an empty file is at close and busybox's ``cat`` ignored -- it is
    an error, never a success."""
    sandbox = modal.Sandbox.create(
        image="busybox:latest",
        volumes={"/data": _mount(read_only_public_bucket)},
    )
    try:
        for data, path in (
            (b"new", "/data/refused.txt"),
            (b"", "/data/empty.txt"),
            (b"replaced", "/data/seed/hello.txt"),
        ):
            with pytest.raises(SandboxFilesystemError) as err:
                sandbox.filesystem.write_bytes(data, path)
            assert type(err.value) is SandboxFilesystemError, path
            assert str(err.value) == "I/O error"
        # Checked in the bucket itself: after a refused overwrite, mount-s3
        # answers reads of that file with EIO, on Modal as here.
        assert _object(read_only_public_bucket, "seed/hello.txt") == (
            200,
            b"hello from the bucket\n",
        )
        assert _object(read_only_public_bucket, "refused.txt")[0] == 404
        assert _object(read_only_public_bucket, "empty.txt")[0] == 404
        # The staged payloads went with the failures.
        process = sandbox.exec("sh", "-c", "ls -A /tmp | grep -c ray-sandbox-tmp")
        assert process.stdout.read().strip() == "0"
    finally:
        sandbox.terminate()


def test_recursive_remove_of_a_file_in_a_writable_bucket(sandbox, public_bucket):
    """A single object: nothing for a recursive removal to walk into."""
    name = f"rf-{uuid.uuid4().hex[:8]}.txt"
    sandbox.filesystem.write_text("x", f"/data/{name}")
    sandbox.filesystem.remove(f"/data/{name}", recursive=True)
    assert _object(public_bucket, name)[0] == 404


def test_a_bucket_under_a_symlinked_directory(public_bucket):
    """busybox's /lib64 links to /lib, so a mount at /lib64/bkt lands at
    /lib/bkt. The filesystem API treats it as the bucket it is, by either
    path. (The link itself stays: busybox's shell loads through it.)"""
    sandbox = modal.Sandbox.create(
        image="busybox:latest",
        volumes={"/lib64/bkt": _mount(public_bucket)},
    )
    try:
        name = f"sl-{uuid.uuid4().hex[:8]}"
        sandbox.filesystem.write_text("via link", f"/lib64/bkt/{name}/a.txt")
        sandbox.filesystem.write_text("direct", f"/lib/bkt/{name}/b.txt")
        assert _object(public_bucket, f"{name}/a.txt") == (200, b"via link")
        assert _object(public_bucket, f"{name}/b.txt") == (200, b"direct")
        assert sorted(
            f.name for f in sandbox.filesystem.list_files(f"/lib/bkt/{name}")
        ) == [
            "a.txt",
            "b.txt",
        ]
        # The directory the mount really is in holds a bucket: removing it
        # recursively would walk into the bucket's objects.
        with pytest.raises(InvalidError, match="not supported for CloudBucketMounts"):
            sandbox.filesystem.remove("/lib", recursive=True)
        assert _object(public_bucket, f"{name}/a.txt") == (200, b"via link")
    finally:
        sandbox.terminate()


def test_recursive_remove_in_a_writable_bucket_is_refused(sandbox):
    name = f"/data/rr-{uuid.uuid4().hex[:8]}"
    sandbox.filesystem.write_text("keep", f"{name}/f.txt")
    with pytest.raises(InvalidError, match="not supported for CloudBucketMounts"):
        sandbox.filesystem.remove(name, recursive=True)
    assert sandbox.filesystem.read_text(f"{name}/f.txt") == "keep"


def test_changes_made_elsewhere_show_up_as_on_modal(sandbox, public_bucket):
    """Caching as measured on Modal (Mountpoint's ``--cache``): a name never
    looked up and every listing are current at once, but metadata -- that a
    name is missing included -- is kept for 60 seconds. An overwrite, a
    deletion, and a file created after its name was looked up show once that
    expires; listing their directory refreshes what it lists sooner, but not
    what it no longer lists. ``reload_volumes()`` cannot hurry any of it, here
    or on Modal."""
    fs = sandbox.filesystem
    name = f"oob-{uuid.uuid4().hex[:8]}"
    listed, unlisted = f"/data/{name}/listed", f"/data/{name}/unlisted"
    for key, body in (
        ("listed/kept.txt", b"old"),
        ("listed/gone.txt", b"x"),
        ("unlisted/kept.txt", b"old"),
    ):
        _send(public_bucket, "PUT", f"{name}/{key}", body)
    # Warm every cache on the way: the entries, the contents, and a lookup of
    # a name that does not exist yet.
    assert fs.read_text(f"{listed}/kept.txt") == "old"
    assert fs.read_text(f"{unlisted}/kept.txt") == "old"
    fs.stat(f"{listed}/gone.txt")
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.stat(f"{listed}/late.txt")

    for method, key, body in (
        ("PUT", "listed/kept.txt", b"replaced, and longer"),
        ("DELETE", "listed/gone.txt", None),
        ("PUT", "listed/new.txt", b"new"),
        ("PUT", "listed/late.txt", b"late"),
        ("PUT", "unlisted/kept.txt", b"replaced, and longer"),
    ):
        _send(public_bucket, method, f"{name}/{key}", body)
    changed = time.monotonic()

    # A name never looked up is current at once.
    assert fs.read_text(f"{listed}/new.txt") == "new"
    # Kept, as on Modal: the old contents, the deleted file, the missing name.
    assert sandbox.reload_volumes() is None
    assert fs.read_text(f"{listed}/kept.txt") == "old"
    fs.stat(f"{listed}/gone.txt")
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.stat(f"{listed}/late.txt")

    # A listing is current, and refreshes what it lists. A deleted file it no
    # longer lists stays cached until it expires.
    listing = sandbox.exec("ls", "-1", listed).stdout.read().split()
    assert listing == ["kept.txt", "late.txt", "new.txt"]
    assert fs.read_text(f"{listed}/kept.txt") == "replaced, and longer"
    assert fs.read_text(f"{listed}/late.txt") == "late"

    # The rest shows within a minute of being cached, and not much sooner.
    def current():
        try:
            fs.stat(f"{listed}/gone.txt")
            return False
        except SandboxFilesystemNotFoundError:
            pass
        try:
            return fs.read_text(f"{unlisted}/kept.txt") == "replaced, and longer"
        except SandboxFilesystemError:
            return False

    _eventually(
        current, "the overwrite never reached the sandbox", timeout=75, interval=1
    )
    assert time.monotonic() - changed > 45, "expired well before Modal's 60 s"


def _object_size(public_bucket, key):
    """The object's size from a HEAD, or None if there is none."""
    endpoint, bucket = public_bucket
    request = urllib.request.Request(f"{endpoint}/{bucket}/{key}", method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=10) as r:
            return int(r.headers["Content-Length"])
    except urllib.error.HTTPError:
        return None


class _AnnouncingActor:
    """The sandbox's actor handle, setting ``event`` when an upload starts
    waiting for its command -- by then its whole payload has been sent."""

    def __init__(self, actor, event, loop):
        self._actor, self._event, self._loop = actor, event, loop

    def __getattr__(self, name):
        method = getattr(self._actor, name)
        if name != "exec_wait":
            return method
        announcer = self

        class _Announcing:
            def remote(self, *args, **kwargs):
                announcer._loop.call_soon_threadsafe(announcer._event.set)
                return method.remote(*args, **kwargs)

        return _Announcing()


@pytest.mark.parametrize(
    "where,delay",
    [
        # Half a second in, the copy into the bucket is uploading: measured
        # 119 MiB of 192 MiB written by 0.6 s.
        ("bucket", 0.5),
        # At once, before the write's own reply can arrive: on an ordinary
        # path the rename follows the payload in moments.
        ("ordinary path", 0.0),
    ],
)
def test_a_write_cancelled_once_its_payload_is_sent_still_lands_whole(
    sandbox, public_bucket, where, delay
):
    """As on Modal, a write whose payload has all been sent finishes even if
    its caller is cancelled. Killing it used to leave a bucket object
    truncated: Mountpoint publishes what was written when the writer dies,
    measured as 119 MiB of a 192 MiB payload replacing the old object."""
    name = f"cancelled-{uuid.uuid4().hex[:8]}.bin"
    path = f"/data/{name}" if where == "bucket" else f"/tmp/{name}"
    old, new = b"old contents\n", os.urandom(192 << 20)
    sandbox.filesystem.write_bytes(old, path)
    impl = sandbox._impl
    actor = impl._actor

    async def cancel_once_sent():
        sent = asyncio.Event()
        impl._actor = _AnnouncingActor(actor, sent, asyncio.get_running_loop())
        try:
            write = asyncio.ensure_future(impl.filesystem.write_bytes(new, path))
            await sent.wait()
            await asyncio.sleep(delay)
            write.cancel()
            with pytest.raises(asyncio.CancelledError):
                await write
        finally:
            impl._actor = actor

    asyncio.run(cancel_once_sent())
    if where == "bucket":
        sizes = set()

        def landed():
            size = _object_size(public_bucket, name)
            sizes.add(size)
            return size == len(new)

        _eventually(landed, "the cancelled write never landed", timeout=60)
        assert sizes <= {len(old), len(new)}, f"a partial object was seen: {sizes}"
        assert _object(public_bucket, name) == (200, new)
    else:
        _eventually(
            lambda: sandbox.filesystem.stat(path).size == len(new),
            "the cancelled write never landed",
            timeout=60,
        )
        assert sandbox.filesystem.read_bytes(path) == new
    # The script cleaned up after itself: nothing staged, nothing hidden.
    process = sandbox.exec(
        "sh", "-c", "ls -A /tmp /data | grep -c ray-sandbox-tmp || true"
    )
    assert process.stdout.read().strip() == "0"


def test_the_rest_of_the_filesystem_is_unaffected(sandbox):
    """Paths outside a bucket keep the usual atomic, rename-based writes."""
    sandbox.filesystem.write_text("tmp", "/tmp/plain.txt")
    assert sandbox.filesystem.read_text("/tmp/plain.txt") == "tmp"
    sandbox.filesystem.make_directory("/tmp/d/e")
    sandbox.filesystem.remove("/tmp/d", recursive=True)
    with pytest.raises(SandboxFilesystemNotFoundError):
        sandbox.filesystem.stat("/tmp/d")


def test_block_network_still_reads_the_bucket(public_bucket):
    sandbox = modal.Sandbox.create(
        image="busybox:latest",
        block_network=True,
        volumes={"/data": _mount(public_bucket, read_only=True)},
    )
    try:
        assert (
            sandbox.filesystem.read_text("/data/seed/hello.txt")
            == "hello from the bucket\n"
        )
    finally:
        sandbox.terminate()


def test_a_missing_bucket_fails_create(public_bucket):
    endpoint, _ = public_bucket
    with pytest.raises(Exception, match="ray-no-such-bucket"):
        modal.Sandbox.create(
            image="busybox:latest",
            volumes={
                "/data": modal.CloudBucketMount(
                    "ray-no-such-bucket",
                    bucket_endpoint_url=endpoint,
                    force_path_style=True,
                )
            },
        )


# -- A sandbox whose worker dies without tearing it down ---------------------------


@ray.remote
class _Owner:
    """Creates a sandbox, which it then owns: when this actor dies, Ray
    force-kills the sandbox's actor too, as when a driver exits without
    terminate()."""

    def make(self, endpoint, bucket):
        self.sandbox = modal.Sandbox.create(
            image="busybox:latest",
            timeout=600,
            block_network=True,
            volumes={
                "/data": modal.CloudBucketMount(
                    bucket, bucket_endpoint_url=endpoint, force_path_style=True
                )
            },
        )
        return (
            self.sandbox.object_id,
            self.sandbox.filesystem.read_text("/data/seed/hello.txt"),
        )


def _mount_s3_pids(object_id):
    """The mount-s3 processes of a sandbox's buckets, from their pid files."""
    pids = []
    for pid_file in glob.glob(f"{bucket_mount.MOUNT_ROOT}/*-{object_id}-*/*.pid"):
        with open(pid_file) as f:
            pids.append(int(f.read()))
    return pids


def _gone(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] == "Z"
    except FileNotFoundError:
        return True


def _wait_gone(pids, timeout=15):
    deadline = time.monotonic() + timeout
    while not all(_gone(p) for p in pids):
        assert time.monotonic() < deadline, f"mount-s3 {pids} still running"
        time.sleep(0.1)


def _on_the_node(object_id):
    with open("/proc/self/mountinfo") as f:
        return [line for line in f if object_id in line]


@pytest.mark.parametrize("how", ["ray.kill(owner)", "kill -9 worker"])
def test_buckets_end_with_a_sandbox_whose_worker_is_killed(public_bucket, how):
    """No teardown runs in the worker either way, and nothing here cleans up:
    the sandbox's supervisor ends the container, which ends mount-s3, and
    removes the rest. The bucket never shows on the node meanwhile."""
    endpoint, bucket = public_bucket
    if how == "ray.kill(owner)":
        owner = _Owner.remote()
        object_id, text = ray.get(owner.make.remote(endpoint, bucket))
    else:
        sandbox = modal.Sandbox.create(
            image="busybox:latest",
            timeout=600,
            block_network=True,
            volumes={"/data": _mount(public_bucket)},
        )
        object_id = sandbox.object_id
        text = sandbox.filesystem.read_text("/data/seed/hello.txt")
    assert text == "hello from the bucket\n"
    pids = _mount_s3_pids(object_id)
    assert pids and not any(_gone(p) for p in pids)
    assert not _on_the_node(object_id)
    root_dir = os.path.join(gvisor._RAY_SANDBOX_DIR, object_id)
    state = GVisorSandboxBackend()._runsc_base_args(
        SandboxConfig(image="busybox:latest", network="none")
    ) + ["state", object_id]
    assert subprocess.run(state, capture_output=True).returncode == 0

    if how == "ray.kill(owner)":
        ray.kill(owner)
    else:
        actor = sandbox._impl._actor
        worker = ray.get(actor.__ray_call__.remote(lambda self: os.getpid()))
        os.kill(worker, signal.SIGKILL)
    _wait_gone(pids, timeout=30)
    deadline = time.monotonic() + 30
    while os.path.exists(root_dir) or glob.glob(
        f"{bucket_mount.MOUNT_ROOT}/*-{object_id}-*"
    ):
        assert time.monotonic() < deadline, "the sandbox was not torn down"
        time.sleep(0.1)
    assert subprocess.run(state, capture_output=True).returncode != 0
    assert not _on_the_node(object_id)


def test_ported_example_sandbox_cloud_bucket(public_bucket):
    """Ports modal-client's ``go/examples/sandbox-cloud-bucket`` and
    ``js/examples/sandbox-cloud-bucket.ts`` (02613d5d): an alpine Sandbox whose
    command lists a bucket mounted read-only under the key prefix ``data/``.

    Theirs mounts a private bucket through a Secret; this one is public.
    """
    import urllib.request

    endpoint, bucket = public_bucket
    for key in ("data/a.csv", "data/b.csv", "other/c.csv"):
        urllib.request.urlopen(
            urllib.request.Request(
                f"{endpoint}/{bucket}/{key}",
                data=b"x,y\n",
                method="PUT",
                headers={"Content-Type": "application/octet-stream"},
            ),
            timeout=10,
        )
    sb = modal.Sandbox.create(
        "sh",
        "-c",
        "ls -la /mnt/s3-bucket",
        image=modal.Image.from_registry("alpine:3.21"),
        volumes={
            "/mnt/s3-bucket": _mount(public_bucket, key_prefix="data/", read_only=True)
        },
    )
    try:
        listing = sb.stdout.read()
        sb.wait()
        assert sb.returncode == 0
    finally:
        sb.terminate()
    names = {line.split()[-1] for line in listing.splitlines() if line.startswith("-")}
    assert names == {"a.csv", "b.csv"}, listing


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
