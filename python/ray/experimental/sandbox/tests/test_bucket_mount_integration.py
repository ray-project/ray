"""Bucket mounts end to end: real mount-s3, gVisor, and a local S3 (moto).

Needs TEST_SANDBOX=1, runsc, a usable /dev/fuse, and mount-s3 (downloaded when
missing). The bucket is public: anyone may read and write it anonymously, which
is what a mount without credentials needs.
"""

import os
import re
import signal
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.request
import uuid

import pytest

from ray.experimental.sandbox import DOCKER_DEFAULT_CAPABILITIES, BucketMount
from ray.experimental.sandbox._internal import bucket_mount
from ray.experimental.sandbox.exceptions import SandboxCreationError
from ray.experimental.sandbox.runtime import SandboxRuntime

pytestmark = pytest.mark.usefixtures("ensure_mount_s3")

IMAGE = "busybox:latest"

# Runs inside the sandbox as one command; $1 is a key prefix of its own, so
# runs never see each other's files. One line per operation: its exit code and
# output, with the prefix replaced, so runs compare byte for byte.
FINGERPRINT = r"""
D=/data; R=/ro; P="$1"; Q="$D/$P"
run() { label=$1; shift; out=$( "$@" 2>&1 ); rc=$?; printf '%s rc=%s %s\n' "$label" "$rc" "$(printf '%s' "$out" | tr '\n' '|' )"; }
sh_run() { label=$1; shift; run "$label" sh -c "$*"; }
run stat_rw_root   stat -c '%u:%g %a %F' $D
run stat_ro_root   stat -c '%u:%g %a %F' $R
run stat_seed      stat -c '%u:%g %a %s' $D/seed/hello.txt
run cat_seed       cat $D/seed/hello.txt
run mkdir_prefix   mkdir -p $Q
sh_run create      "echo created > $Q/f1 && cat $Q/f1"
sh_run overwrite   "echo over > $Q/f1 && cat $Q/f1"
sh_run cp_over     "cp $D/seed/hello.txt $Q/f1 && cat $Q/f1"
sh_run dd_new      "echo dd | dd of=$Q/f2 2>/dev/null && cat $Q/f2"
sh_run append      "echo app >> $Q/f1"
sh_run truncate0   "truncate -s 0 $Q/f1"
sh_run touch_old   "touch $Q/f1"
sh_run mv          "mv $Q/f2 $Q/f4"
sh_run read_while_write "(sleep 1; echo w) > $Q/f5 & sleep 0.3; cat $Q/f5; wait"
sh_run rm          "rm $Q/f2"
sh_run mkdir_write "mkdir $Q/sub && echo n > $Q/sub/n.txt && cat $Q/sub/n.txt"
run stat_new       stat -c '%u:%g %a %s' $Q/sub/n.txt
sh_run ro_write    "echo x > $R/x"
sh_run ro_mkdir    "mkdir $R/d"
sh_run ro_rm       "rm $R/seed/hello.txt"
sh_run ls_prefix   "ls $Q | sort"
"""


@pytest.fixture(scope="module")
def runtime():
    return SandboxRuntime()


def _bucket_mounts(public_bucket):
    endpoint, bucket = public_bucket
    common = dict(endpoint_url=endpoint, region="us-east-1", force_path_style=True)
    return [
        BucketMount(bucket, "/data", read_only=False, **common),
        BucketMount(bucket, "/ro", read_only=True, **common),
    ]


def _http(public_bucket, method, key, body=None):
    endpoint, bucket = public_bucket
    request = urllib.request.Request(
        f"{endpoint}/{bucket}/{key}",
        data=body,
        method=method,
        # urllib's default form type makes moto store an empty body.
        headers={"Content-Type": "application/octet-stream"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as err:
        return err.code, b""


def _keys(public_bucket, prefix):
    endpoint, bucket = public_bucket
    with urllib.request.urlopen(
        f"{endpoint}/{bucket}?list-type=2&prefix={prefix}/", timeout=10
    ) as response:
        body = response.read().decode()
    return sorted(k[len(prefix) + 1 :] for k in re.findall(r"<Key>([^<]*)</Key>", body))


def _sh(runtime, sid, script, user=None):
    result = runtime.exec(sid, ["sh", "-c", script], timeout=120, user=user)
    return result.exit_code, (result.stdout + result.stderr).strip()


def _fingerprint(runtime, sid, public_bucket, user):
    prefix = f"fp-{uuid.uuid4().hex[:8]}"
    out = runtime.exec(
        sid, ["sh", "-c", FINGERPRINT, "sh", prefix], timeout=120, user=user
    )
    lines = [out.stdout]
    lines.append("s3_keys " + " ".join(_keys(public_bucket, prefix)))
    # close() completes the upload: the object is in S3 once exec returns.
    _sh(runtime, sid, f"echo durable > /data/{prefix}/dur.txt", user=user)
    lines.append(
        "durable_after_close %s %r" % _http(public_bucket, "GET", f"{prefix}/dur.txt")
    )
    # As another user, the script's mkdir of the prefix failed, and the cache
    # keeps that miss for 60 seconds, as on Modal: what is added under the
    # prefix from outside reads as missing until then -- the same everywhere.
    _http(public_bucket, "PUT", f"{prefix}/oob.txt", b"oob\n")
    lines.append(
        "oob_put_seen %s" % (_sh(runtime, sid, f"ls /data/{prefix}", user=user),)
    )
    _http(public_bucket, "PUT", f"{prefix}/same.txt", b"AAAA")
    first = _sh(runtime, sid, f"cat /data/{prefix}/same.txt", user=user)
    _http(public_bucket, "PUT", f"{prefix}/same.txt", b"BBBB")
    second = _sh(runtime, sid, f"cat /data/{prefix}/same.txt", user=user)
    lines.append(f"same_size_overwrite {first} {second}")
    # Every line, so error messages naming a path compare across cells too.
    return "\n".join(lines).replace(prefix, "<P>")


def test_bucket_behavior_is_config_agnostic(runtime, public_bucket, ensure_slirp4netns):
    """One fingerprint, byte-identical across every sandbox configuration.

    Commands run as root -- the default, and all the Modal layer ever uses --
    get full access. Commands run as another user get one view of their own,
    the same everywhere: read-only, as files belong to the sandbox's root.
    """
    fingerprints = {}
    # (network, rootless). A root worker can also run runsc without --rootless,
    # which network="sandbox" requires.
    modes = [("none", True), ("public", True), ("host", True)]
    if os.geteuid() == 0:
        modes.append(("sandbox", False))
    for network, rootless in modes:
        for caps in (None, DOCKER_DEFAULT_CAPABILITIES):
            for readonly in (False, True):
                sid = runtime.create(
                    IMAGE,
                    network=network,
                    rootless=rootless,
                    capabilities=caps,
                    readonly=readonly,
                    shell="/bin/sh",
                    bucket_mounts=_bucket_mounts(public_bucket),
                )
                try:
                    cell = (network, "docker" if caps else "default", readonly)
                    for user in (None, "1000"):
                        fingerprints.setdefault(user, {})[cell] = _fingerprint(
                            runtime, sid, public_bucket, user
                        )
                finally:
                    runtime.delete(sid)
    for user, by_cell in fingerprints.items():
        distinct = set(by_cell.values())
        assert len(distinct) == 1, (user, by_cell)

    root = next(iter(fingerprints[None].values()))
    for expected in (
        "stat_rw_root rc=0 0:0 755 directory",
        "stat_seed rc=0 0:0 644 22",
        "create rc=0 created",
        "overwrite rc=0 over",
        "mv rc=1 mv: can't rename '/data/<P>/f2': Function not implemented",
        "append rc=1 sh: line 0: can't create /data/<P>/f1: Operation not permitted",
        "ro_write rc=1 sh: line 0: can't create /ro/x: Read-only file system",
        "durable_after_close 200 b'durable\\n'",
        # Read moments after an overwrite elsewhere, the cached contents, as
        # on Modal: the cache keeps metadata for 60 seconds.
        "same_size_overwrite (0, 'AAAA') (0, 'AAAA')",
    ):
        assert expected in root, (expected, root)
    other = next(iter(fingerprints["1000"].values()))
    assert "cat_seed rc=0 hello from the bucket" in other
    assert (
        "mkdir_prefix rc=1 mkdir: can't create directory '/data/<P>': Permission denied"
        in other
    )


def test_key_prefix_scopes_the_view(runtime, public_bucket):
    endpoint, bucket = public_bucket
    sid = runtime.create(
        IMAGE,
        shell="/bin/sh",
        readonly=False,
        bucket_mounts=[
            BucketMount(
                bucket,
                "/seed",
                endpoint_url=endpoint,
                region="us-east-1",
                force_path_style=True,
                key_prefix="seed/",
            )
        ],
    )
    try:
        assert _sh(runtime, sid, "ls /seed") == (0, "hello.txt")
    finally:
        runtime.delete(sid)


def test_one_bucket_at_two_paths(runtime, public_bucket):
    endpoint, bucket = public_bucket
    common = dict(
        endpoint_url=endpoint,
        region="us-east-1",
        force_path_style=True,
        read_only=False,
    )
    sid = runtime.create(
        IMAGE,
        shell="/bin/sh",
        readonly=False,
        bucket_mounts=[
            BucketMount(bucket, "/a", **common),
            BucketMount(bucket, "/b c", **common),
        ],
    )
    try:
        mounts = runtime.backend._sandbox_metadata[sid]["bucket_mounts"]
        assert len({m.process.pid for m in mounts._mounts}) == 2, "one process per path"
        name = f"two-{uuid.uuid4().hex[:8]}.txt"
        assert _sh(runtime, sid, f"echo shared > /a/{name}")[0] == 0
        assert _sh(runtime, sid, f"cat '/b c/{name}'") == (0, "shared")
    finally:
        runtime.delete(sid)


def test_teardown_leaves_nothing_behind(runtime, public_bucket):
    sid = runtime.create(
        IMAGE,
        shell="/bin/sh",
        readonly=False,
        bucket_mounts=_bucket_mounts(public_bucket),
    )
    mounts = runtime.backend._sandbox_metadata[sid]["bucket_mounts"]
    processes = [m.process for m in mounts._mounts]
    host_dirs = [source for _, source, _ in mounts.bind_mounts()]
    runtime.delete(sid)
    assert all(p.poll() is not None for p in processes)
    assert not any(bucket_mount.is_mounted(d) for d in host_dirs)
    assert not any(os.path.exists(d) for d in host_dirs)
    assert not os.path.exists(mounts._parent_dir)


def test_a_bucket_that_cannot_be_mounted_fails_create_and_leaves_nothing(
    runtime, public_bucket
):
    endpoint, _ = public_bucket
    before = (
        set(os.listdir(bucket_mount.MOUNT_ROOT))
        if os.path.isdir(bucket_mount.MOUNT_ROOT)
        else set()
    )
    with pytest.raises(SandboxCreationError, match="ray-no-such-bucket"):
        runtime.create(
            IMAGE,
            shell="/bin/sh",
            bucket_mounts=[
                BucketMount(
                    "ray-no-such-bucket",
                    "/data",
                    endpoint_url=endpoint,
                    region="us-east-1",
                    force_path_style=True,
                )
            ],
        )
    assert set(os.listdir(bucket_mount.MOUNT_ROOT)) == before


def test_a_dead_mount_fails_loudly_without_killing_the_sandbox(runtime, public_bucket):
    sid = runtime.create(
        IMAGE,
        shell="/bin/sh",
        readonly=False,
        bucket_mounts=_bucket_mounts(public_bucket),
    )
    try:
        mounts = runtime.backend._sandbox_metadata[sid]["bucket_mounts"]
        os.killpg(mounts._mounts[1].process.pid, signal.SIGKILL)  # the /ro mount
        mounts._mounts[1].process.wait(10)
        code, out = _sh(runtime, sid, "cat /ro/seed/hello.txt")
        assert code != 0 and "Transport endpoint is not connected" in out
        assert _sh(runtime, sid, "cat /data/seed/hello.txt") == (
            0,
            "hello from the bucket",
        )
        assert _sh(runtime, sid, "echo alive") == (0, "alive")
    finally:
        runtime.delete(sid)


def _wait_for_exit(pid, what):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)
    pytest.fail(f"{what} is still running")


def _sandbox_mounts_in_mountinfo():
    with open("/proc/self/mountinfo") as f:
        return [line for line in f if bucket_mount.MOUNT_ROOT in line]


def test_a_running_sandbox_holds_its_buckets_alone(runtime, public_bucket):
    """Once the sandbox runs, nothing on the node is mounted: the container
    holds the buckets itself, and mount-s3 serves them through that."""
    sid = runtime.create(
        IMAGE,
        shell="/bin/sh",
        readonly=False,
        bucket_mounts=_bucket_mounts(public_bucket),
    )
    try:
        mounts = runtime.backend._sandbox_metadata[sid]["bucket_mounts"]
        host_dirs = [source for _, source, _ in mounts.bind_mounts()]
        assert not any(bucket_mount.is_mounted(d) for d in host_dirs)
        assert not _sandbox_mounts_in_mountinfo()
        assert all(os.listdir(d) == [] for d in host_dirs)
        assert all(m.process.poll() is None for m in mounts._mounts)

        name = f"held-{uuid.uuid4().hex[:8]}.txt"
        assert _sh(runtime, sid, "cat /data/seed/hello.txt") == (
            0,
            "hello from the bucket",
        )
        assert _sh(runtime, sid, f"echo held > /data/{name}")[0] == 0
        assert _http(public_bucket, "GET", name) == (200, b"held\n")
        code, out = _sh(runtime, sid, f"echo x > /ro/{name}")
        assert code != 0 and "Read-only file system" in out
    finally:
        runtime.delete(sid)


def test_mount_s3_ends_with_the_container(runtime, public_bucket):
    """The container ending is what ends mount-s3, whoever ends it: killed
    out of band here, with none of our teardown run, it exits by itself."""
    sid = runtime.create(
        IMAGE,
        shell="/bin/sh",
        readonly=False,
        bucket_mounts=_bucket_mounts(public_bucket),
    )
    meta = runtime.backend._sandbox_metadata[sid]
    processes = [m.process for m in meta["bucket_mounts"]._mounts]
    try:
        subprocess.run(
            runtime.backend._runsc_base_args(meta["config"]) + ["kill", sid, "SIGKILL"],
            capture_output=True,
            timeout=10,
        )
        runtime.backend._delete_container_state(meta["config"], sid)
        assert [p.wait(10) for p in processes] == [0, 0]
        assert not _sandbox_mounts_in_mountinfo()
    finally:
        runtime.delete(sid)


def test_sweep_reclaims_a_mount_whose_owner_died_mid_create(public_bucket):
    """Before its sandbox runs, a bucket is mounted on the node. An owner killed
    in that window leaves it, live, for the sweep."""
    endpoint, bucket = public_bucket
    owner = subprocess.Popen(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                f"""
                import time
                from ray.experimental.sandbox._internal import bucket_mount
                from ray.experimental.sandbox.config import BucketMount
                mounts = bucket_mount.BucketMountSet.allocate("sb-killed", [BucketMount(
                    {bucket!r}, "/data", endpoint_url={endpoint!r}, region="us-east-1",
                    force_path_style=True)])
                mounts.start()
                print(mounts.bind_mounts()[0][1], mounts._mounts[0].process.pid, flush=True)
                time.sleep(600)
                """
            ),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    host_dir, pid = owner.stdout.readline().split()
    pid = int(pid)
    owner.kill()
    owner.wait()
    # Killed outright: its mount and mount-s3 are still there.
    assert bucket_mount.is_mounted(host_dir)
    os.kill(pid, 0)

    bucket_mount.sweep_leaked_mounts()
    assert not bucket_mount.is_mounted(host_dir)
    assert not os.path.exists(os.path.dirname(host_dir))
    _wait_for_exit(pid, "mount-s3 of the killed owner")


def _internet_reaches(url):
    try:
        urllib.request.urlopen(url, timeout=5)
        return True
    except urllib.error.HTTPError:
        return True
    except Exception:
        return False


def test_a_real_public_bucket_with_the_network_off(runtime):
    """The bucket's traffic leaves through the node, as on Modal, so it stays
    readable from a sandbox with no network."""
    if not _internet_reaches("https://noaa-ghcn-pds.s3.amazonaws.com/"):
        pytest.skip("no internet access to AWS Open Data")
    sid = runtime.create(
        IMAGE,
        shell="/bin/sh",
        network="none",
        readonly=False,
        bucket_mounts=[BucketMount("noaa-ghcn-pds", "/noaa")],
    )
    try:
        code, out = _sh(runtime, sid, "head -c 26 /noaa/readme.txt")
        assert code == 0 and out.startswith("README FILE FOR DAILY")
        code, out = _sh(runtime, sid, "echo x > /noaa/x")
        assert code != 0 and "Read-only file system" in out
    finally:
        runtime.delete(sid)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
