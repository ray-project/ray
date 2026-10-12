"""Unit tests for bucket mounts: config rules, the mount-s3 command line, and the
mount lifecycle, driven by fake ``mount-s3`` and ``fusermount`` scripts so no
FUSE device, network, or gVisor is needed."""

import json
import logging
import os
import signal
import subprocess
import sys
import time
import types

import pytest

from ray.experimental.sandbox._internal import bucket_mount, teardown
from ray.experimental.sandbox.config import BucketMount, SandboxConfig
from ray.experimental.sandbox.exceptions import SandboxCreationError

# -- BucketMount and SandboxConfig rules -------------------------------------


@pytest.mark.parametrize(
    "path", ["/data", "/mnt/bucket", "/tmp/x", "/root", "/root/x", "/a b", "/etcx"]
)
def test_bucket_mount_accepts_path(path):
    assert BucketMount("bucket", path).mount_path == path


@pytest.mark.parametrize(
    "path",
    [
        "data",  # relative
        "/data/",  # not canonical
        "/a/../b",
        "/a/./b",
        "//data",
        "/",
        "/tmp",
        # Runtime-managed paths, below and above.
        "/proc",
        "/proc/x",
        "/dev/shm",
        "/sys",
        "/etc",
        "/etc/hosts",
        "/etc/resolv.conf",
    ],
)
def test_bucket_mount_refuses_path(path):
    with pytest.raises(ValueError):
        BucketMount("bucket", path)


@pytest.mark.parametrize("name", ["", "-flag", "a/b", "a b", "a\tb"])
def test_bucket_mount_refuses_bucket_name(name):
    with pytest.raises(ValueError, match="bucket name"):
        BucketMount(name, "/data")


@pytest.mark.parametrize("region", ["", "-x", "us east"])
def test_bucket_mount_refuses_region(region):
    with pytest.raises(ValueError, match="region"):
        BucketMount("bucket", "/data", region=region)


def test_bucket_mount_key_prefix_must_end_in_slash():
    with pytest.raises(ValueError, match="must end in a '/'"):
        BucketMount("bucket", "/data", key_prefix="dir")
    assert BucketMount("bucket", "/data", key_prefix="dir/").key_prefix == "dir/"
    # Empty means no prefix, as in Modal.
    assert BucketMount("bucket", "/data", key_prefix="").key_prefix == ""


@pytest.mark.parametrize(
    "url", ["storage.googleapis.com", "ftp://host", "https://", "http:///x"]
)
def test_bucket_mount_refuses_endpoint(url):
    with pytest.raises(ValueError, match="endpoint_url"):
        BucketMount("bucket", "/data", endpoint_url=url)


def test_bucket_mount_defaults_to_read_only():
    assert BucketMount("bucket", "/data").read_only is True


def test_sandbox_config_bucket_mount_rules():
    a = BucketMount("a", "/data")
    assert SandboxConfig(image="busybox", bucket_mounts=[a]).bucket_mounts == [a]
    assert SandboxConfig(image="busybox").bucket_mounts == []
    assert SandboxConfig(image="busybox", bucket_mounts=None).bucket_mounts == []
    with pytest.raises(ValueError, match="share the path"):
        SandboxConfig(image="busybox", bucket_mounts=[a, BucketMount("b", "/data")])
    with pytest.raises(ValueError, match="nested"):
        SandboxConfig(image="busybox", bucket_mounts=[a, BucketMount("b", "/data/x")])
    with pytest.raises(ValueError, match="nested"):
        SandboxConfig(image="busybox", bucket_mounts=[BucketMount("b", "/data/x"), a])
    with pytest.raises(ValueError, match="BucketMount"):
        SandboxConfig(image="busybox", bucket_mounts=[{"bucket_name": "a"}])
    # The same bucket at two separate paths is fine.
    SandboxConfig(image="busybox", bucket_mounts=[a, BucketMount("a", "/data2")])


def test_sandbox_config_refuses_readonly_workdir_inside_a_bucket():
    a = BucketMount("a", "/data")
    with pytest.raises(ValueError, match="workdir"):
        SandboxConfig(
            image="busybox", workdir="/data/w", readonly=True, bucket_mounts=[a]
        )
    # On a writable rootfs the workdir is just the cwd, so it may be in a bucket.
    SandboxConfig(image="busybox", workdir="/data/w", readonly=False, bucket_mounts=[a])
    SandboxConfig(image="busybox", workdir="/work", readonly=True, bucket_mounts=[a])


# -- The mount-s3 command line ------------------------------------------------


def _argv(mount, **kwargs):
    return bucket_mount.build_mount_command(
        mount, "/mnt/0", "/mnt/0-home", "/mnt/0-cache", **kwargs
    )


def test_mount_command_fixed_flags_and_positionals():
    argv, env = _argv(BucketMount("noaa-ghcn-pds", "/data"))
    assert argv[0] == "mount-s3"
    for flag in (
        "--foreground",
        "--no-sign-request",
        "--dir-mode=0755",
        "--file-mode=0644",
        "--memory-target=512",
        "--no-log",
        # Modal's caching, with its defaults: 60 s for metadata and for names
        # that do not exist, so neither TTL is overridden.
        "--cache=/mnt/0-cache",
        "--max-cache-size=1024",
        "--read-only",
    ):
        assert flag in argv
    assert not any(
        a.startswith(("--metadata-ttl", "--negative-metadata-ttl")) for a in argv
    )
    assert argv[-3:] == ["--", "noaa-ghcn-pds", "/mnt/0"]
    # Never anything that widens access or depends on a helper the node may
    # lack.
    assert not any(
        a.startswith(("--allow-other", "--allow-root", "--auto-unmount")) for a in argv
    )
    assert "--allow-delete" not in argv


@pytest.mark.parametrize(
    "memory,bucket_mounts,reserved",
    [
        (None, [], None),
        (None, [BucketMount("a", "/a"), BucketMount("a", "/b")], 1 << 30),
        ("1Gi", [BucketMount("a", "/a")], (1 << 30) + (512 << 20)),
    ],
)
def test_create_reserves_each_buckets_memory(
    monkeypatch, memory, bucket_mounts, reserved
):
    """mount-s3 runs beside the sandbox, outside its memory limit, and may use
    up to its 512 MiB target: Ray reserves that per mount, on top of the
    request."""
    import ray.experimental.sandbox as sandbox_pkg

    asked = {}

    class _Sandbox:
        @staticmethod
        def options(**options):
            asked.update(options)
            return types.SimpleNamespace(remote=lambda **kwargs: None)

    monkeypatch.setattr(sandbox_pkg, "Sandbox", _Sandbox)
    sandbox_pkg.create("busybox:latest", memory=memory, bucket_mounts=bucket_mounts)
    assert asked.get("memory") == reserved
    assert bucket_mount.memory_reservation_bytes(bucket_mounts) == len(
        bucket_mounts
    ) * (512 << 20)


def test_mount_command_writable_allows_delete_and_overwrite():
    argv, _ = _argv(BucketMount("b", "/data", read_only=False))
    assert "--read-only" not in argv
    assert "--allow-delete" in argv and "--allow-overwrite" in argv


def test_mount_command_is_the_same_for_every_caller_uid(monkeypatch):
    mount = BucketMount("b", "/data", read_only=False)
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    as_root = _argv(mount)
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    assert _argv(mount) == as_root


def test_mount_command_env_carries_no_credentials(monkeypatch):
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
    ):
        monkeypatch.setenv(key, "secret")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy:3128")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/opt/fuse/lib")
    monkeypatch.setenv("SOME_OTHER", "x")
    _, env = _argv(BucketMount("b", "/data"))
    assert not [
        k for k in env if k.startswith("AWS_") and k != "AWS_EC2_METADATA_DISABLED"
    ]
    assert env["AWS_EC2_METADATA_DISABLED"] == "true"
    assert env["HOME"] == "/mnt/0-home"
    assert env["HTTPS_PROXY"] == "http://proxy:3128"
    assert env["LD_LIBRARY_PATH"] == "/opt/fuse/lib"
    assert "SOME_OTHER" not in env
    assert "PATH" in env


@pytest.mark.parametrize(
    "endpoint,region,expect_region,expect_checksums_off",
    [
        (None, None, None, False),
        (None, "eu-west-1", "eu-west-1", False),
        ("https://s3.us-west-2.amazonaws.com", None, None, False),
        ("https://acct.r2.cloudflarestorage.com", None, "auto", True),
        ("https://acct.r2.cloudflarestorage.com", "weur", "weur", True),
        ("https://storage.googleapis.com", None, None, True),
        ("http://127.0.0.1:9000", "us-east-1", "us-east-1", True),
    ],
)
def test_mount_command_provider_handling(
    endpoint, region, expect_region, expect_checksums_off
):
    argv, _ = _argv(BucketMount("b", "/data", endpoint_url=endpoint, region=region))
    regions = [a for a in argv if a.startswith("--region=")]
    assert regions == ([f"--region={expect_region}"] if expect_region else [])
    assert ("--upload-checksums=off" in argv) == expect_checksums_off
    assert ([f"--endpoint-url={endpoint}"] if endpoint else []) == [
        a for a in argv if a.startswith("--endpoint-url=")
    ]


def test_mount_command_prefix_and_path_style():
    argv, _ = _argv(BucketMount("b", "/data", key_prefix="dir/", force_path_style=True))
    assert "--prefix=dir/" in argv and "--force-path-style" in argv
    argv, _ = _argv(BucketMount("b", "/data", key_prefix=""))
    assert not any(a.startswith("--prefix") for a in argv)


# -- Mount table parsing -------------------------------------------------------


def test_is_mounted_reads_mountinfo_with_escapes(tmp_path):
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        "36 35 0:30 / /mnt/plain rw,relatime - tmpfs tmpfs rw\n"
        "37 35 0:31 / /mnt/with\\040space rw - fuse mountpoint-s3 rw\n"
    )
    assert bucket_mount.is_mounted("/mnt/plain", str(mountinfo))
    assert bucket_mount.is_mounted("/mnt/with space", str(mountinfo))
    assert not bucket_mount.is_mounted("/mnt/other", str(mountinfo))


# -- Prerequisites -------------------------------------------------------------


@pytest.fixture
def all_prerequisites(monkeypatch):
    monkeypatch.setattr(bucket_mount.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(bucket_mount, "_mount_s3_version", lambda: (1, 24, 0))
    monkeypatch.setattr(bucket_mount.os, "access", lambda path, mode: True)
    monkeypatch.setattr(bucket_mount, "_no_new_privs", lambda: False)
    return monkeypatch


def test_prerequisites_met(all_prerequisites):
    bucket_mount.check_prerequisites()


@pytest.mark.parametrize(
    "patch,match",
    [
        (
            lambda m: m.setattr(
                bucket_mount.shutil, "which", lambda n: None if n == "mount-s3" else n
            ),
            "mount-s3",
        ),
        (
            lambda m: m.setattr(bucket_mount, "_mount_s3_version", lambda: (1, 23, 9)),
            "1.24.0 or newer",
        ),
        (
            lambda m: m.setattr(bucket_mount, "_mount_s3_version", lambda: None),
            "unknown version",
        ),
        (
            lambda m: m.setattr(bucket_mount.os, "access", lambda p, mode: False),
            "/dev/fuse",
        ),
        (
            lambda m: m.setattr(
                bucket_mount.shutil, "which", lambda n: None if "fusermount" in n else n
            ),
            "fusermount",
        ),
    ],
)
def test_prerequisite_failures(all_prerequisites, patch, match):
    patch(all_prerequisites)
    with pytest.raises(SandboxCreationError, match=match):
        bucket_mount.check_prerequisites()


@pytest.mark.parametrize("euid", [1000, 0])
def test_fusermount3_alone_can_only_unmount(all_prerequisites, euid):
    """mount-s3's libfuse 2 mounts through ``fusermount`` unless it is root."""
    all_prerequisites.setattr(
        bucket_mount.shutil,
        "which",
        lambda name: None if name == "fusermount" else f"/usr/bin/{name}",
    )
    all_prerequisites.setattr(bucket_mount.os, "geteuid", lambda: euid)
    if euid == 0:
        bucket_mount.check_prerequisites()
    else:
        with pytest.raises(SandboxCreationError, match="'fusermount3' does not"):
            bucket_mount.check_prerequisites()


@pytest.mark.parametrize("euid", [1000, 0])
def test_no_new_privs_blocks_only_a_non_root_mount(all_prerequisites, euid):
    """A hardened pod (allowPrivilegeEscalation: false) stops the setuid
    fusermount a non-root mount needs; root mounts without it."""
    all_prerequisites.setattr(bucket_mount, "_no_new_privs", lambda: True)
    all_prerequisites.setattr(bucket_mount.os, "geteuid", lambda: euid)
    if euid == 0:
        bucket_mount.check_prerequisites()
    else:
        with pytest.raises(SandboxCreationError, match="allowPrivilegeEscalation"):
            bucket_mount.check_prerequisites()


def test_no_new_privs_is_read_from_the_process():
    flag = subprocess.run(
        [
            "setpriv",
            "--no-new-privs",
            sys.executable,
            "-c",
            "from ray.experimental.sandbox._internal import bucket_mount; "
            "print(bucket_mount._no_new_privs())",
        ],
        capture_output=True,
        text=True,
    )
    if flag.returncode != 0 and "setpriv" in flag.stderr:
        pytest.skip("setpriv is not available")
    assert flag.stdout.strip() == "True"
    assert bucket_mount._no_new_privs() is (
        "NoNewPrivs:\t1" in open("/proc/self/status").read()
    )


def test_mount_s3_version_is_asked_once_per_binary(tmp_path, monkeypatch):
    binary = tmp_path / "mount-s3"
    calls = tmp_path / "calls"

    def install(version):
        binary.write_text(f'#!/bin/sh\necho x >> {calls}\necho "mount-s3 {version}"\n')
        binary.chmod(0o755)

    install("1.24.0")
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ.get('PATH', '')}")
    bucket_mount._cached_mount_s3_version.cache_clear()
    try:
        assert bucket_mount._mount_s3_version() == (1, 24, 0)
        assert bucket_mount._mount_s3_version() == (1, 24, 0)
        assert len(calls.read_text().splitlines()) == 1
        # Upgraded in place: a different file, asked again.
        install("1.25.10")
        assert bucket_mount._mount_s3_version() == (1, 25, 10)
        assert len(calls.read_text().splitlines()) == 2
    finally:
        bucket_mount._cached_mount_s3_version.cache_clear()


# -- Lifecycle, with fake mount-s3 and fusermount ------------------------------

# A fake mount-s3: "mounts" by creating <mountpoint>.mounted, then serves until
# nothing references the filesystem any more -- neither the node's mountpoint
# (that marker) nor a container (a <mountpoint>.held marker a test creates) --
# and exits 0, like the real one. Its argv is recorded beside it. A bucket named "missing" fails like mount-s3 does, one
# named "hang" never mounts, and one named "late" mounts after 3 seconds. Like
# the real one, it keeps blocks under <cache>/mountpoint-cache while serving
# and empties that when it exits cleanly; killed, it leaves them.
_FAKE_MOUNT_S3 = r"""#!/bin/sh
for a; do mp=$a; done
prev=
for a; do [ "$prev" = "--" ] && bucket=$a; prev=$a; done
for a; do case "$a" in --cache=*) cache=${a#--cache=};; esac; done
echo "$@" >> "$(dirname "$0")/mount-s3.calls"
case "$bucket" in
  missing) printf 'Error: Failed to create S3 client\n\nCaused by:\n    0: The bucket does not exist\n' >&2; exit 1;;
  hang) exec sleep 1000;;
  late) sleep 3;;
esac
mkdir -p "$cache/mountpoint-cache/V2/ab" && echo block > "$cache/mountpoint-cache/V2/ab/0"
touch "$mp.mounted"
while [ -e "$mp.mounted" ] || [ -e "$mp.held" ]; do sleep 0.02; done
rm -rf "$cache/mountpoint-cache"
exit 0
"""

# A fake fusermount: -u removes the marker, unless <mountpoint>.busy exists.
_FAKE_FUSERMOUNT = r"""#!/bin/sh
for a; do mp=$a; done
[ -e "$mp.busy" ] && exit 1
rm -f "$mp.mounted"
"""


@pytest.fixture
def fake_mounts(tmp_path, monkeypatch):
    """Fake binaries first on PATH, MOUNT_ROOT in tmp, and a mount table that
    reports every ``<dir>.mounted`` marker as a mount."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("mount-s3", _FAKE_MOUNT_S3), ("fusermount", _FAKE_FUSERMOUNT)):
        script = bin_dir / name
        script.write_text(body)
        script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ.get('PATH', '')}")
    root = tmp_path / "mounts"
    monkeypatch.setattr(bucket_mount, "MOUNT_ROOT", str(root))

    def mount_points(mountinfo="/proc/self/mountinfo"):
        points = set()
        for dirpath, _, files in os.walk(root):
            points.update(
                os.path.join(dirpath, f[: -len(".mounted")])
                for f in files
                if f.endswith(".mounted")
            )
        return points

    monkeypatch.setattr(teardown, "_mount_points", mount_points)
    monkeypatch.setattr(teardown, "_unmount_binary", lambda: "fusermount")
    monkeypatch.setattr(bucket_mount, "_MOUNT_READY_TIMEOUT_S", 2.0)
    monkeypatch.setattr(bucket_mount, "_EXIT_GRACE_S", 2.0)
    return root, bin_dir


def _calls(bin_dir):
    path = bin_dir / "mount-s3.calls"
    return path.read_text().splitlines() if path.exists() else []


def test_mount_set_starts_and_stops_cleanly(fake_mounts):
    root, bin_dir = fake_mounts
    mounts = bucket_mount.BucketMountSet.allocate(
        "sb-1", [BucketMount("a", "/data"), BucketMount("a", "/data2", read_only=False)]
    )
    binds = mounts.bind_mounts()
    assert [(dest, ro) for dest, _, ro in binds] == [("/data", True), ("/data2", False)]
    (parent,) = os.listdir(root)
    assert parent.startswith(f"{os.getpid()}-sb-1-")
    assert oct(os.stat(root / parent).st_mode & 0o777) == "0o700"
    # Mountpoints live outside the sandbox tree, never under /tmp/ray/sandbox.
    assert all(not source.startswith("/tmp/ray/sandbox/") for _, source, _ in binds)

    mounts.start()
    # One process per path, even for the same bucket.
    assert len(_calls(bin_dir)) == 2
    assert all(bucket_mount.is_mounted(source) for _, source, _ in binds)
    processes = [m.process for m in mounts._mounts]

    mounts.stop()
    assert all(p.poll() == 0 for p in processes), "exited by itself after the unmount"
    assert os.listdir(root) == []
    mounts.stop()  # idempotent


def test_mount_failure_reports_mount_s3_reason_and_cleans_up(fake_mounts):
    root, _ = fake_mounts
    mounts = bucket_mount.BucketMountSet.allocate(
        "sb-2", [BucketMount("a", "/data"), BucketMount("missing", "/gone")]
    )
    with pytest.raises(SandboxCreationError) as err:
        mounts.start()
    message = str(err.value)
    assert "'missing' at /gone" in message
    assert "The bucket does not exist" in message
    assert os.listdir(root) == []
    assert all(m.process.poll() is not None for m in mounts._mounts)


def test_mount_timeout_cleans_up(fake_mounts):
    root, _ = fake_mounts
    mounts = bucket_mount.BucketMountSet.allocate(
        "sb-3", [BucketMount("hang", "/data")]
    )
    with pytest.raises(SandboxCreationError, match="Timed out"):
        mounts.start()
    assert (
        mounts._mounts[0].process.poll() is not None
    ), "signalled after the grace period"
    assert os.listdir(root) == []


def test_a_mount_that_comes_up_while_stopping_is_unmounted(fake_mounts, caplog):
    """It timed out, then mounted during the grace period before it was
    signalled: the mount is attached and its server gone."""
    root, _ = fake_mounts
    mounts = bucket_mount.BucketMountSet.allocate(
        "sb-late", [BucketMount("late", "/data")]
    )
    with caplog.at_level(logging.WARNING), pytest.raises(
        SandboxCreationError, match="Timed out"
    ):
        mounts.start()
    assert "still mounted" not in caplog.text
    assert os.listdir(root) == []


def test_allocate_cleans_up_when_it_fails_part_way(fake_mounts, monkeypatch):
    root, _ = fake_mounts
    real_mkdir = os.mkdir
    made = []

    def mkdir(path, *args):
        if len(made) == 3:
            raise OSError(28, "No space left on device")
        made.append(path)
        return real_mkdir(path, *args)

    monkeypatch.setattr(bucket_mount.os, "mkdir", mkdir)
    with pytest.raises(SandboxCreationError, match="No space left"):
        bucket_mount.BucketMountSet.allocate(
            "sb-fail", [BucketMount("a", "/data"), BucketMount("a", "/data2")]
        )
    assert os.listdir(root) == []


def test_detach_leaves_the_bucket_to_the_container(fake_mounts, caplog):
    """After detach() the node has no mount, yet mount-s3 keeps serving while a
    container holds the filesystem, and exits by itself -- quietly -- once that
    container is gone."""
    root, _ = fake_mounts
    mounts = bucket_mount.BucketMountSet.allocate(
        "sb-d", [BucketMount("a", "/data"), BucketMount("a", "/ro")]
    )
    mounts.start()
    host_dirs = [source for _, source, _ in mounts.bind_mounts()]
    for d in host_dirs:
        open(d + ".held", "w").close()  # the running container's references

    mounts.detach()
    assert not any(bucket_mount.is_mounted(d) for d in host_dirs)
    time.sleep(0.2)
    assert all(m.process.poll() is None for m in mounts._mounts), "still serving"

    with caplog.at_level(logging.WARNING):
        for d in host_dirs:
            os.unlink(d + ".held")  # the sandbox ends, however it ends
        for m in mounts._mounts:
            assert m.process.wait(5) == 0
        time.sleep(0.1)
    assert "while the sandbox was running" not in caplog.text

    mounts.stop()
    assert os.listdir(root) == []


def test_detach_never_raises(fake_mounts, caplog):
    """A mountpoint that cannot be detached stays attached, and stop() still
    releases it."""
    root, _ = fake_mounts
    mounts = bucket_mount.BucketMountSet.allocate("sb-e", [BucketMount("a", "/data")])
    mounts.start()
    host_dir = mounts.bind_mounts()[0][1]
    open(host_dir + ".busy", "w").close()
    with caplog.at_level(logging.WARNING):
        mounts.detach()
    assert bucket_mount.is_mounted(host_dir)
    assert "Could not unmount" in caplog.text
    os.unlink(host_dir + ".busy")
    mounts.stop()
    assert os.listdir(root) == []


def test_a_killed_mount_s3s_cache_is_removed(fake_mounts):
    """A clean exit empties the cache; a killed mount-s3 leaves its blocks,
    which go with the mount's directory."""
    root, _ = fake_mounts
    mounts = bucket_mount.BucketMountSet.allocate("sb-c", [BucketMount("a", "/data")])
    mounts.start()
    mount = mounts._mounts[0]
    assert oct(os.stat(mount.cache_dir).st_mode & 0o777) == "0o700"
    assert os.listdir(os.path.join(mount.cache_dir, "mountpoint-cache"))
    os.killpg(mount.process.pid, signal.SIGKILL)
    mount.process.wait(5)
    assert os.path.exists(os.path.join(mount.cache_dir, "mountpoint-cache"))
    mounts.stop()
    assert os.listdir(root) == []


def test_a_cache_with_a_mount_under_it_is_left_in_place(fake_mounts, caplog):
    """The cache is the one tree removed recursively, so it gets the mount
    check every recursive removal here does."""
    root, _ = fake_mounts
    mounts = bucket_mount.BucketMountSet.allocate("sb-cm", [BucketMount("a", "/data")])
    mounts.start()
    inner = os.path.join(mounts._mounts[0].cache_dir, "inner")
    os.mkdir(inner)
    open(inner + ".mounted", "w").close()
    with caplog.at_level(logging.WARNING):
        mounts.stop()
    assert os.path.isdir(inner)
    assert "something is mounted under it" in caplog.text
    os.unlink(inner + ".mounted")
    teardown._rmtree_unless_mounted(mounts._mounts[0].cache_dir)
    teardown._remove_mount_files(mounts._parent_dir, 1)
    assert os.listdir(root) == []


def test_still_mounted_directory_is_left_in_place(fake_mounts, caplog):
    root, _ = fake_mounts
    mounts = bucket_mount.BucketMountSet.allocate("sb-4", [BucketMount("a", "/data")])
    mounts.start()
    host_dir = mounts.bind_mounts()[0][1]
    open(host_dir + ".busy", "w").close()
    with caplog.at_level(logging.WARNING):
        mounts.stop()
    assert os.path.isdir(host_dir), "never removed while mounted"
    assert "still mounted" in caplog.text
    # Release it so the fake process exits.
    os.unlink(host_dir + ".busy")
    os.unlink(host_dir + ".mounted")


@pytest.mark.parametrize("detached", [False, True])
def test_watchdog_warns_when_mount_s3_dies(fake_mounts, caplog, detached):
    """A crash is reported whether or not the mount was detached; only a clean
    exit after a detach is the sandbox ending."""
    mounts = bucket_mount.BucketMountSet.allocate("sb-5", [BucketMount("a", "/data")])
    mounts.start()
    if detached:
        open(mounts.bind_mounts()[0][1] + ".held", "w").close()
        mounts.detach()
    process = mounts._mounts[0].process
    with caplog.at_level(logging.WARNING):
        os.killpg(process.pid, signal.SIGKILL)
        deadline = time.monotonic() + 5
        while (
            "while the sandbox was running" not in caplog.text
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
    assert "while the sandbox was running" in caplog.text
    assert "'a' at /data" in caplog.text
    mounts.stop()
    if detached:
        os.unlink(mounts.bind_mounts()[0][1] + ".held")


# -- Reclaiming leaked mounts ---------------------------------------------------


def _leaked_set(root, owner):
    """A mount directory as a killed worker leaves it, serving through a real
    (fake) mount-s3 process."""
    mounts = bucket_mount.BucketMountSet.allocate(
        "sb-leak", [BucketMount("a", "/data")]
    )
    mounts.start()
    (parent,) = [p for p in os.listdir(root) if p.startswith(f"{os.getpid()}-sb-leak")]
    with open(os.path.join(root, parent, "owner"), "w") as f:
        json.dump(owner, f)
    return mounts, os.path.join(root, parent)


def _dead_pid():
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


def test_sweep_reclaims_a_dead_owner(fake_mounts):
    root, _ = fake_mounts
    mounts, parent = _leaked_set(
        root,
        {"pid": _dead_pid(), "start_time": 1, "pid_ns": bucket_mount._pid_namespace()},
    )
    process = mounts._mounts[0].process
    bucket_mount.sweep_leaked_mounts()
    assert not os.path.exists(parent)
    assert process.wait(5) is not None


def test_sweep_reclaims_a_reused_pid(fake_mounts):
    root, _ = fake_mounts
    # The pid is alive (this test's parent) but started at another time.
    mounts, parent = _leaked_set(
        root,
        {
            "pid": os.getppid(),
            "start_time": -1,
            "pid_ns": bucket_mount._pid_namespace(),
        },
    )
    bucket_mount.sweep_leaked_mounts()
    assert not os.path.exists(parent)
    mounts._mounts[0].process.wait(5)


@pytest.mark.parametrize("which", ["live owner", "other pid namespace", "self"])
def test_sweep_leaves_mounts_it_must_not_touch(fake_mounts, which):
    root, _ = fake_mounts
    ppid = os.getppid()
    owner = {
        "live owner": {
            "pid": ppid,
            "start_time": bucket_mount._proc_start_time(ppid),
            "pid_ns": bucket_mount._pid_namespace(),
        },
        "other pid namespace": {"pid": _dead_pid(), "start_time": 1, "pid_ns": -1},
        "self": {
            "pid": os.getpid(),
            "start_time": 1,
            "pid_ns": bucket_mount._pid_namespace(),
        },
    }[which]
    mounts, parent = _leaked_set(root, owner)
    bucket_mount.sweep_leaked_mounts()
    assert os.path.isdir(parent)
    assert mounts._mounts[0].process.poll() is None
    mounts.stop()


def test_sweep_leaves_a_live_supervisors_mounts_alone(fake_mounts):
    """The owner is dead, but its sandbox's supervisor is tearing it down:
    that teardown gives mount-s3 time to finish its uploads, which a sweep
    killing it now would not."""
    root, _ = fake_mounts
    ppid = os.getppid()
    mounts, parent = _leaked_set(
        root,
        {
            "pid": _dead_pid(),
            "start_time": 1,
            "pid_ns": bucket_mount._pid_namespace(),
            "supervisor_pid": ppid,
            "supervisor_start_time": bucket_mount._proc_start_time(ppid),
        },
    )
    bucket_mount.sweep_leaked_mounts()
    assert os.path.isdir(parent)
    assert mounts._mounts[0].process.poll() is None
    mounts.stop()


def test_detach_reports_whether_it_detached(fake_mounts):
    root, _ = fake_mounts
    mounts = bucket_mount.BucketMountSet.allocate(
        "sb-r", [BucketMount("a", "/data"), BucketMount("a", "/ro")]
    )
    mounts.start()
    host_dirs = [source for _, source, _ in mounts.bind_mounts()]
    open(host_dirs[1] + ".busy", "w").close()
    assert mounts.detach() is False
    os.unlink(host_dirs[1] + ".busy")
    assert mounts.detach() is True
    mounts.stop()
    assert os.listdir(root) == []


def test_sweep_does_not_kill_an_unrelated_reused_pid(fake_mounts):
    root, _ = fake_mounts
    mounts, parent = _leaked_set(
        root,
        {"pid": _dead_pid(), "start_time": 1, "pid_ns": bucket_mount._pid_namespace()},
    )
    bystander = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        # The recorded mount-s3 pid now belongs to something else.
        with open(os.path.join(parent, "0.pid"), "w") as f:
            f.write(str(bystander.pid))
        bucket_mount.sweep_leaked_mounts()
        assert bystander.poll() is None
    finally:
        bystander.kill()
        mounts._mounts[0].process.wait(5)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
