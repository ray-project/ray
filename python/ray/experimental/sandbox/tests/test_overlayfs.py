import os
import subprocess
import sys

import pytest

from ray.experimental.sandbox._internal import overlayfs
from ray.experimental.sandbox.exceptions import SandboxCreationError


def _overlay(in_userns):
    return overlayfs.RootfsOverlay(
        lower="/lower",
        mountpoint="/bundle/rootfs",
        tmpfs_dir="/bundle/overlayfs-tmpfs",
        in_userns=in_userns,
    )


def _run_mount_script(tmp_path, in_userns, mount_exit_code=0):
    """Run the mount snippet with fake `mount` and `chown` first on PATH that
    record each argv to `<tool>.args` in ``tmp_path``, and return its result,
    the recorded mount argvs and its directories."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool, exit_code in (("mount", mount_exit_code), ("chown", 0)):
        fake = bin_dir / tool
        fake.write_text(
            f'#!/bin/sh\necho "$@" >> "{tmp_path}/{tool}.args"\nexit {exit_code}\n'
        )
        fake.chmod(0o755)
    lower, tmpfs, mountpoint = (tmp_path / d for d in ("lower", "tmpfs", "mnt"))
    for d in (lower, tmpfs, mountpoint):
        d.mkdir()
    lower.chmod(0o751)
    os.utime(lower, (1_000_000_000, 1_000_000_000))
    overlay = overlayfs.RootfsOverlay(
        lower=str(lower),
        mountpoint=str(mountpoint),
        tmpfs_dir=str(tmpfs),
        in_userns=in_userns,
    )
    res = subprocess.run(
        ["bash", "-c", overlay._mount_script(), "_", *overlay._mount_script_args()],
        env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
    )
    args_file = tmp_path / "mount.args"
    argvs = args_file.read_text().splitlines() if args_file.exists() else []
    return res, argvs, (lower, tmpfs, mountpoint)


@pytest.mark.parametrize("in_userns", [True, False])
def test_mount_script_mounts_tmpfs_then_overlay(tmp_path, in_userns):
    res, argvs, (lower, tmpfs, mountpoint) = _run_mount_script(tmp_path, in_userns)
    assert res.returncode == 0, res.stderr

    options = f"lowerdir={lower},upperdir={tmpfs}/upper,workdir={tmpfs}/work"
    if in_userns:
        options += ",userxattr"
    assert argvs == [
        f"-t tmpfs -o size={overlayfs.TMPFS_SIZE},mode=0755 tmpfs {tmpfs}",
        f"-t overlay overlay -o {options} {mountpoint}",
    ]
    # The overlay's root takes the unpacked tree's mode and timestamps, and
    # its owner too outside a user namespace.
    upper = os.stat(tmpfs / "upper")
    assert upper.st_mode & 0o777 == 0o751
    assert upper.st_mtime == 1_000_000_000
    assert (tmp_path / "chown.args").exists() is not in_userns
    assert (tmpfs / "work").is_dir()


def test_mount_script_reports_a_failed_mount(tmp_path):
    res, _, _ = _run_mount_script(tmp_path, in_userns=True, mount_exit_code=32)
    assert res.returncode == 1
    assert "rootfs overlay mount failed" in res.stderr


@pytest.mark.parametrize("in_userns", [True, False])
def test_wrap_mounts_the_overlay_before_running_argv(in_userns):
    overlay = _overlay(in_userns)
    userns = ["--user", "--map-root-user"] if in_userns else []
    assert overlay.wrap(["runsc", "run"]) == [
        "unshare",
        *userns,
        "--mount",
        "--",
        "bash",
        "-c",
        f'{overlay._mount_script()} && shift 3 && exec "$@"',
        "_",
        "/lower",
        "/bundle/overlayfs-tmpfs",
        "/bundle/rootfs",
        "runsc",
        "run",
    ]


@pytest.mark.parametrize("mode", list(overlayfs.MountMode))
def test_prepare_creates_the_overlay_dirs(tmp_path, mode):
    mountpoint = tmp_path / "rootfs"
    overlay = overlayfs.prepare(
        bundle_dir=str(tmp_path),
        lower="/lower",
        mountpoint=str(mountpoint),
        mount_mode=mode,
    )
    assert overlay.tmpfs_dir == str(tmp_path / "overlayfs-tmpfs")
    assert os.path.isdir(overlay.tmpfs_dir)
    assert mountpoint.is_dir()
    assert overlay.in_userns is (mode is overlayfs.MountMode.USERNS)


def test_prepare_rejects_separators_in_the_lower_layer(tmp_path):
    with pytest.raises(SandboxCreationError, match="separators"):
        overlayfs.prepare(
            bundle_dir=str(tmp_path),
            lower="/cache/a,b/root",
            mountpoint=str(tmp_path / "rootfs"),
            mount_mode=overlayfs.MountMode.USERNS,
        )


@pytest.mark.parametrize(
    "uid_map, initial",
    [
        ("         0          0 4294967295\n", True),
        ("         0       1000          1\n", False),
        ("         0          0      65536\n", False),
    ],
)
def test_is_initial_userns(uid_map, initial):
    """Root in a nested user namespace can unshare a mount namespace too, but
    only the initial user namespace's root has real mount privilege."""
    assert overlayfs._is_initial_userns(uid_map) is initial


@pytest.mark.parametrize(
    "privileged, userns, expected",
    [
        (True, False, overlayfs.MountMode.PRIVILEGED),
        (True, True, overlayfs.MountMode.PRIVILEGED),
        (False, True, overlayfs.MountMode.USERNS),
    ],
)
def test_mount_mode_picks_the_mount_mode(monkeypatch, privileged, userns, expected):
    monkeypatch.setattr("shutil.which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(overlayfs, "has_mount_privilege", lambda: privileged)
    monkeypatch.setattr(overlayfs, "can_mount_in_userns", lambda: userns)
    assert overlayfs.mount_mode() is expected


@pytest.mark.parametrize(
    "probe", [overlayfs.has_mount_privilege, overlayfs.can_mount_in_userns]
)
def test_probe_that_hangs_raises_and_probes_again(monkeypatch, probe):
    """A probe that times out raises rather than caching an answer, so the
    next call probes again."""
    monkeypatch.setattr(overlayfs, "_is_initial_userns", lambda uid_map: True)

    def hang(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, 30)

    probe.cache_clear()
    try:
        monkeypatch.setattr(subprocess, "run", hang)
        with pytest.raises(SandboxCreationError, match="Timed out checking"):
            probe()

        monkeypatch.setattr(
            subprocess,
            "run",
            lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0),
        )
        assert probe() is True
    finally:
        probe.cache_clear()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
