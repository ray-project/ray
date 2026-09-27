import logging
import os
import shlex
import signal
import subprocess
import sys
from types import SimpleNamespace
from unittest import mock

import pytest

from ray.experimental.sandbox._internal import overlayfs
from ray.experimental.sandbox._internal.overlayfs import (
    ImageMountMode,
    UserNamespaceType,
)
from ray.experimental.sandbox.exceptions import SandboxCreationError

# A worker's subordinate uid and gid ranges, and the maps they give it as
# uid 1000 and gid 1000.
_UIDS = ((100000, 65536),)
_GIDS = ((200000, 65536),)
_ID_MAPS = overlayfs.IdMaps(
    uid_map=((0, 1000, 1), (1, 100000, 65536)),
    gid_map=((0, 1000, 1), (1, 200000, 65536)),
)


def _overlay(userns, image_mount_mode, id_maps=None):
    return overlayfs.RootfsOverlay(
        image="/cache/rootfs.erofs",
        mountpoint="/bundle/rootfs",
        tmpfs_dir="/bundle/overlayfs-tmpfs",
        userns=userns,
        image_mount_mode=image_mount_mode,
        id_maps=id_maps,
    )


def _run_mount_script(
    tmp_path,
    userns,
    image_mount_mode,
    mount_exit_code=0,
    command=("true",),
    **fake_bodies,
):
    """Run the mount snippet around ``command`` with fake `mount`,
    `erofsfuse`, `mountpoint` and `chown` first on PATH that record each argv
    to `<tool>.args` in ``tmp_path``, and return its result, the recorded
    mount and erofsfuse argvs and its directories. Mounting the image gives
    the lower mount point the image root's mode 0751 and an mtime of 1e9.
    ``fake_bodies`` replaces the body of any fake, by tool name."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    image_root = 'chmod 751 "$last"; touch -d @1000000000 "$last"'
    fakes = {
        "mount": (
            f'case "$*" in *"-t erofs"*) {image_root} ;; esac\n'
            f"exit {mount_exit_code}"
        ),
        "erofsfuse": image_root,
        "mountpoint": "exit 0",
        "chown": "exit 0",
        **fake_bodies,
    }
    for tool, body in fakes.items():
        fake = bin_dir / tool
        fake.write_text(
            f'#!/bin/sh\necho "$@" >> "{tmp_path}/{tool}.args"\n'
            'for last in "$@"; do :; done\n'
            f"{body}\n"
        )
        fake.chmod(0o755)
    tmpfs, mountpoint = (tmp_path / d for d in ("tmpfs", "mnt"))
    for d in (tmpfs, mountpoint):
        d.mkdir()
    image = tmp_path / "rootfs.erofs"
    overlay = overlayfs.RootfsOverlay(
        image=str(image),
        mountpoint=str(mountpoint),
        tmpfs_dir=str(tmpfs),
        userns=userns,
        image_mount_mode=image_mount_mode,
        id_maps=None,
    )
    res = subprocess.run(
        overlay._wrap_in_overlay_mount(list(command)),
        env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        timeout=30,
    )

    def recorded(tool):
        args_file = tmp_path / f"{tool}.args"
        return args_file.read_text().splitlines() if args_file.exists() else []

    return res, recorded("mount"), recorded("erofsfuse"), (image, tmpfs, mountpoint)


@pytest.mark.parametrize(
    "userns, image_mount_mode",
    [
        (UserNamespaceType.HOST, ImageMountMode.KERNEL),
        (UserNamespaceType.HOST, ImageMountMode.FUSE),
        (UserNamespaceType.PRIVATE, ImageMountMode.FUSE),
    ],
)
def test_mount_script_mounts_tmpfs_image_then_overlay(
    tmp_path, userns, image_mount_mode
):
    res, mounts, fuse, (image, tmpfs, mountpoint) = _run_mount_script(
        tmp_path, userns, image_mount_mode
    )
    assert res.returncode == 0, res.stderr

    lower = f"{tmpfs}/lower"
    options = f"lowerdir={lower},upperdir={tmpfs}/upper,workdir={tmpfs}/work"
    # Inside a new user namespace created for the sandbox, the overlay keeps
    # its metadata in user xattrs.
    if userns is UserNamespaceType.PRIVATE:
        options += ",userxattr"
    tmpfs_mount = f"-t tmpfs -o size={overlayfs._TMPFS_SIZE},mode=0755 tmpfs {tmpfs}"
    overlay_mount = f"-t overlay overlay -o {options} {mountpoint}"
    if image_mount_mode is ImageMountMode.FUSE:
        assert mounts == [tmpfs_mount, overlay_mount]
        assert fuse == [f"-f --dbglevel=0 -o allow_other {image} {lower}"]
    else:
        loop_mount = f"-t erofs -o ro,loop {image} {lower}"
        assert mounts == [tmpfs_mount, loop_mount, overlay_mount]
        assert fuse == []
    # The overlay's root takes the image root's owner, mode and timestamps.
    upper = os.stat(tmpfs / "upper")
    assert upper.st_mode & 0o777 == 0o751
    assert upper.st_mtime == 1_000_000_000
    assert (tmp_path / "chown.args").read_text().split() == [
        f"--reference={tmpfs}/lower",
        f"{tmpfs}/upper",
    ]
    assert (tmpfs / "work").is_dir()


def _still_running(pid_file) -> bool:
    """Whether the process whose pid is in ``pid_file`` is still running. A
    zombie counts as stopped, since nothing reaps an orphan when pytest runs
    as a container's init."""
    try:
        with open(f"/proc/{int(pid_file.read_text())}/stat") as f:
            state = f.read().rsplit(")", 1)[1].split()[0]
    except FileNotFoundError:
        return False
    return state != "Z"


@pytest.mark.parametrize("overlay_exit_code", [0, 32])
def test_mount_script_stops_erofsfuse_when_it_exits(tmp_path, overlay_exit_code):
    """The script stops erofsfuse when it exits, whether the overlay mount
    fails or the command it runs exits."""
    pid_file = tmp_path / "erofsfuse.pid"
    res, *_ = _run_mount_script(
        tmp_path,
        UserNamespaceType.PRIVATE,
        ImageMountMode.FUSE,
        erofsfuse=f'echo $$ > "{pid_file}"; exec sleep 60',
        mount=(
            f'case "$*" in *"-t overlay"*) exit {overlay_exit_code} ;; esac\n' "exit 0"
        ),
    )
    assert res.returncode == (0 if overlay_exit_code == 0 else 1), res.stderr
    assert not _still_running(pid_file)


def test_mount_script_stops_erofsfuse_when_it_gets_killed(tmp_path):
    """The script stops erofsfuse even when it gets killed, such as when a
    probe that hangs times out."""
    pid_file = tmp_path / "erofsfuse.pid"
    res, *_ = _run_mount_script(
        tmp_path,
        UserNamespaceType.PRIVATE,
        ImageMountMode.FUSE,
        command=["sh", "-c", "kill -KILL $$"],
        erofsfuse=f'echo $$ > "{pid_file}"; exec sleep 60',
    )
    assert res.returncode == -signal.SIGKILL, res.stderr
    assert not _still_running(pid_file)


def test_mount_script_mounts_the_overlay_when_chown_fails(tmp_path):
    """A user namespace that doesn't map the image root's owner can't chown
    the upper layer to it, which leaves its root owned by root instead of
    failing the mount."""
    res, mounts, *_ = _run_mount_script(
        tmp_path, UserNamespaceType.PRIVATE, ImageMountMode.FUSE, chown="exit 1"
    )
    assert res.returncode == 0, res.stderr
    assert mounts[-1].startswith("-t overlay overlay")


def test_mount_script_reports_a_failed_mount(tmp_path):
    res, *_ = _run_mount_script(
        tmp_path, UserNamespaceType.PRIVATE, ImageMountMode.FUSE, mount_exit_code=32
    )
    assert res.returncode == 1
    assert "rootfs overlay mount failed" in res.stderr


@pytest.mark.parametrize("image_mount_mode", list(ImageMountMode))
def test_wrap_mounts_the_overlay_before_running_argv(image_mount_mode):
    overlay = _overlay(UserNamespaceType.HOST, image_mount_mode)
    assert overlay.wrap(["runsc", "run"]) == [
        "unshare",
        "--mount",
        "--",
        *overlay._wrap_in_overlay_mount(["runsc", "run"]),
    ]


def test_wrap_maps_subordinate_ids_before_mounting(monkeypatch):
    """In a new user namespace created for the sandbox, the script runs with
    root mapped to the worker and the rest of its ids to the worker's
    subordinate ids, which only newuidmap and newgidmap can do, so it waits
    on a FIFO until they have."""
    monkeypatch.setattr(os, "getuid", lambda: 1000)
    monkeypatch.setattr(os, "getgid", lambda: 1000)
    overlay = _overlay(UserNamespaceType.PRIVATE, ImageMountMode.FUSE, id_maps=_ID_MAPS)
    argv = overlay.wrap(["runsc", "run"])

    assert argv[:2] == ["bash", "-c"] and len(argv) == 3
    outer = argv[2]
    ready_file = "/bundle/overlayfs-tmpfs.ready"
    assert f"mkfifo {ready_file}" in outer
    assert 'newuidmap "$ns" 0 1000 1 1 100000 65536' in outer
    assert 'newgidmap "$ns" 0 1000 1 1 200000 65536' in outer
    # The command waits on the FIFO, then mounts the overlay and runs argv.
    mount_and_run = overlay._wrap_in_overlay_mount(["runsc", "run"])
    run_when_ready = f"read -r _ <&3; exec env -- {shlex.join(mount_and_run)} 3<&-"
    assert (
        f"unshare --user --mount -- bash -c {shlex.quote(run_when_ready)} "
        f"3<> {ready_file} &"
    ) in outer


def test_wrap_in_private_userns_without_subordinate_ids_maps_only_root(monkeypatch):
    monkeypatch.setattr(os, "getuid", lambda: 1000)
    overlay = _overlay(UserNamespaceType.PRIVATE, ImageMountMode.FUSE)
    assert overlay.wrap(["runsc", "run"]) == [
        "unshare",
        "--user",
        "--map-root-user",
        "--mount",
        "--",
        *overlay._wrap_in_overlay_mount(["runsc", "run"]),
    ]


def test_wrap_in_private_userns_as_root_writes_the_maps_itself(monkeypatch):
    """newuidmap and newgidmap only map ids /etc/subuid and /etc/subgid allow,
    so a worker running as root writes its maps itself."""
    monkeypatch.setattr(os, "getuid", lambda: 0)
    own_ids = overlayfs.IdMaps(
        uid_map=((0, 0, 4294967295),), gid_map=((0, 0, 1000), (2000, 2000, 1000))
    )
    overlay = _overlay(UserNamespaceType.PRIVATE, ImageMountMode.FUSE, id_maps=own_ids)
    argv = overlay.wrap(["runsc", "run"])

    assert argv[:2] == ["bash", "-c"] and len(argv) == 3
    outer = argv[2]
    assert (
        'm=$(printf "%s %s %s\\n" 0 0 4294967295) && '
        'cat <<< "$m" > /proc/$ns/uid_map'
    ) in outer
    assert (
        'm=$(printf "%s %s %s\\n" 0 0 1000 2000 2000 1000) && '
        'cat <<< "$m" > /proc/$ns/gid_map'
    ) in outer
    assert "newuidmap" not in outer and "newgidmap" not in outer


def test_id_maps_for_root_are_its_own_ids(monkeypatch, tmp_path):
    """A worker running as root takes every id range it has, including 0 and
    any gaps between ranges, so it maps each of them to itself."""
    maps = {
        "uid_map": "         0          0 4294967295\n",
        "gid_map": "0 0 1000\n2000 2000 1000\n",
    }
    real_open = open

    def fake_open(path, *args, **kwargs):
        name = os.path.basename(path)
        if os.path.dirname(path) == "/proc/self" and name in maps:
            path = tmp_path / name
            path.write_text(maps[name])
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "getuid", lambda: 0)
    monkeypatch.setattr("builtins.open", fake_open)
    assert overlayfs.IdMaps.detect() == overlayfs.IdMaps(
        uid_map=((0, 0, 4294967295),),
        gid_map=((0, 0, 1000), (2000, 2000, 1000)),
    )


def test_subids_getsubids_lists_every_range(tmp_path, monkeypatch):
    """getsubids prints one "<index>: <username> <start> <count>" line per
    range, and exits nonzero when it finds none."""
    fake = tmp_path / "getsubids"
    fake.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = -g ]; then echo "0: ray 300000 65536"; exit 0; fi\n'
        'if [ "$1" = ray ]; then\n'
        '  echo "0: ray 100000 65536"; echo "1: ray 500000 1000"; exit 0\n'
        "fi\n"
        'echo "Error fetching ranges" >&2; exit 1\n'
    )
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    getsubids = overlayfs.IdMaps._getsubids
    assert getsubids("ray", "uid") == ((100000, 65536), (500000, 1000))
    assert getsubids("ray", "gid") == ((300000, 65536),)
    assert getsubids("nobody", "uid") == ()


def test_subids_getsubids_not_installed(monkeypatch):
    monkeypatch.setenv("PATH", "")
    assert overlayfs.IdMaps._getsubids("ray", "uid") == ()


def test_id_maps_detect_looks_up_again_after_getsubids_hangs(monkeypatch):
    """A getsubids that hangs, such as when SSSD or LDAP stalls, raises rather
    than caching no subordinate ids, so the next call looks them up again."""
    _as_user(monkeypatch)

    def hang(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, 30)

    monkeypatch.setattr(subprocess, "run", hang)
    with pytest.raises(SandboxCreationError, match="Timed out looking up"):
        overlayfs.IdMaps.detect()

    ranges = {"uid": _UIDS, "gid": _GIDS}
    monkeypatch.setattr(
        overlayfs.IdMaps, "_getsubids", lambda username, kind: ranges[kind]
    )
    assert overlayfs.IdMaps.detect() == _ID_MAPS


def _as_user(monkeypatch, getsubids=True):
    """Run the test as a worker running as uid and gid 1000, with or without
    getsubids installed."""
    monkeypatch.setattr(os, "getuid", lambda: 1000)
    monkeypatch.setattr(os, "getgid", lambda: 1000)
    monkeypatch.setattr(
        overlayfs.pwd, "getpwuid", lambda uid: SimpleNamespace(pw_name="worker")
    )
    monkeypatch.setattr(
        overlayfs.shutil,
        "which",
        lambda name: f"/usr/bin/{name}" if getsubids else None,
    )


def test_id_maps_detect_uses_getsubids(monkeypatch):
    """Where getsubids is installed, it finds ranges wherever newuidmap would,
    so detect doesn't read the files."""
    _as_user(monkeypatch)
    ranges = {"uid": _UIDS, "gid": _GIDS}
    monkeypatch.setattr(
        overlayfs.IdMaps, "_getsubids", lambda username, kind: ranges[kind]
    )
    monkeypatch.setattr(overlayfs.IdMaps, "_read_ranges", lambda *args: ())
    assert overlayfs.IdMaps.detect() == _ID_MAPS


def test_id_maps_detect_reads_the_files_without_getsubids(monkeypatch):
    _as_user(monkeypatch, getsubids=False)
    files = {"/etc/subuid": _UIDS, "/etc/subgid": _GIDS}
    monkeypatch.setattr(
        overlayfs.IdMaps, "_read_ranges", lambda path, username, uid: files[path]
    )
    assert overlayfs.IdMaps.detect() == _ID_MAPS


def test_id_maps_detect_finds_none(monkeypatch):
    _as_user(monkeypatch)
    monkeypatch.setattr(overlayfs.IdMaps, "_getsubids", lambda *args, **kw: ())
    monkeypatch.setattr(overlayfs.IdMaps, "_read_ranges", lambda *args: ())
    assert overlayfs.IdMaps.detect() is None


def test_build_idmap_stacks_the_ranges():
    """Ids inside the namespace map to each range in turn, right after the
    one before."""
    ranges = ((100000, 65536), (500000, 1000))
    assert overlayfs.IdMaps._build_idmap(1000, ranges) == (
        (0, 1000, 1),
        (1, 100000, 65536),
        (65537, 500000, 1000),
    )


def test_id_maps_detect_needs_a_user_name(monkeypatch):
    """newuidmap and newgidmap refuse to run for a uid with no user name."""
    _as_user(monkeypatch)

    def no_user(uid):
        raise KeyError(uid)

    monkeypatch.setattr(overlayfs.pwd, "getpwuid", no_user)
    monkeypatch.setattr(overlayfs.IdMaps, "_getsubids", lambda *args: _UIDS)
    monkeypatch.setattr(overlayfs.IdMaps, "_read_ranges", lambda *args: _UIDS)
    assert overlayfs.IdMaps.detect() is None


def test_subids_read_ranges(tmp_path):
    """/etc/subuid and /etc/subgid list a user by name or by uid, on as many
    lines as it has ranges."""
    subuid = tmp_path / "subuid"
    subuid.write_text("other:1:2\n\nray:100000:65536\n1000:5:6\nray:500000:1000\n")
    read = overlayfs.IdMaps._read_ranges
    assert read(str(subuid), "ray", 2000) == ((100000, 65536), (500000, 1000))
    assert read(str(subuid), "app", 1000) == ((5, 6),)
    assert read(str(subuid), "ray", 1000) == ((100000, 65536), (5, 6), (500000, 1000))
    assert read(str(subuid), "app", 2000) == ()
    assert read(str(tmp_path / "missing"), "ray", 1000) == ()


@pytest.mark.parametrize("userns", list(UserNamespaceType))
@pytest.mark.parametrize("image_mount_mode", list(ImageMountMode))
def test_prepare_creates_the_overlay_dirs(
    tmp_path, monkeypatch, userns, image_mount_mode
):
    monkeypatch.setattr(overlayfs.IdMaps, "detect", lambda: _ID_MAPS)
    monkeypatch.setattr(overlayfs.UserNamespaceType, "detect", lambda: userns)
    monkeypatch.setattr(
        overlayfs.ImageMountMode, "detect", lambda userns: image_mount_mode
    )
    mountpoint = tmp_path / "rootfs"
    overlay = overlayfs.prepare(
        image="/cache/rootfs.erofs",
        mountpoint=str(mountpoint),
        bundle_dir=str(tmp_path),
    )
    assert overlay.tmpfs_dir == str(tmp_path / "overlayfs-tmpfs")
    assert os.path.isdir(overlay.tmpfs_dir)
    assert mountpoint.is_dir()
    assert overlay.image == "/cache/rootfs.erofs"
    assert overlay.userns is userns
    assert overlay.image_mount_mode is image_mount_mode
    # Only a new user namespace created for the sandbox maps subordinate ids.
    private = userns is UserNamespaceType.PRIVATE
    assert overlay.id_maps == (_ID_MAPS if private else None)


def test_prepare_rejects_separators_in_the_bundle_path(tmp_path):
    with pytest.raises(SandboxCreationError, match="separators"):
        overlayfs.prepare(
            image="/cache/rootfs.erofs",
            mountpoint=str(tmp_path / "rootfs"),
            bundle_dir=str(tmp_path / "a,b"),
        )


@pytest.mark.parametrize(
    "inode, host", [(overlayfs._HOST_USERNS_INO, True), (4026532203, False)]
)
def test_is_host_userns(inode, host):
    """Only the host's user namespace has the kernel's fixed inode number,
    even where a nested one maps every uid to itself."""
    with mock.patch("os.stat", return_value=SimpleNamespace(st_ino=inode)):
        result = overlayfs._is_host_userns()
    assert result is host


def test_is_host_userns_without_proc():
    with mock.patch("os.stat", side_effect=FileNotFoundError):
        result = overlayfs._is_host_userns()
    assert result is False


def test_is_host_userns_in_a_new_user_namespace(tmp_path):
    """A new user namespace doesn't count as the host's, even with root mapped
    to the worker, or every id mapped to itself for a worker running as
    root."""
    check = (
        "from ray.experimental.sandbox._internal import overlayfs; "
        "print(overlayfs._is_host_userns())"
    )
    overlay = overlayfs.RootfsOverlay(
        image="",
        image_mount_mode=ImageMountMode.FUSE,
        userns=UserNamespaceType.PRIVATE,
        id_maps=overlayfs.IdMaps.detect(),
        mountpoint=str(tmp_path / "mnt"),
        tmpfs_dir=str(tmp_path / "tmpfs"),
    )
    try:
        res = subprocess.run(
            overlay._wrap_in_private_userns([sys.executable, "-c", check]),
            capture_output=True,
            text=True,
            timeout=60,
        )
    except FileNotFoundError:
        pytest.skip("unshare isn't installed")
    if res.returncode != 0:
        pytest.skip(
            f"can't run {sys.executable} in a new user namespace here: "
            f"{res.stderr.strip()}"
        )
    assert res.stdout.strip() == "False"


def test_has_mount_privileges_without_unshare(monkeypatch):
    """Without unshare, a worker has no mount privileges to use."""

    def no_unshare(argv, **kwargs):
        raise FileNotFoundError(argv[0])

    overlayfs._has_mount_privileges.cache_clear()
    try:
        monkeypatch.setattr(subprocess, "run", no_unshare)
        assert overlayfs._has_mount_privileges() is False
    finally:
        overlayfs._has_mount_privileges.cache_clear()


@pytest.mark.parametrize(
    "host, privileged, expected",
    [
        (True, True, UserNamespaceType.HOST),
        (True, False, UserNamespaceType.PRIVATE),
        # Root in a nested user namespace can unshare a mount namespace too.
        (False, True, UserNamespaceType.PRIVATE),
    ],
)
def test_user_namespace_is_host_only_with_host_mount_privileges(
    monkeypatch, host, privileged, expected
):
    """Only a worker in the host's user namespace with mount privileges mounts
    the rootfs there. Any other worker uses a private user namespace."""
    monkeypatch.setattr(overlayfs, "_is_host_userns", lambda: host)
    monkeypatch.setattr(overlayfs, "_has_mount_privileges", lambda: privileged)
    monkeypatch.setattr(overlayfs.IdMaps, "detect", lambda: _ID_MAPS)
    assert overlayfs.UserNamespaceType.detect() is expected


def test_private_user_namespace_without_subordinate_ids_warns(monkeypatch, caplog):
    """A worker without mount privileges or subordinate ids still gets a
    private user namespace, which maps only root, and says so."""
    monkeypatch.setattr(os, "getuid", lambda: 1000)
    monkeypatch.setattr(overlayfs, "_has_mount_privileges", lambda: False)
    monkeypatch.setattr(overlayfs.IdMaps, "detect", lambda: None)
    with caplog.at_level(logging.WARNING, logger=overlayfs.__name__):
        assert overlayfs.UserNamespaceType.detect() is UserNamespaceType.PRIVATE
    assert "only uid 0 and gid 0 inside it map to uid" in caplog.text


def test_private_user_namespace_says_which_ids_it_maps(monkeypatch, caplog):
    """Files owned by ids beyond the mapped ranges show up as owned by nobody,
    so a private user namespace says which ids it maps, however many. It logs
    that at DEBUG, since nothing needs fixing, and Ray workers show INFO."""
    id_maps = overlayfs.IdMaps(
        uid_map=((0, 1000, 1), (1, 100000, 600), (601, 500000, 400)),
        gid_map=((0, 1000, 1), (1, 200000, 65536)),
    )
    monkeypatch.setattr(os, "getuid", lambda: 1000)
    monkeypatch.setattr(overlayfs, "_has_mount_privileges", lambda: False)
    monkeypatch.setattr(overlayfs.IdMaps, "detect", lambda: id_maps)
    with caplog.at_level(logging.DEBUG, logger=overlayfs.__name__):
        assert overlayfs.UserNamespaceType.detect() is UserNamespaceType.PRIVATE
    (record,) = caplog.records
    assert record.levelno == logging.DEBUG
    assert "uids 1 through 1000 and gids 1 through 65536" in record.getMessage()


def test_private_user_namespace_as_root_doesnt_warn(monkeypatch, caplog):
    """A worker running as root without mount privileges maps every id it has
    in its private user namespace, so no file loses its owner."""
    monkeypatch.setattr(os, "getuid", lambda: 0)
    monkeypatch.setattr(overlayfs, "_has_mount_privileges", lambda: False)
    with caplog.at_level(logging.WARNING, logger=overlayfs.__name__):
        assert overlayfs.UserNamespaceType.detect() is UserNamespaceType.PRIVATE
    assert caplog.text == ""


def _stub_probe_mount(monkeypatch, errors):
    """Stub the mount probes. ``errors`` maps each (user namespace, image
    mount) to why its mount fails, and a way it leaves out works."""

    def probe(userns, image_mount_mode):
        error = errors.get((userns, image_mount_mode))
        return error is None, error or ""

    monkeypatch.setattr(overlayfs, "_probe_mount", probe)


@pytest.mark.parametrize(
    "userns, errors, expected",
    [
        (UserNamespaceType.HOST, {}, ImageMountMode.KERNEL),
        # A failed kernel mount falls back to erofsfuse.
        (
            UserNamespaceType.HOST,
            {(UserNamespaceType.HOST, ImageMountMode.KERNEL): "no kernel"},
            ImageMountMode.FUSE,
        ),
        (UserNamespaceType.PRIVATE, {}, ImageMountMode.FUSE),
    ],
)
def test_image_mount_picks_how_to_mount_the_image(
    monkeypatch, userns, errors, expected
):
    _stub_probe_mount(monkeypatch, errors)
    assert overlayfs.ImageMountMode.detect(userns) is expected


def test_image_mount_reports_why_erofsfuse_failed(monkeypatch):
    """A failed mount's stderr says what broke, such as a missing tool."""
    fuse_error = "bash: line 1: erofsfuse: command not found"
    _stub_probe_mount(
        monkeypatch, {(UserNamespaceType.PRIVATE, ImageMountMode.FUSE): fuse_error}
    )
    with pytest.raises(SandboxCreationError, match=fuse_error):
        overlayfs.ImageMountMode.detect(UserNamespaceType.PRIVATE)


def test_image_mount_reports_why_both_mounts_failed(monkeypatch):
    _stub_probe_mount(
        monkeypatch,
        {
            (UserNamespaceType.HOST, ImageMountMode.KERNEL): "no kernel",
            (UserNamespaceType.HOST, ImageMountMode.FUSE): "no fuse",
        },
    )
    with pytest.raises(SandboxCreationError) as err:
        overlayfs.ImageMountMode.detect(UserNamespaceType.HOST)
    assert (
        "driver failed with: no kernel. Mounting with erofsfuse failed with: no fuse"
        in str(err.value)
    )


@pytest.mark.parametrize(
    "probe",
    [
        overlayfs._has_mount_privileges,
        lambda: overlayfs._probe_mount(UserNamespaceType.HOST, ImageMountMode.KERNEL),
        lambda: overlayfs._probe_mount(UserNamespaceType.PRIVATE, ImageMountMode.FUSE),
    ],
)
def test_probe_that_hangs_raises_and_probes_again(monkeypatch, probe):
    """A probe that times out raises rather than caching an answer, so the
    next call probes again."""
    monkeypatch.setattr(overlayfs.IdMaps, "detect", lambda: _ID_MAPS)
    # The mount probes build a tiny EROFS image to mount first.
    monkeypatch.setattr(
        overlayfs.image_utils, "build_erofs_image", lambda *args, **kwargs: None
    )

    def hang(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, 30)

    class HangingPopen:
        """A probe command that runs until it's killed."""

        pid = 0

        def __init__(self, argv, **kwargs):
            self.args = argv

        def wait(self, timeout=None):
            if timeout is not None:
                raise subprocess.TimeoutExpired(self.args, timeout)
            return -signal.SIGKILL

    class ExitingPopen(HangingPopen):
        """A probe command that exits right away."""

        def wait(self, timeout=None):
            return 0

    # The mount probes kill their process group once they're done.
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: None)
    overlayfs._has_mount_privileges.cache_clear()
    overlayfs._probe_mount.cache_clear()
    try:
        monkeypatch.setattr(subprocess, "run", hang)
        monkeypatch.setattr(subprocess, "Popen", HangingPopen)
        with pytest.raises(SandboxCreationError, match="Timed out checking"):
            probe()

        monkeypatch.setattr(
            subprocess,
            "run",
            lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0),
        )
        monkeypatch.setattr(subprocess, "Popen", ExitingPopen)
        # A mount probe reports no error for a mount that works.
        works = True if probe is overlayfs._has_mount_privileges else (True, "")
        assert probe() == works
    finally:
        overlayfs._has_mount_privileges.cache_clear()
        overlayfs._probe_mount.cache_clear()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
