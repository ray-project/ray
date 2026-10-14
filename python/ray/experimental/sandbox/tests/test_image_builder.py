"""The image builder: layer encoding, in-place application, and caching.

Runs the real ``mkfs.erofs`` (1.8+, for ``--incremental``) and ``fsck.erofs``
but no gVisor: a fake backend stands in for build sandboxes, and each step's
commands script the overlay diff it "produces" (see :class:`FakeBackend`).
The end-to-end path through runsc is in test_modal_image_build_integration.
"""

import hashlib
import io
import json
import os
import shutil
import stat
import subprocess
import sys
import tarfile
import threading
import time
import uuid

import pytest

from ray.experimental.sandbox import image_builder as image_builder_module
from ray.experimental.sandbox._internal import image_layers
from ray.experimental.sandbox._internal.erofs_reader import ErofsImage
from ray.experimental.sandbox._internal.image_utils import (
    LAYER_TAR,
    ROOTFS_IMAGE,
    BuiltImageEvictedError,
    evict_least_recently_used_images,
    mkfs_erofs_path,
)
from ray.experimental.sandbox.exceptions import SandboxImageBuildError
from ray.experimental.sandbox.image_build import (
    COPY_MTIME,
    CopyFromImageStep,
    CopyStep,
    ImageBuildPlan,
    RunStep,
    step_key,
)
from ray.experimental.sandbox.image_builder import ImageBuilder
from ray.experimental.sandbox.image_manager import ImageManager
from ray.experimental.sandbox.runtime import SandboxRuntime


@pytest.fixture(autouse=True)
def _needs_incremental_mkfs():
    mkfs_erofs_path.cache_clear()
    image_layers.mkfs_erofs_supports_incremental.cache_clear()
    if not image_layers.mkfs_erofs_supports_incremental():
        pytest.skip("needs mkfs.erofs 1.8+ (erofs-utils) with --incremental")


# -- fixtures ----------------------------------------------------------------


def _tar_add(
    tar,
    name,
    *,
    type=tarfile.REGTYPE,
    data=b"",
    mode=0o644,
    uid=0,
    gid=0,
    linkname="",
    pax=None,
    dev=(0, 0),
):
    info = tarfile.TarInfo(name)
    info.type = type
    info.mode = mode
    info.uid = uid
    info.gid = gid
    info.mtime = 1_700_000_000
    info.linkname = linkname
    info.devmajor, info.devminor = dev
    if pax:
        info.pax_headers = dict(pax)
    if type == tarfile.REGTYPE:
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    else:
        tar.addfile(info)


def _write_base_tar(path):
    """A small root filesystem with a UsrMerge /bin and special modes."""
    with tarfile.open(path, "w", format=tarfile.PAX_FORMAT) as tar:
        d = tarfile.DIRTYPE
        _tar_add(tar, "usr", type=d, mode=0o755)
        _tar_add(tar, "usr/bin", type=d, mode=0o755)
        _tar_add(tar, "usr/bin/tool", data=b"#!x\n", mode=0o4755)
        _tar_add(tar, "bin", type=tarfile.SYMTYPE, linkname="usr/bin", mode=0o777)
        _tar_add(tar, "etc", type=d, mode=0o755)
        _tar_add(tar, "etc/issue", data=b"base\n")
        _tar_add(tar, "root", type=d, mode=0o700)
        _tar_add(tar, "tmp", type=d, mode=0o1777)
        _tar_add(tar, "usr/share", type=d, mode=0o755)
        _tar_add(tar, "usr/share/doc", type=d, mode=0o755)
        _tar_add(tar, "usr/share/doc/old", data=b"old\n")
        _tar_add(tar, "home", type=d, mode=0o755)
        _tar_add(tar, "home/user", type=d, mode=0o750, uid=1000, gid=1000)


@pytest.fixture
def image_manager(tmp_path):
    return ImageManager(images_dir=str(tmp_path / "images"))


@pytest.fixture
def base_tar(tmp_path):
    path = tmp_path / "base.tar"
    _write_base_tar(path)
    return str(path)


def _ls(image_path, path):
    with ErofsImage(image_path) as image:
        st = image.lstat(path)
        if st is None or not st.is_dir:
            return None
        names = {n.decode() for n, _ in image._dirents(image._inode(st.nid))}
    return sorted(names - {".", ".."})


def _read(image_path, path):
    with ErofsImage(image_path) as image:
        st = image.lstat(path)
        if st is None:
            return None
    res = subprocess.run(
        ["dump.erofs", "--cat", f"--path={path}", image_path], capture_output=True
    )
    return res.stdout


class FakeBackend:
    """Stands in for gVisor: a step's commands describe the diff it makes.

    ``put <path> <content>`` writes a file, ``del <path>`` deletes one, and
    ``fail`` exits 1. ``export_rootfs_upper`` writes what gVisor would: the
    root entry, every ancestor directory, one character-device whiteout with
    every further deletion a hard link to it, and opaque new directories.
    """

    def __init__(self, image_manager):
        self.image_manager = image_manager
        self.runs = []
        self.created = []
        self.configs = []
        self.killed = []
        # Called with each command as it starts, from the building thread.
        self.on_exec = None
        # Seconds each delete_sandbox takes, and what happened when.
        self.delete_delay = 0.0
        self.events = []
        self._ops = {}
        self._images = {}
        self._lock = threading.Lock()

    def create_sandbox(self, config):
        sandbox_id = f"fake-{uuid.uuid4().hex[:8]}"
        self.image_manager.pull_image(config.image, instance_id=sandbox_id)
        with self._lock:
            self._ops[sandbox_id] = []
            self._images[sandbox_id] = config.image
            self.created.append((sandbox_id, config.image))
            self.configs.append(config)
        return sandbox_id

    def exec_argv(self, sandbox_id, command, pid_file=None):
        text = command[-1]
        with self._lock:
            self._ops[sandbox_id].append(text)
            self.runs.append(text)
        if self.on_exec is not None:
            self.on_exec(text)
        if text == "fail":
            return ["sh", "-c", "echo running; exit 1"]
        if text.startswith("hang"):
            # Records its pid as runsc would, then outlives its client: the
            # background sleep keeps the output pipe open, as a command left
            # running in a real sandbox does.
            script = "echo $$ > {}; sleep 600 & wait".format(pid_file)
            # Its own process group, as each runsc exec gets in a sandbox.
            return ["setsid", "sh", "-c", script]
        return ["true"]

    def kill_process_group_argv(self, sandbox_id, pid):
        self.killed.append(pid)
        return ["kill", "-KILL", "--", f"-{pid}"]

    def export_rootfs_upper(self, sandbox_id, path):
        whiteout = None
        with tarfile.open(path, "w", format=tarfile.PAX_FORMAT) as tar:
            _tar_add(tar, "./", type=tarfile.DIRTYPE, mode=0o777, uid=1234)
            written = set()
            for op in self._ops[sandbox_id]:
                words = op.split(" ", 2)
                if words[0] not in ("put", "del"):
                    continue
                target = "./" + words[1].lstrip("/")
                parts = target.split("/")[1:-1]
                for i in range(len(parts)):
                    d = "./" + "/".join(parts[: i + 1])
                    if d not in written:
                        written.add(d)
                        _tar_add(
                            tar,
                            d,
                            type=tarfile.DIRTYPE,
                            mode=0o755,
                            pax={"SCHILY.xattr.trusted.overlay.opaque": "y"}
                            if d.startswith("./new")
                            else None,
                        )
                if words[0] == "put":
                    _tar_add(tar, target, data=words[2].encode())
                elif words[0] == "del":
                    if whiteout is None:
                        whiteout = target
                        _tar_add(tar, target, type=tarfile.CHRTYPE, mode=0)
                    else:
                        _tar_add(tar, target, type=tarfile.LNKTYPE, linkname=whiteout)

    def delete_sandbox(self, sandbox_id):
        self.events.append(("delete-start", sandbox_id))
        time.sleep(self.delete_delay)
        self.image_manager.release_image(self._images.pop(sandbox_id), sandbox_id)
        self._ops.pop(sandbox_id, None)
        self.events.append(("delete-end", sandbox_id))


@pytest.fixture
def builder(image_manager):
    return ImageBuilder(image_manager, FakeBackend(image_manager))


def _run(*commands, **kwargs):
    return RunStep(commands=tuple(commands), **kwargs)


def _image_file(builder, ref):
    return os.path.join(builder._image_manager.get_image_dir(ref), ROOTFS_IMAGE)


# -- the EROFS reader --------------------------------------------------------


def test_reader_reports_exact_modes_owners_and_links(image_manager, base_tar):
    image = os.path.join(image_manager.pull_image(base_tar), ROOTFS_IMAGE)
    with ErofsImage(image) as erofs:
        assert erofs.lstat("/tmp").mode == stat.S_IFDIR | 0o1777
        assert erofs.lstat("/usr/bin/tool").mode == stat.S_IFREG | 0o4755
        assert erofs.lstat("/root").mode & 0o7777 == 0o700
        user = erofs.lstat("/home/user")
        assert (user.uid, user.gid, user.mode & 0o7777) == (1000, 1000, 0o750)
        bin_ = erofs.lstat("/bin")
        assert bin_.is_symlink and erofs.readlink(bin_.nid) == "usr/bin"
        assert erofs.lstat("/bin/tool") is None  # lstat follows no symlink
        assert erofs.lstat("/nope") is None


# -- layer encoding ----------------------------------------------------------


def _gvisor_style_diff(path):
    with tarfile.open(path, "w", format=tarfile.PAX_FORMAT) as tar:
        _tar_add(tar, "./", type=tarfile.DIRTYPE, mode=0o777, uid=1234)
        _tar_add(tar, "./etc", type=tarfile.DIRTYPE, mode=0o755)
        _tar_add(tar, "./etc/issue", type=tarfile.CHRTYPE, mode=0)
        _tar_add(tar, "./etc/gone", type=tarfile.LNKTYPE, linkname="./etc/issue")
        _tar_add(
            tar,
            "./etc/new",
            data=b"n",
            pax={
                "SCHILY.xattr.user.keep": "1",
                "SCHILY.xattr.trusted.overlay.origin": "x",
            },
        )
        _tar_add(tar, "./etc/new-link", type=tarfile.LNKTYPE, linkname="./etc/new")
        _tar_add(tar, "./usr", type=tarfile.DIRTYPE, mode=0o755)
        _tar_add(tar, "./usr/share", type=tarfile.DIRTYPE, mode=0o755)
        _tar_add(
            tar,
            "./usr/share/doc",
            type=tarfile.DIRTYPE,
            mode=0o755,
            pax={"SCHILY.xattr.trusted.overlay.opaque": "y"},
        )
        _tar_add(tar, "./usr/share/doc/fresh", data=b"f")


def test_normalize_turns_overlay_encoding_into_an_oci_layer(
    tmp_path, image_manager, base_tar
):
    raw, layer = tmp_path / "raw.tar", tmp_path / "layer.tar"
    _gvisor_style_diff(raw)
    image = os.path.join(image_manager.pull_image(base_tar), ROOTFS_IMAGE)
    with ErofsImage(image) as erofs:
        root = erofs.stat(erofs.root_nid)
    with open(raw, "rb") as src:
        image_layers.normalize_overlay_diff(src, str(layer), root)
    with tarfile.open(layer) as tar:
        members = {m.name: m for m in tar.getmembers()}
    # The root carries the image's own metadata, not the upper layer's.
    assert members["."].uid == 0 and members["."].mode == 0o755
    # Both whiteout forms -- the device and hard links to it -- become .wh.
    assert ".wh.issue" in {os.path.basename(n) for n in members}
    assert "etc/.wh.gone" in members
    assert "usr/share/doc/.wh..wh..opq" in members
    assert (
        members["etc/new-link"].islnk()
        and members["etc/new-link"].linkname == "etc/new"
    )
    xattrs = members["etc/new"].pax_headers
    assert xattrs.get("SCHILY.xattr.user.keep") == "1"
    assert not any("trusted.overlay" in key for key in xattrs)


def test_applying_a_layer_deletes_replaces_and_keeps_metadata(
    tmp_path, image_manager, base_tar
):
    raw, layer = tmp_path / "raw.tar", tmp_path / "layer.tar"
    _gvisor_style_diff(raw)
    work = str(tmp_path / "work.erofs")
    shutil.copyfile(
        os.path.join(image_manager.pull_image(base_tar), ROOTFS_IMAGE), work
    )
    with ErofsImage(work) as erofs:
        root = erofs.stat(erofs.root_nid)
    with open(raw, "rb") as src:
        image_layers.normalize_overlay_diff(src, str(layer), root)
    image_layers.apply_layer_to_erofs(work, str(layer))

    assert subprocess.run(["fsck.erofs", work], capture_output=True).returncode == 0
    assert "issue" not in _ls(work, "/etc") and "new" in _ls(work, "/etc")
    # Opaque: the old contents are hidden, not merged.
    assert _ls(work, "/usr/share/doc") == ["fresh"]
    with ErofsImage(work) as erofs:
        assert erofs.stat(erofs.root_nid).mode & 0o7777 == 0o755
        assert erofs.stat(erofs.root_nid).uid == 0
        assert erofs.lstat("/tmp").mode & 0o7777 == 0o1777
        assert erofs.lstat("/etc/new").nid == erofs.lstat("/etc/new-link").nid


def _copy_archive(path, files):
    with tarfile.open(path, "w", format=tarfile.PAX_FORMAT) as tar:
        for name, data, mode in files:
            _tar_add(tar, name, data=data, mode=mode)


def test_copy_layers_resolve_destinations_against_the_image(
    tmp_path, image_manager, base_tar
):
    work = str(tmp_path / "work.erofs")
    shutil.copyfile(
        os.path.join(image_manager.pull_image(base_tar), ROOTFS_IMAGE), work
    )
    archive, layer = tmp_path / "copy.tar", tmp_path / "layer.tar"
    _copy_archive(
        archive,
        [
            ("bin/added", b"#!/bin/sh\n", 0o755),
            ("tmp/deep/f", b"f", 0o600),
            ("root/app/main.py", b"print()\n", 0o644),
            ("home/user/x", b"x", 0o644),
        ],
    )
    with ErofsImage(work) as erofs:
        image_layers.prepare_copy_layer(erofs, str(archive), str(layer))
    image_layers.apply_layer_to_erofs(work, str(layer))

    with ErofsImage(work) as erofs:
        # Through the UsrMerge symlink, which stays a symlink.
        assert erofs.lstat("/bin").is_symlink
        assert erofs.lstat("/usr/bin/added").mode & 0o7777 == 0o755
        assert "tool" in _ls(work, "/usr/bin")
        # Existing directories keep their metadata; new ones are root 0755.
        assert erofs.lstat("/tmp").mode & 0o7777 == 0o1777
        assert erofs.lstat("/root").mode & 0o7777 == 0o700
        user = erofs.lstat("/home/user")
        assert (user.uid, user.mode & 0o7777) == (1000, 0o750)
        new = erofs.lstat("/tmp/deep")
        assert (new.uid, new.mode & 0o7777) == (0, 0o755)
        assert erofs.stat(erofs.root_nid).mode & 0o7777 == 0o755


def test_copy_layers_refuse_a_file_on_the_way(tmp_path, image_manager, base_tar):
    work = os.path.join(image_manager.pull_image(base_tar), ROOTFS_IMAGE)
    archive = tmp_path / "copy.tar"
    _copy_archive(archive, [("etc/issue/x", b"x", 0o644)])
    with ErofsImage(work) as erofs, pytest.raises(
        SandboxImageBuildError, match="not a directory"
    ):
        image_layers.prepare_copy_layer(
            erofs, str(archive), str(tmp_path / "layer.tar")
        )


# -- the builder -------------------------------------------------------------


def test_a_plan_builds_once_and_then_hits(builder, base_tar):
    plan = ImageBuildPlan((_run("put /a 1"), _run("put /b 2", "del /etc/issue")))
    ref = builder.build(base_tar, plan)
    assert builder._backend.runs == ["put /a 1", "put /b 2", "del /etc/issue"]
    image = _image_file(builder, ref)
    assert _read(image, "/a") == b"1" and _read(image, "/b") == b"2"
    assert "issue" not in _ls(image, "/etc")

    assert builder.build(base_tar, plan) == ref
    assert len(builder._backend.runs) == 3


def test_images_sharing_a_prefix_run_only_their_suffix(builder, base_tar):
    builder.build(base_tar, ImageBuildPlan((_run("put /a 1"), _run("put /b 2"))))
    ref = builder.build(base_tar, ImageBuildPlan((_run("put /a 1"), _run("put /c 3"))))
    assert builder._backend.runs == ["put /a 1", "put /b 2", "put /c 3"]
    image = _image_file(builder, ref)
    assert _read(image, "/a") == b"1" and _read(image, "/c") == b"3"
    assert _read(image, "/b") is None


def test_a_cached_prefix_image_is_the_starting_point(builder, base_tar):
    builder.build(base_tar, ImageBuildPlan((_run("put /a 1"),)))
    ref = builder.build(base_tar, ImageBuildPlan((_run("put /a 1"), _run("put /b 2"))))
    assert builder._backend.runs == ["put /a 1", "put /b 2"]
    assert _read(_image_file(builder, ref), "/a") == b"1"


def test_an_evicted_leaf_is_rebuilt_from_cached_layers_without_running(
    builder, base_tar
):
    plan = ImageBuildPlan((_run("put /a 1"), _run("put /b 2")))
    ref = builder.build(base_tar, plan)
    os.remove(_image_file(builder, ref))  # the image goes; its diff stays
    ref2 = builder.build(base_tar, plan)
    assert ref2 == ref and len(builder._backend.runs) == 2
    assert _read(_image_file(builder, ref), "/b") == b"2"


def test_an_evicted_intermediate_is_rerun_and_rebuilds_later_layers(builder, base_tar):
    plan = ImageBuildPlan((_run("put /a 1"), _run("put /b 2")))
    ref = builder.build(base_tar, plan)
    chain = builder._resolve_chain(base_tar, plan)
    shutil.rmtree(builder._entry_dir(chain.keys[1]))  # evict layer 1
    os.remove(_image_file(builder, ref))  # and the leaf's image
    builder.build(base_tar, plan)
    # Layer 1 runs again, a new build, so layer 2's stored diff -- made on
    # the old build -- may not be replayed onto it: it runs again too.
    assert builder._backend.runs == ["put /a 1", "put /b 2", "put /a 1", "put /b 2"]


def test_an_intermediate_rebuilt_after_eviction_leaves_existing_images_alone(
    builder, base_tar
):
    a = ImageBuildPlan((_run("put /a 1"), _run("put /b 2")))
    b = ImageBuildPlan((_run("put /a 1"), _run("put /c 3")))
    ref_a = builder.build(base_tar, a)
    shutil.rmtree(builder._entry_dir(builder._resolve_chain(base_tar, a).keys[1]))
    builder.build(base_tar, b)
    runs = len(builder._backend.runs)
    assert builder.build(base_tar, a) == ref_a
    assert len(builder._backend.runs) == runs


def test_force_rebuilds_once_per_token_and_rekeys_other_images(builder, base_tar):
    other = ImageBuildPlan((_run("put /a 1"), _run("put /z 9")))
    other_ref = builder.build(base_tar, other)
    token = uuid.uuid4().hex
    forced = ImageBuildPlan(
        (_run("put /a 1", force_token=token), _run("put /b 2", force_token=token))
    )
    builder.build(base_tar, forced)
    builder.build(base_tar, forced)
    assert builder._backend.runs.count("put /a 1") == 2
    # Modal: a forced rebuild breaks the cache for every image built on it.
    assert builder.build(base_tar, other) != other_ref
    assert builder._backend.runs.count("put /z 9") == 2


def test_a_failing_step_raises_and_cleans_up(builder, base_tar):
    plan = ImageBuildPlan((_run("put /a 1"), _run("fail")))
    with pytest.raises(SandboxImageBuildError, match="exit code 1") as info:
        builder.build(base_tar, plan)
    assert "running" in str(info.value) and info.value.image_id
    leftovers = [n for n in os.listdir(builder.images_dir) if ".work." in n]
    assert leftovers == []
    # The first layer was published and is reused next time.
    with pytest.raises(SandboxImageBuildError):
        builder.build(base_tar, plan)
    assert builder._backend.runs.count("put /a 1") == 1


def test_concurrent_builds_of_one_image_build_it_once(builder, base_tar):
    plan = ImageBuildPlan((_run("put /a 1"), _run("put /b 2")))
    refs, errors = [], []

    def go():
        try:
            refs.append(builder.build(base_tar, plan))
        except Exception as err:  # noqa: BLE001
            errors.append(err)

    threads = [threading.Thread(target=go) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors and len(set(refs)) == 1
    assert builder._backend.runs == ["put /a 1", "put /b 2"]


def test_copy_steps_need_no_sandbox(builder, base_tar, tmp_path):
    archive = tmp_path / "c.tar"
    _copy_archive(archive, [("bin/x", b"x", 0o755)])
    plan = ImageBuildPlan((CopyStep("d1", archive.read_bytes(), "COPY x"),))
    ref = builder.build(base_tar, plan)
    assert builder._backend.runs == []
    assert _read(_image_file(builder, ref), "/usr/bin/x") == b"x"


def test_image_env_edits_the_image_config(builder, base_tar):
    plan = ImageBuildPlan(
        (
            _run("put /a 1", image_env=(("A", "one"),)),
            _run("put /b 2", image_env=(("PATH", "/x:$A:${A}"),)),
        )
    )
    ref = builder.build(base_tar, plan)
    config = builder._image_manager.get_image_config(ref)
    assert config["config"]["Env"] == ["A=one", "PATH=/x:one:one"]


def test_a_stale_work_copy_is_swept(builder, base_tar):
    dead = os.path.join(builder.images_dir, "raylayer_abc.work.dead")
    os.makedirs(dead)
    with open(os.path.join(dead, image_layers.BUILDER_FILE), "w") as f:
        json.dump({"pid": 2**22 + 7, "start_time": "1"}, f)
    builder.build(base_tar, ImageBuildPlan((_run("put /a 1"),)))
    assert not os.path.exists(dead)


def test_layer_metadata_never_holds_build_env_values(builder, base_tar):
    plan = ImageBuildPlan((_run("put /a 1", build_env=(("TOKEN", "s3cret"),)),))
    ref = builder.build(base_tar, plan)
    meta = open(
        os.path.join(builder._image_manager.get_image_dir(ref), "layer.json")
    ).read()
    assert "s3cret" not in meta and "TOKEN" in meta


def test_a_cache_hit_refreshes_lru_order(builder, base_tar):
    plan = ImageBuildPlan((_run("put /a 1"),))
    ref = builder.build(base_tar, plan)
    marker = os.path.join(builder._image_manager.get_image_dir(ref), ".extracted")
    os.utime(marker, (1, 1))
    builder.build(base_tar, plan)
    assert os.path.getmtime(marker) > time.time() - 60


def test_a_timed_out_step_is_killed_inside_the_sandbox(builder, base_tar, monkeypatch):
    monkeypatch.setenv("RAY_SANDBOX_IMAGE_BUILD_TIMEOUT_SECONDS", "1")
    started = time.monotonic()
    with pytest.raises(SandboxImageBuildError, match="timed out after 1s"):
        builder.build(base_tar, ImageBuildPlan((_run("hang"),)))
    # Killing the client alone would leave the background sleep holding the
    # output pipe; the step must end at its deadline, not the sleep's.
    assert time.monotonic() - started < 10
    assert len(builder._backend.killed) == 1


def test_a_copy_from_another_image(builder, base_tar, tmp_path):
    source = tmp_path / "uv.tar"
    with tarfile.open(source, "w", format=tarfile.PAX_FORMAT) as tar:
        _tar_add(tar, "uv", data=b"#!uv\n", mode=0o755, uid=1000)
    plan = ImageBuildPlan((CopyFromImageStep(str(source), "/uv", "/.uv/uv"),))
    ref = builder.build(base_tar, plan)
    image = _image_file(builder, ref)
    assert _read(image, "/.uv/uv") == b"#!uv\n"
    with ErofsImage(image) as erofs:
        st = erofs.lstat("/.uv/uv")
    # Root-owned, as Docker's COPY --from writes it, mode kept.
    assert (st.uid, st.mode & 0o7777) == (0, 0o755)
    assert builder._backend.runs == []


@pytest.mark.parametrize("from_prefix", [False, True])
def test_the_start_image_survives_eviction_mid_build(builder, base_tar, from_prefix):
    first = (_run("put /a 1"),)
    if from_prefix:
        builder.build(base_tar, ImageBuildPlan(first))
    # The eviction runs once the build has moved on to its work copy, when
    # no build sandbox holds the start image any more.
    plan = ImageBuildPlan(first + (_run("put /x 0"), _run("evict"), _run("put /b 2")))
    start_ref = (
        image_layers.layer_reference(builder._resolve_chain(base_tar, plan).keys[1])
        if from_prefix
        else base_tar
    )
    start_dir = builder._image_manager.get_image_dir(start_ref)

    def evict(text):
        if text == "evict":
            # As tight as a cache can be: only pinned entries stay.
            evict_least_recently_used_images(builder.images_dir, 1)
            assert os.path.isfile(os.path.join(start_dir, ROOTFS_IMAGE))

    builder._backend.on_exec = evict
    ref = builder.build(base_tar, plan)
    assert _read(_image_file(builder, ref), "/b") == b"2"
    # The build's own pin is released with the build.
    users = os.path.join(start_dir, ".users")
    assert not [u for u in os.listdir(users) if u.startswith("build-")]


def test_teardown_overlaps_the_build_unless_it_maps_the_work_copy(
    builder, base_tar, monkeypatch
):
    backend = builder._backend
    backend.delete_delay = 0.5
    real_apply = image_builder_module.apply_layer_to_erofs

    def apply(image, layer):
        backend.events.append(("apply", None))
        real_apply(image, layer)

    monkeypatch.setattr(image_builder_module, "apply_layer_to_erofs", apply)
    builder.build(base_tar, ImageBuildPlan((_run("put /a 1"), _run("put /b 2"))))
    (on_base, _), (on_work, work_ref) = backend.created
    assert ".work." in work_ref
    events = backend.events
    applies = [i for i, (kind, _) in enumerate(events) if kind == "apply"]
    # The first sandbox ran on the base: its deletion runs behind the apply.
    assert applies[0] < events.index(("delete-end", on_base))
    # The second ran on the work copy, which the apply rewrites: waited for.
    assert events.index(("delete-end", on_work)) < applies[1]


def test_layer_metadata_records_the_layer_digest(builder, base_tar):
    ref = builder.build(base_tar, ImageBuildPlan((_run("put /a 1"),)))
    entry = builder._image_manager.get_image_dir(ref)
    meta = image_layers.read_layer_json(entry)
    with open(os.path.join(entry, LAYER_TAR), "rb") as f:
        data = f.read()
    assert meta["layer"] == {
        "sha256": hashlib.sha256(data).hexdigest(),
        "size": len(data),
    }


def test_build_env_values_are_keyed_with_a_node_secret(tmp_path, base_tar):
    plan = ImageBuildPlan((_run("put /a 1", build_env=(("TOKEN", "s3cret"),)),))
    keys = []
    for name in ("one", "two"):
        manager = ImageManager(images_dir=str(tmp_path / name))
        builder = ImageBuilder(manager, FakeBackend(manager))
        keys.append(builder._resolve_chain(base_tar, plan).leaf)
        assert builder._resolve_chain(base_tar, plan).leaf == keys[-1]
        secret = os.path.join(manager.images_dir, image_layers.BUILD_ENV_KEY_FILE)
        assert stat.S_IMODE(os.stat(secret).st_mode) == 0o600
    # Each node has its own secret, and neither key is the plain hash that
    # anyone could recompute from the step and a guessed value.
    base = builder._resolve_chain(base_tar, plan).keys[0]
    assert keys[0] != keys[1] and step_key(base, 0, plan.steps[0]) not in keys
    # Changing the value still changes the key.
    other = ImageBuildPlan((_run("put /a 1", build_env=(("TOKEN", "other"),)),))
    assert builder._resolve_chain(base_tar, other).leaf != keys[1]


def test_builds_of_different_images_over_a_shared_prefix_do_not_deadlock(
    image_manager, base_tar
):
    backend = FakeBackend(image_manager)
    plans = [
        ImageBuildPlan((_run("put /a 1"), _run("put /b 2"))),
        ImageBuildPlan((_run("put /a 1"), _run("put /b 2"), _run("put /c 3"))),
    ]
    errors = []

    def go(plan):
        # One builder per thread, as each sandbox actor has its own.
        try:
            ImageBuilder(image_manager, backend).build(base_tar, plan)
        except Exception as err:  # noqa: BLE001
            errors.append(err)

    for _ in range(3):
        threads = [threading.Thread(target=go, args=(p,)) for p in plans]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        assert not any(t.is_alive() for t in threads), "builds deadlocked"
    assert not errors
    # Each shared layer ran once, however the builds interleaved.
    assert sorted(backend.runs) == ["put /a 1", "put /b 2", "put /c 3"]


# -- concurrent builds never redo each other's work ---------------------------


def test_a_layer_forced_by_another_build_with_the_same_token_is_reused(
    image_manager, base_tar
):
    # Modal: base = Image.debian_slim(force_build=True), then base.pip_install
    # of different packages, all created at once. Every build forces the
    # shared layer with the same token, so it must be rebuilt once, not once
    # per image.
    backend = FakeBackend(image_manager)
    backend.on_exec = lambda text: time.sleep(0.5) if text == "put /a 1" else None
    token = uuid.uuid4().hex
    shared = _run("put /a 1", force_token=token)
    plans = [
        ImageBuildPlan((shared,)),
        ImageBuildPlan((shared, _run("put /b 2", force_token=token))),
        ImageBuildPlan((shared, _run("put /c 3", force_token=token))),
    ]
    errors = []

    def go(plan):
        try:
            ImageBuilder(image_manager, backend).build(base_tar, plan)
        except Exception as err:  # noqa: BLE001
            errors.append(err)

    threads = [threading.Thread(target=go, args=(p,)) for p in plans]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert not errors
    assert backend.runs.count("put /a 1") == 1
    # The image that ends at the shared layer kept its image: no later build
    # rebuilt the layer and replaced it.
    builder = ImageBuilder(image_manager, backend)
    leaf = builder._resolve_chain(base_tar, plans[0]).leaf
    assert os.path.isfile(os.path.join(builder._entry_dir(leaf), ROOTFS_IMAGE))


def test_a_build_adopts_layers_another_build_made_on_a_new_lineage(
    builder, base_tar, monkeypatch
):
    monkeypatch.setenv("RAY_SANDBOX_IMAGE_BUILD_CONCURRENCY", "0")
    s1, s2, s3 = _run("put /a 1"), _run("put /b 2"), _run("put /c 3")
    builder.build(base_tar, ImageBuildPlan((s1,)))
    first = builder._entry_dir(
        builder._resolve_chain(base_tar, ImageBuildPlan((s1,))).leaf
    )
    # Y below then starts from the base and replays this layer's diff.
    os.remove(os.path.join(first, ROOTFS_IMAGE))
    other = ImageBuilder(builder._image_manager, builder._backend)
    plan_x = ImageBuildPlan((s1, s2))
    real_apply = image_builder_module.apply_layer_to_erofs
    fired = []

    def apply(image, layer):
        real_apply(image, layer)
        if not fired:
            fired.append(True)
            # Right after Y replayed it, eviction takes that layer, and
            # another build makes it again and X on top: a new lineage.
            shutil.rmtree(first)
            other.build(base_tar, plan_x)

    monkeypatch.setattr(image_builder_module, "apply_layer_to_erofs", apply)
    ref_y = builder.build(base_tar, ImageBuildPlan((s1, s2, s3)))
    # X ran s1 and s2; Y ran only its own step, on what X left.
    assert builder._backend.runs == ["put /a 1", "put /a 1", "put /b 2", "put /c 3"]
    ref_x = image_layers.layer_reference(other._resolve_chain(base_tar, plan_x).leaf)
    assert os.path.isfile(_image_file(builder, ref_x))
    image = _image_file(builder, ref_y)
    assert [_read(image, p) for p in ("/a", "/b", "/c")] == [b"1", b"2", b"3"]


def test_rebases_are_bounded(builder, base_tar, monkeypatch):
    # However often another build seems to supply a layer, the build ends:
    # its last attempt builds rather than starting over again.
    monkeypatch.setattr(ImageBuilder, "_covered", lambda *args: True)
    attempts = []
    real = ImageBuilder._materialize

    def materialize(self, *args, **kwargs):
        attempts.append(kwargs["allow_rebase"])
        return real(self, *args, **kwargs)

    monkeypatch.setattr(ImageBuilder, "_materialize", materialize)
    ref = builder.build(base_tar, ImageBuildPlan((_run("put /a 1"),)))
    assert attempts == [True, True, True, False]
    assert _read(_image_file(builder, ref), "/a") == b"1"


# -- cache policy and resource bounds ----------------------------------------


def _entry_size(path):
    return sum(
        os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs
    )


def _age(builder, ref, seconds):
    marker = os.path.join(builder._image_manager.get_image_dir(ref), ".extracted")
    stamp = time.time() - seconds
    os.utime(marker, (stamp, stamp))


def test_eviction_takes_a_built_image_before_its_layer(builder, base_tar):
    plan_a = ImageBuildPlan((_run("put /a 1"),))
    plan_b = ImageBuildPlan((_run("put /b 2"),))
    ref_a, ref_b = builder.build(base_tar, plan_a), builder.build(base_tar, plan_b)
    _age(builder, base_tar, 300)
    _age(builder, ref_a, 200)
    _age(builder, ref_b, 100)
    dir_a = builder._image_manager.get_image_dir(ref_a)
    image_a = os.path.getsize(os.path.join(dir_a, ROOTFS_IMAGE))
    total = sum(
        _entry_size(os.path.join(builder.images_dir, n))
        for n in os.listdir(builder.images_dir)
        if os.path.isdir(os.path.join(builder.images_dir, n))
    )
    base_size = _entry_size(builder._image_manager.get_image_dir(base_tar))
    # Room for all but the base and A's image: the base, oldest, goes whole;
    # A loses only its image, keeping the layer that rebuilds it.
    evict_least_recently_used_images(
        builder.images_dir, total - base_size - image_a + 1
    )
    assert not os.path.exists(builder._image_manager.get_image_dir(base_tar))
    assert not os.path.exists(os.path.join(dir_a, ROOTFS_IMAGE))
    assert os.path.isfile(os.path.join(dir_a, LAYER_TAR))
    assert os.path.isfile(_image_file(builder, ref_b))
    # Nothing can run A until it is rebuilt, which replays its layer.
    with pytest.raises(BuiltImageEvictedError):
        builder._image_manager.pull_image(ref_a)
    runs = len(builder._backend.runs)
    assert builder.build(base_tar, plan_a) == ref_a
    assert len(builder._backend.runs) == runs
    assert _read(_image_file(builder, ref_a), "/a") == b"1"


def test_eviction_takes_layers_too_when_it_must(builder, base_tar):
    ref = builder.build(base_tar, ImageBuildPlan((_run("put /a 1"),)))
    evict_least_recently_used_images(builder.images_dir, 1)
    assert not os.path.exists(builder._image_manager.get_image_dir(ref))


def test_a_hit_keeps_the_layers_below_the_image_warm(builder, base_tar):
    plan = ImageBuildPlan((_run("put /a 1"), _run("put /b 2"), _run("put /c 3")))
    ref = builder.build(base_tar, plan)
    keys = builder._resolve_chain(base_tar, plan).keys
    markers = [os.path.join(builder._entry_dir(k), ".extracted") for k in keys[1:]]
    for marker in markers:
        os.utime(marker, (1, 1))
    builder.build(base_tar, plan)
    assert all(os.path.getmtime(m) > time.time() - 60 for m in markers)
    # Touched recently: left alone, rather than written on every hit.
    os.utime(markers[0], (time.time() - 30, time.time() - 30))
    before = os.path.getmtime(markers[0])
    builder.build(base_tar, plan)
    assert os.path.getmtime(markers[0]) == before
    assert ref == image_layers.layer_reference(keys[-1])


@pytest.mark.parametrize("limit, serial", [("1", True), ("0", False)])
def test_build_slots_bound_concurrent_builds(
    image_manager, base_tar, monkeypatch, limit, serial
):
    monkeypatch.setenv("RAY_SANDBOX_IMAGE_BUILD_CONCURRENCY", limit)
    backend = FakeBackend(image_manager)
    lock = threading.Lock()
    active = [0]
    peak = [0]

    def step(text):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        time.sleep(0.3)
        with lock:
            active[0] -= 1

    backend.on_exec = step
    threads = [
        threading.Thread(
            target=lambda i=i: ImageBuilder(image_manager, backend).build(
                base_tar, ImageBuildPlan((_run(f"put /f{i} {i}"),))
            )
        )
        for i in range(3)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert len(backend.runs) == 3
    assert (peak[0] == 1) == serial


def test_build_sandboxes_take_the_node_caps(builder, base_tar, monkeypatch):
    monkeypatch.setenv("RAY_SANDBOX_IMAGE_BUILD_CPU", "2")
    monkeypatch.setenv("RAY_SANDBOX_IMAGE_BUILD_MEMORY", "4Gi")
    builder.build(base_tar, ImageBuildPlan((_run("put /a 1"),)))
    (config,) = builder._backend.configs
    assert (config.cpu, config.memory) == (2.0, "4Gi")


def test_build_sandboxes_are_uncapped_by_default(builder, base_tar):
    builder.build(base_tar, ImageBuildPlan((_run("put /a 1"),)))
    (config,) = builder._backend.configs
    assert (config.cpu, config.memory) == (0.0, 0)


def test_runtime_rebuilds_an_image_evicted_before_its_sandbox_pinned_it(
    image_manager, base_tar
):
    backend = FakeBackend(image_manager)
    runtime = SandboxRuntime.__new__(SandboxRuntime)
    runtime._image_manager = image_manager
    runtime._backend = backend
    runtime._image_builder = ImageBuilder(image_manager, backend)
    runtime._ttl_timers = {}
    plan = ImageBuildPlan((_run("put /a 1"),))
    real_create = backend.create_sandbox
    raced = []

    def create_sandbox(config):
        if not raced and config.image.startswith("raylayer@"):
            # Eviction got to the image between its build and this pin.
            raced.append(config.image)
            os.remove(_image_file(runtime._image_builder, config.image))
        return real_create(config)

    backend.create_sandbox = create_sandbox
    instance = runtime.create(base_tar, build=plan)
    assert raced and backend._images[instance] == raced[0]
    # Rebuilt from its cached layer, without running the step again.
    assert backend.runs == ["put /a 1"]


# -- reproducibility across nodes ---------------------------------------------


def _tree(image_path):
    """Every path in an image with its metadata and content."""
    found = {}
    with ErofsImage(image_path) as erofs:

        def walk(nid, path):
            for name, child in erofs._dirents(erofs._inode(nid)):
                if name in (b".", b".."):
                    continue
                child_path = f"{path}/{name.decode()}"
                st = erofs.stat(child)
                found[child_path] = (st.mode, st.uid, st.gid, st.mtime)
                if st.is_dir:
                    walk(child, child_path)

        walk(erofs.root_nid, "")
    for path, meta in found.items():
        if stat.S_ISREG(meta[0]):
            found[path] = meta + (_read(image_path, path),)
    return found


def test_copy_layers_come_out_the_same_on_every_node(tmp_path, base_tar):
    source = tmp_path / "tool.tar"
    with tarfile.open(source, "w", format=tarfile.PAX_FORMAT) as tar:
        _tar_add(tar, "tool", data=b"tool\n", mode=0o755)
    archive = tmp_path / "c.tar"
    _copy_archive(archive, [("srv/app/main.py", b"print(1)\n", 0o644)])
    plan = ImageBuildPlan(
        (
            CopyStep("d1", archive.read_bytes(), "COPY app"),
            CopyFromImageStep(str(source), "/tool", "/opt/tool"),
        )
    )
    trees, layers = [], []
    for node in ("node-a", "node-b"):
        manager = ImageManager(images_dir=str(tmp_path / node))
        builder = ImageBuilder(manager, FakeBackend(manager))
        ref = builder.build(base_tar, plan)
        trees.append(_tree(_image_file(builder, ref)))
        chain = builder._resolve_chain(base_tar, plan)
        layers.append(
            [
                open(os.path.join(builder._entry_dir(k), LAYER_TAR), "rb").read()
                for k in chain.keys[1:]
            ]
        )
        # The directories a COPY creates carry the fixed time, not the build's.
        assert trees[-1]["/srv"][3] == COPY_MTIME
        time.sleep(1.1)  # a later build, on a later clock
    assert layers[0] == layers[1]
    assert trees[0] == trees[1]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
