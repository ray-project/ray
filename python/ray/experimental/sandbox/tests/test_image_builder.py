"""The image builder: layer encoding, in-place application, and caching.

Runs the real ``mkfs.erofs`` (1.8+, for ``--incremental``) and ``fsck.erofs``
but no gVisor: a fake backend stands in for build sandboxes, and each step's
commands script the overlay diff it "produces" (see :class:`FakeBackend`).
The end-to-end path through runsc is in test_modal_image_build_integration.
"""

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

from ray.experimental.sandbox._internal import image_layers
from ray.experimental.sandbox._internal.erofs_reader import ErofsImage
from ray.experimental.sandbox._internal.image_utils import (
    ROOTFS_IMAGE,
    mkfs_erofs_path,
)
from ray.experimental.sandbox.exceptions import SandboxImageBuildError
from ray.experimental.sandbox.image_build import (
    CopyStep,
    ImageBuildPlan,
    RunStep,
)
from ray.experimental.sandbox.image_builder import ImageBuilder
from ray.experimental.sandbox.image_manager import ImageManager


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
        self._ops = {}
        self._images = {}
        self._lock = threading.Lock()

    def create_sandbox(self, config):
        sandbox_id = f"fake-{uuid.uuid4().hex[:8]}"
        self.image_manager.pull_image(config.image, instance_id=sandbox_id)
        with self._lock:
            self._ops[sandbox_id] = []
            self._images[sandbox_id] = config.image
        return sandbox_id

    def exec_argv(self, sandbox_id, command):
        text = command[-1]
        with self._lock:
            self._ops[sandbox_id].append(text)
            self.runs.append(text)
        return ["sh", "-c", "echo running; exit 1" if text == "fail" else "true"]

    def export_rootfs_upper(self, sandbox_id, path):
        whiteout = None
        with tarfile.open(path, "w", format=tarfile.PAX_FORMAT) as tar:
            _tar_add(tar, "./", type=tarfile.DIRTYPE, mode=0o777, uid=1234)
            written = set()
            for op in self._ops[sandbox_id]:
                words = op.split(" ", 2)
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
        self.image_manager.release_image(self._images.pop(sandbox_id), sandbox_id)
        self._ops.pop(sandbox_id, None)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
