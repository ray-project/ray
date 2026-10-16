"""Integration tests for image building through the Modal-compatible API.

These build real layers in real build sandboxes -- pip and apt reach the
network -- and check what reaches the sandbox, and what is reused from the
node's layer cache. They need runsc, slirp4netns, mkfs.erofs 1.8+ and a Ray
cluster, so they run only under ``TEST_SANDBOX=1``.
"""

import glob
import json
import os
import sys
import uuid

import pytest

import ray
from ray.experimental.sandbox import modal
from ray.experimental.sandbox._internal import image_layers
from ray.experimental.sandbox._internal.image_utils import DEFAULT_IMAGES_DIR

pytestmark = pytest.mark.usefixtures("ensure_slirp4netns")

PYTHON_IMAGE = "python:3.13-slim"


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    image_layers.mkfs_erofs_supports_incremental.cache_clear()
    if not image_layers.mkfs_erofs_supports_incremental():
        pytest.skip("needs mkfs.erofs 1.8+ (erofs-utils) with --incremental")
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield


def run(sandbox, *args):
    process = sandbox.exec(*args)
    output = process.stdout.read()
    assert process.wait() == 0, output
    return output.strip()


def layers_running(text):
    """The cached layers whose commands mention ``text``: {key: layer.json}."""
    found = {}
    for path in glob.glob(os.path.join(DEFAULT_IMAGES_DIR, "raylayer_*", "layer.json")):
        if ".work." in path:
            continue
        with open(path) as f:
            meta = json.load(f)
        if any(text in c for c in meta["step"].get("commands", [])):
            found[meta["key"]] = meta
    return found


def test_built_layers_reach_the_sandbox():
    tag = uuid.uuid4().hex[:8]
    image = (
        modal.Image.from_registry(PYTHON_IMAGE)
        .pip_install("six")
        .run_commands(f"echo {tag} > /built", "rm /etc/issue")
    )
    sandbox = modal.Sandbox.create(image=image, timeout=600)
    try:
        assert run(sandbox, "cat", "/built") == tag
        assert run(sandbox, "python", "-c", "import six; print(six.__name__)") == "six"
        assert run(sandbox, "sh", "-c", "test -e /etc/issue || echo gone") == "gone"
    finally:
        sandbox.terminate()


def test_a_second_sandbox_reuses_the_cached_image():
    tag = uuid.uuid4().hex[:8]
    image = modal.Image.from_registry(PYTHON_IMAGE).run_commands(f"echo {tag} > /t")
    for _ in range(2):
        sandbox = modal.Sandbox.create(image=image, timeout=600)
        try:
            assert run(sandbox, "cat", "/t") == tag
        finally:
            sandbox.terminate()
    (meta,) = layers_running(tag).values()
    # One build, not two: the second create found it cached.
    first_build = meta["build_id"]
    sandbox = modal.Sandbox.create(image=image, timeout=600)
    sandbox.terminate()
    (meta,) = layers_running(tag).values()
    assert meta["build_id"] == first_build


def test_images_sharing_steps_share_their_layers():
    tag = uuid.uuid4().hex[:8]
    common = modal.Image.from_registry(PYTHON_IMAGE).run_commands(
        f"echo {tag} > /shared"
    )
    first = modal.Sandbox.create(image=common.run_commands("echo a > /x"), timeout=600)
    first.terminate()
    (shared,) = layers_running(tag).values()
    second = modal.Sandbox.create(image=common.run_commands("echo b > /x"), timeout=600)
    try:
        assert run(second, "cat", "/x") == "b"
        assert run(second, "cat", "/shared") == tag
    finally:
        second.terminate()
    (again,) = layers_running(tag).values()
    assert again["build_id"] == shared["build_id"]


def test_metadata_set_before_a_step_reaches_it():
    image = (
        modal.Image.from_registry(PYTHON_IMAGE)
        .env({"GREETING": "hola"})
        .workdir("/work/dir")
        .run_commands("echo $GREETING $ONLY_HERE > here.txt", env={"ONLY_HERE": "x"})
    )
    sandbox = modal.Sandbox.create(image=image, timeout=600)
    try:
        assert run(sandbox, "cat", "/work/dir/here.txt") == "hola x"
        # Build-only variables do not reach the sandbox.
        assert run(sandbox, "sh", "-c", "echo ${ONLY_HERE:-unset}") == "unset"
    finally:
        sandbox.terminate()


def test_copied_files_feed_later_steps(tmp_path):
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "data.txt").write_text("payload")
    image = (
        modal.Image.from_registry(PYTHON_IMAGE)
        .add_local_dir(tmp_path / "pkg", "/app", copy=True)
        .run_commands("cat /app/data.txt > /copied")
    )
    sandbox = modal.Sandbox.create(image=image, timeout=600)
    try:
        assert run(sandbox, "cat", "/copied") == "payload"
        assert run(sandbox, "stat", "-c", "%U %a", "/root") == "root 700"
    finally:
        sandbox.terminate()


def test_apt_install_works():
    image = modal.Image.from_registry(PYTHON_IMAGE).apt_install("jq")
    sandbox = modal.Sandbox.create(image=image, timeout=600)
    try:
        assert run(sandbox, "sh", "-c", "echo '{\"a\": 1}' | jq .a") == "1"
    finally:
        sandbox.terminate()


def test_a_failing_step_raises_image_build_error():
    tag = uuid.uuid4().hex[:8]
    image = modal.Image.from_registry(PYTHON_IMAGE).run_commands(f"echo {tag}; exit 7")
    with pytest.raises(modal.exception.ImageBuildError) as info:
        modal.Sandbox.create(image=image, timeout=600)
    assert "exit code 7" in str(info.value) and tag in str(info.value)


def test_files_given_xattrs_stay_readable():
    # gVisor's EROFS reader refuses any inode that carries an xattr, and
    # setcap, some apt packages and plain user xattrs all leave them: the
    # builder drops them, so the files stay readable.
    script = (
        "import os, shutil; shutil.copy('/bin/true', '/usr/local/bin/captrue'); "
        "os.setxattr('/usr/local/bin/captrue', 'security.capability', "
        "bytes([1, 0, 0, 2, 0, 32, 0, 0] + [0] * 12)); "
        "open('/note', 'w').write('note'); os.setxattr('/note', 'user.test', b'1')"
    )
    image = modal.Image.from_registry(PYTHON_IMAGE).run_commands(
        f'python -c "{script}"'
    )
    sandbox = modal.Sandbox.create(image=image, timeout=600)
    try:
        assert run(sandbox, "cat", "/note") == "note"
        run(sandbox, "/usr/local/bin/captrue")
    finally:
        sandbox.terminate()


def test_layers_keep_replacements_links_and_special_files():
    tag = uuid.uuid4().hex[:8]
    image = (
        modal.Image.from_registry("busybox:latest")
        .run_commands(
            f"echo {tag} > /tag && mkdir -p /m/dir2file /m/opq",
            "echo x > /m/dir2file/inner && echo f > /m/file2dir && echo hl > /m/lower",
            "echo old > /m/opq/old && echo gone > /m/gone",
        )
        .run_commands(
            "rm -rf /m/dir2file && echo now > /m/dir2file",
            "rm /m/file2dir && mkdir /m/file2dir && echo i > /m/file2dir/inner",
            "ln /m/lower /m/hardlink && ln -s lower /m/symlink && mkfifo /m/fifo",
            "rm -rf /m/opq && mkdir /m/opq && echo new > /m/opq/new && rm /m/gone",
            "truncate -s 1M /m/sparse",
        )
    )
    sandbox = modal.Sandbox.create(image=image, timeout=600)
    try:
        out = run(
            sandbox,
            "sh",
            "-c",
            "cd /m && stat -c '%n|%F|%s|%h|%i' dir2file file2dir lower hardlink "
            "symlink fifo sparse",
        )
        st = {line.split("|")[0]: line.split("|")[1:] for line in out.splitlines()}
        assert st["dir2file"][0] == "regular file"
        assert st["file2dir"][0] == "directory"
        # One inode, linked twice.
        assert st["hardlink"][2:] == st["lower"][2:] and st["lower"][2] == "2"
        assert st["symlink"][0] == "symbolic link"
        assert st["fifo"][0] == "fifo"
        assert st["sparse"][:2] == ["regular file", str(1 << 20)]
        assert run(sandbox, "cat", "/m/dir2file", "/m/file2dir/inner") == "now\ni"
        assert run(sandbox, "cat", "/m/symlink") == "hl"
        assert run(sandbox, "ls", "/m/opq") == "new"
        assert run(sandbox, "sh", "-c", "test -e /m/gone || echo gone") == "gone"
    finally:
        sandbox.terminate()


def test_steps_run_as_root_whatever_the_image_user():
    # The image's config sets USER 101. Measured on Modal: its build steps,
    # and execs in the sandbox, run as root regardless.
    tag = uuid.uuid4().hex[:8]
    image = modal.Image.from_registry(
        "nginxinc/nginx-unprivileged:alpine"
    ).run_commands(f"echo {tag} > /tag && id -u > /build_uid")
    sandbox = modal.Sandbox.create(image=image, timeout=600)
    try:
        assert run(sandbox, "cat", "/build_uid") == "0"
        assert run(sandbox, "id", "-u") == "0"
    finally:
        sandbox.terminate()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
