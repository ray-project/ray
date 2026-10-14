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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
