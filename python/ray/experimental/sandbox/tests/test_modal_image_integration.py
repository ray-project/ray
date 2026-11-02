"""Integration tests for the Modal-compatible Image, against real containers.

These pull real images and create real sandboxes from them, checking that an
Image's runtime metadata -- env, workdir, cmd, entrypoint, startup files --
actually reaches the container. They need runsc and a Ray cluster, so they run
only under ``TEST_SANDBOX=1`` (enforced by this directory's conftest).

Image building is not implemented in this version, so there is nothing here
that builds. The runsc-free half of the Image coverage lives in
``sandbox/modal/tests/test_modal_image.py``.
"""

import hashlib
import os
import sys

import pytest

import ray
from ray.experimental.sandbox import modal

# Sandboxes here run on Modal's default network, network="public", which
# needs slirp4netns and a host that allows a per-sandbox network namespace.
pytestmark = pytest.mark.usefixtures("ensure_slirp4netns")

# busybox has a shell but no package managers; the python image has GNU
# coreutils rather than busybox applets.
MINIMAL_IMAGE = "busybox:latest"
PYTHON_IMAGE = "python:3.13-slim"
# Its config sets ENTRYPOINT ["git"] and CMD ["--help"].
ENTRYPOINT_IMAGE = "alpine/git:latest"


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield


def run(sandbox, *args):
    """Run a command and return its stripped stdout, asserting success."""
    process = sandbox.exec(*args)
    output = process.stdout.read()
    assert process.wait() == 0, output
    return output.strip()


# -- runtime metadata ------------------------------------------------------


def test_image_metadata_reaches_an_exec_and_create_overrides_it():
    """Also confirms runsc exec inherits the container spec's environment."""
    image = (
        modal.Image.from_registry(MINIMAL_IMAGE)
        .env({"GREETING": "hola", "K": "from-image"})
        .workdir("/tmp")
    )
    sandbox = modal.Sandbox.create(image=image, timeout=300, env={"K": "from-create"})
    try:
        assert run(sandbox, "sh", "-c", "echo $GREETING $K") == "hola from-create"
        assert run(sandbox, "pwd") == "/tmp"
    finally:
        sandbox.terminate()


# -- the main process ------------------------------------------------------


@pytest.mark.parametrize(
    "entrypoint,args,expected",
    [
        # The image's CMD becomes the main process...
        (None, (), "from-cmd"),
        # ...create()'s arguments override it...
        (None, ("echo", "from-create"), "from-create"),
        # ...and an ENTRYPOINT prefixes whichever wins: `env sh -c 'echo ...'`.
        (["env"], ("sh", "-c", "echo prefixed"), "prefixed"),
    ],
    ids=["cmd", "create-arguments", "entrypoint"],
)
def test_the_main_process_follows_modals_precedence(entrypoint, args, expected):
    image = modal.Image.from_registry(MINIMAL_IMAGE).cmd(["echo", "from-cmd"])
    if entrypoint is not None:
        image = image.entrypoint(entrypoint)
    sandbox = modal.Sandbox.create(*args, image=image, timeout=300)
    try:
        assert sandbox.stdout.read().strip() == expected
        sandbox.wait()
        assert sandbox.returncode == 0
    finally:
        sandbox.terminate()


@pytest.mark.parametrize(
    "clear,expected",
    [
        # The base image's ENTRYPOINT ["git"] turns the arguments into a git
        # subcommand...
        (False, "git: 'echo' is not a git command."),
        # ...and entrypoint([]) removes it, as on Modal.
        (True, "MAIN"),
    ],
)
def test_an_empty_entrypoint_clears_the_base_images(clear, expected):
    image = modal.Image.from_registry(ENTRYPOINT_IMAGE)
    if clear:
        image = image.entrypoint([])
    sandbox = modal.Sandbox.create("echo", "MAIN", image=image, timeout=300)
    try:
        sandbox.wait(raise_on_termination=False)
        output = sandbox.stdout.read() + sandbox.stderr.read()
        assert output.strip().startswith(expected), output
    finally:
        sandbox.terminate()


# -- startup files ---------------------------------------------------------


def test_copy_false_files_land_before_the_main_process_runs(tmp_path):
    source = tmp_path / "pushed.txt"
    source.write_bytes(b"pushed\n")
    image = (
        modal.Image.from_registry(MINIMAL_IMAGE)
        .add_local_file(source, "/pushed.txt")
        .cmd(["cat", "/pushed.txt"])
    )
    sandbox = modal.Sandbox.create(image=image, timeout=300)
    try:
        # The main process could only read it if the push happened first.
        assert sandbox.stdout.read().strip() == "pushed"
    finally:
        sandbox.terminate()


def test_copy_false_files_do_not_enter_the_image(tmp_path):
    source = tmp_path / "ephemeral.txt"
    source.write_bytes(b"ephemeral\n")
    image = modal.Image.from_registry(MINIMAL_IMAGE).add_local_file(
        source, "/ephemeral.txt"
    )

    first = modal.Sandbox.create(image=image, timeout=300)
    try:
        assert first.filesystem.read_text("/ephemeral.txt").strip() == "ephemeral"
    finally:
        first.terminate()

    # Pushed per sandbox rather than baked in, so the reference is still the base.
    assert image.reference == MINIMAL_IMAGE


def test_a_copy_false_file_of_many_chunks_is_pushed_byte_for_byte(tmp_path):
    """A startup file travels the same upload path as write_bytes, and one
    larger than a chunk used to arrive with its chunks out of order."""
    payload = os.urandom(5 * 1024 * 1024 + 123)
    source = tmp_path / "blob.bin"
    source.write_bytes(payload)
    image = modal.Image.from_registry(MINIMAL_IMAGE).add_local_file(
        source, "/data/blob.bin"
    )
    sandbox = modal.Sandbox.create(image=image, timeout=300)
    try:
        process = sandbox.exec("sh", "-c", "sha256sum /data/blob.bin | cut -d' ' -f1")
        assert process.stdout.read().strip() == hashlib.sha256(payload).hexdigest()
        assert process.wait() == 0
    finally:
        sandbox.terminate()


def test_a_local_directory_is_pushed_whole(tmp_path):
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "a.txt").write_bytes(b"a\n")
    (tmp_path / "b.txt").write_bytes(b"b\n")

    image = modal.Image.from_registry(MINIMAL_IMAGE).add_local_dir(tmp_path, "/tree")
    sandbox = modal.Sandbox.create(image=image, timeout=300)
    try:
        assert sandbox.filesystem.read_text("/tree/b.txt").strip() == "b"
        assert sandbox.filesystem.read_text("/tree/pkg/a.txt").strip() == "a"
    finally:
        sandbox.terminate()


def test_force_build_leaves_a_running_sandbox_of_that_image_intact():
    """Measured before the fix: once another sandbox forced a re-pull, the
    running one's ``ls /etc`` came back empty -- its cache entry was deleted
    while its container was still being served from it."""
    running = modal.Sandbox.create(image=MINIMAL_IMAGE, timeout=300)
    try:
        before = run(running, "sh", "-c", "ls /etc | wc -l")
        forced = modal.Sandbox.create(
            image=modal.Image.from_registry(MINIMAL_IMAGE, force_build=True),
            timeout=300,
        )
        forced.terminate()
        assert run(running, "sh", "-c", "ls /etc | wc -l") == before
        assert run(running, "sh", "-c", "cat /etc/group | head -1").startswith("root")
    finally:
        running.terminate()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
