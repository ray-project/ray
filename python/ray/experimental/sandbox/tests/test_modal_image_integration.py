"""Integration tests for the Modal-compatible Image, against real containers.

These pull real images and create real sandboxes from them, checking that an
Image's runtime metadata -- env, workdir, cmd, entrypoint, startup files --
actually reaches the container. They need runsc and a Ray cluster, so they run
only under ``TEST_SANDBOX=1`` (enforced by this directory's conftest).

Image building is not implemented in this version, so there is nothing here
that builds. The runsc-free half of the Image coverage lives in
``sandbox/modal/tests/test_modal_image.py``.
"""

import sys

import pytest

import ray
from ray.experimental.sandbox import modal

# busybox has a shell but no package managers; the python image has GNU
# coreutils rather than busybox applets.
MINIMAL_IMAGE = "busybox:latest"
PYTHON_IMAGE = "python:3.13-slim"


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


def test_image_env_reaches_an_exec():
    """Also confirms runsc exec inherits the container spec's environment."""
    image = modal.Image.from_registry(MINIMAL_IMAGE).env({"GREETING": "hola"})
    sandbox = modal.Sandbox.create(image=image, timeout=300)
    try:
        assert run(sandbox, "sh", "-c", "echo $GREETING") == "hola"
    finally:
        sandbox.terminate()


def test_create_env_overrides_image_env():
    image = modal.Image.from_registry(MINIMAL_IMAGE).env({"K": "from-image"})
    sandbox = modal.Sandbox.create(image=image, timeout=300, env={"K": "from-create"})
    try:
        assert run(sandbox, "sh", "-c", "echo $K") == "from-create"
    finally:
        sandbox.terminate()


def test_image_workdir_is_the_working_directory():
    image = modal.Image.from_registry(MINIMAL_IMAGE).workdir("/tmp")
    sandbox = modal.Sandbox.create(image=image, timeout=300)
    try:
        assert run(sandbox, "pwd") == "/tmp"
    finally:
        sandbox.terminate()


def test_metadata_alone_resolves_to_the_base_tag():
    """Metadata is sandbox config, so the image reference is the base tag."""
    image = modal.Image.from_registry(MINIMAL_IMAGE).env({"A": "1"}).workdir("/tmp")
    assert image.reference == MINIMAL_IMAGE
    sandbox = modal.Sandbox.create(image=image, timeout=300)
    try:
        assert run(sandbox, "sh", "-c", "echo $A") == "1"
    finally:
        sandbox.terminate()


# -- the main process ------------------------------------------------------


def test_image_cmd_becomes_the_main_process():
    image = modal.Image.from_registry(MINIMAL_IMAGE).cmd(["echo", "from-cmd"])
    sandbox = modal.Sandbox.create(image=image, timeout=300)
    try:
        assert sandbox.stdout.read().strip() == "from-cmd"
        sandbox.wait()
        assert sandbox.returncode == 0
    finally:
        sandbox.terminate()


def test_create_arguments_override_the_image_cmd():
    image = modal.Image.from_registry(MINIMAL_IMAGE).cmd(["echo", "from-cmd"])
    sandbox = modal.Sandbox.create("echo", "from-create", image=image, timeout=300)
    try:
        assert sandbox.stdout.read().strip() == "from-create"
    finally:
        sandbox.terminate()


def test_entrypoint_prefixes_whichever_command_wins():
    image = (
        modal.Image.from_registry(MINIMAL_IMAGE)
        .entrypoint(["env"])
        .cmd(["sh", "-c", "echo ignored"])
    )
    sandbox = modal.Sandbox.create(
        "sh", "-c", "echo prefixed", image=image, timeout=300
    )
    try:
        # `env sh -c 'echo prefixed'` runs the overriding command via entrypoint.
        assert sandbox.stdout.read().strip() == "prefixed"
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
