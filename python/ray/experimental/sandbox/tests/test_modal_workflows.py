"""End-to-end workflow tests for the Modal-compatible Sandbox API.

The rest of the suite tests one behaviour at a time: create a sandbox, assert
something, tear down. That leaves the interesting failures untested, because
they live in the seams -- state not surviving between execs, a half-consumed
stream wedging teardown, cleanup skipped when a step raises midway, two
sandboxes from one image reaching each other through the shared root filesystem.

Each test here drives a realistic *sequence* instead, and is named for the
property the sequence proves rather than for the call it happens to end on.
Where a test mirrors one of Modal's own (``py/test/sandbox_test.py``), the
docstring names it so the correspondence is greppable.

Needs runsc and a Ray cluster, so it runs only under ``TEST_SANDBOX=1``
(enforced by this directory's conftest).
"""

import concurrent.futures
import sys
import textwrap
import time

import pytest

import ray
from ray.experimental.sandbox import modal
from ray.experimental.sandbox.modal.exception import (
    SandboxTerminatedError,
    SandboxTimeoutError,
)

# Sandboxes here run on Modal's default network, network="public", which
# needs slirp4netns and a host that allows a per-sandbox network namespace.
pytestmark = pytest.mark.usefixtures("ensure_slirp4netns")

# No package manager, but a shell and the coreutils applets a workflow needs.
# Everything that does not specifically exercise the toolchain uses this: a
# debian_slim build is ~619MB and minutes on a cold cache.
IMAGE = "busybox:latest"
PYTHON_IMAGE = "python:3.13-slim"


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield


@pytest.fixture
def sandbox():
    """An idle sandbox, torn down even when the test fails."""
    sb = modal.Sandbox.create(image=IMAGE, timeout=300)
    try:
        yield sb
    finally:
        sb.terminate()


def run(sandbox, *args, **kwargs):
    """Run a command to completion and return its stripped stdout.

    Asserts success, and puts the output in the failure message -- a workflow
    that breaks three steps in is otherwise reported as a bare exit code.
    """
    process = sandbox.exec(*args, **kwargs)
    output = process.stdout.read()
    assert process.wait() == 0, f"{args} failed: {output}"
    return output.strip()


# -- the agent loop --------------------------------------------------------


def test_an_agent_can_write_run_and_collect_across_steps(sandbox):
    """The canonical use: author code, run it, collect what it produced.

    Each step depends on the one before, which is what distinguishes this from
    the per-operation tests: it fails if the filesystem namespace and exec do
    not observe the same container state.
    """
    fs = sandbox.filesystem

    fs.make_directory("/work")
    fs.write_text(
        textwrap.dedent(
            """\
            #!/bin/sh
            echo "computing"
            echo 42 > /work/result.txt
            """
        ),
        "/work/task.sh",
    )

    assert run(sandbox, "sh", "/work/task.sh") == "computing"

    # The artifact the script produced is visible to the filesystem namespace,
    # not just to the process that made it.
    assert fs.read_text("/work/result.txt").strip() == "42"

    # ...and to a later, independent exec.
    assert run(sandbox, "cat", "/work/result.txt") == "42"


def test_state_written_by_one_exec_survives_into_the_next(sandbox):
    """A sandbox is a durable environment, not a series of fresh containers."""
    run(sandbox, "sh", "-c", "mkdir -p /state && echo first > /state/log")
    run(sandbox, "sh", "-c", "echo second >> /state/log")

    assert run(sandbox, "cat", "/state/log").splitlines() == ["first", "second"]


def test_a_workflow_reads_back_what_it_uploaded(sandbox, tmp_path):
    """Round trip through the host: upload, transform in the sandbox, download."""
    source = tmp_path / "input.txt"
    source.write_text("alpha\nbeta\ngamma\n")
    fs = sandbox.filesystem

    fs.copy_from_local(str(source), "/work/input.txt")
    run(sandbox, "sh", "-c", "tr a-z A-Z < /work/input.txt > /work/output.txt")

    destination = tmp_path / "output.txt"
    fs.copy_to_local("/work/output.txt", str(destination))

    assert destination.read_text() == "ALPHA\nBETA\nGAMMA\n"


# -- a registry image, end to end ------------------------------------------


def test_a_registry_image_supports_a_whole_workflow():
    """Set a workspace up at runtime, then do real work in it.

    Image building is not implemented, so the setup a recipe used to bake in is
    done with exec and the filesystem API once the sandbox is up -- which is
    what the docs now tell callers to do.
    """
    sandbox = modal.Sandbox.create(image=PYTHON_IMAGE, timeout=600)
    try:
        run(sandbox, "mkdir", "-p", "/opt/workspace")
        sandbox.filesystem.write_text("ready\n", "/opt/workspace/state")
        assert sandbox.filesystem.read_text("/opt/workspace/state").strip() == "ready"

        # The image is a working Python environment on top of it.
        sandbox.filesystem.write_text(
            "import json; print(json.dumps({'ok': True}))", "/opt/workspace/run.py"
        )
        assert run(sandbox, "python", "/opt/workspace/run.py") == '{"ok": true}'

        run(sandbox, "sh", "-c", "echo done >> /opt/workspace/state")
        assert "done" in sandbox.filesystem.read_text("/opt/workspace/state")
    finally:
        sandbox.terminate()


# -- streaming and long-running work ---------------------------------------


def test_output_can_be_consumed_while_the_process_is_still_running(sandbox):
    """A slow producer is readable as it goes, not only once it exits."""
    process = sandbox.exec(
        "sh", "-c", "for i in 1 2 3 4 5; do echo line$i; sleep 0.3; done"
    )

    started = time.monotonic()
    first = next(iter(process.stdout))
    elapsed = time.monotonic() - started

    assert first.strip() == "line1"
    # The whole command takes ~1.5s; the first line must not wait for it.
    assert elapsed < 1.2, f"first line took {elapsed:.2f}s -- output looks buffered"

    assert process.wait() == 0


def test_a_sandbox_terminates_cleanly_with_a_stream_half_consumed(sandbox):
    """Teardown must not depend on the caller having drained the output.

    A workflow that stops reading early -- found what it needed, hit an error --
    would otherwise leave the sandbox wedged.
    """
    process = sandbox.exec("sh", "-c", "for i in $(seq 1 500); do echo $i; done")

    assert next(iter(process.stdout)).strip() == "1"

    # Terminating with hundreds of lines still queued must not hang.
    sandbox.terminate()
    assert sandbox.poll() is not None


def test_a_process_is_interactive_across_several_turns(sandbox):
    """stdin and stdout compose over a conversation, not just one exchange."""
    process = sandbox.exec("sh")

    process.stdin.write(b"echo first\n")
    process.stdin.drain()
    process.stdin.write(b"echo second\n")
    process.stdin.write_eof()
    process.stdin.drain()

    assert process.wait() == 0
    assert process.stdout.read().split() == ["first", "second"]


# -- failure and recovery --------------------------------------------------


def test_a_failed_command_leaves_the_sandbox_usable(sandbox):
    """One bad step must not poison the session.

    An agent retrying after an error is the common case; if the sandbox were
    unusable afterwards the retry would fail for the wrong reason.
    """
    failed = sandbox.exec("sh", "-c", "echo oops >&2; exit 7")
    assert failed.wait() == 7
    assert failed.stderr.read().strip() == "oops"

    assert run(sandbox, "echo", "still-here") == "still-here"


def test_a_workflow_that_raises_midway_still_tears_down():
    """Cleanup is the caller's job here -- assert the idiom actually works."""
    sandbox = modal.Sandbox.create(image=IMAGE, timeout=300)
    try:
        with pytest.raises(ZeroDivisionError):
            try:
                run(sandbox, "echo", "before")
                raise ZeroDivisionError("simulated failure mid-workflow")
            finally:
                sandbox.terminate()
    finally:
        sandbox.terminate()  # idempotent; proves the double call is safe

    assert sandbox.poll() is not None


def test_a_sandbox_that_outlives_its_timeout_reports_it():
    """The deadline is enforced, and reported as the documented 124."""
    sandbox = modal.Sandbox.create("sleep", "infinity", image=IMAGE, timeout=5)
    try:
        with pytest.raises(SandboxTimeoutError):
            sandbox.wait()
        assert sandbox.returncode == 124
    finally:
        sandbox.terminate()


# -- concurrency over one image --------------------------------------------


def test_concurrent_sandboxes_from_one_image_stay_isolated():
    """The storage model, asserted from the outside.

    Every sandbox from an image shares one read-only root filesystem, and since
    the content-addressed store landed, identical files across images share an
    inode. Both are safe only because a container's writes go to its own
    overlay. If that ever stopped holding, this is the test that catches it:
    each sandbox writes to the *same path* and must still read back its own
    value.
    """
    count = 4

    def workload(index):
        sandbox = modal.Sandbox.create(image=IMAGE, timeout=300)
        try:
            marker = f"sandbox-{index}"
            sandbox.filesystem.write_text(marker, "/tmp/shared-path")
            time.sleep(0.2)  # widen the window for interference
            return sandbox.filesystem.read_text("/tmp/shared-path")
        finally:
            sandbox.terminate()

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        results = list(pool.map(workload, range(count)))

    assert results == [f"sandbox-{i}" for i in range(count)]


def test_a_shared_base_file_is_unchanged_after_a_sandbox_writes_over_it():
    """A write must copy up, never modify the image the next sandbox will use."""
    # A file the image actually ships, and one busybox and slim distro images
    # both have. /etc/hostname is not: runsc supplies it per container rather
    # than the image carrying it.
    shared = "/etc/passwd"

    first = modal.Sandbox.create(image=IMAGE, timeout=300)
    try:
        original = first.filesystem.read_text(shared)
        assert original, "the base image should ship this file"
        first.filesystem.write_text("clobbered\n", shared)
        assert first.filesystem.read_text(shared) == "clobbered\n"
    finally:
        first.terminate()

    second = modal.Sandbox.create(image=IMAGE, timeout=300)
    try:
        assert second.filesystem.read_text(shared) == original
    finally:
        second.terminate()


# -- parity with Modal's own suite -----------------------------------------


def test_a_sandbox_without_a_main_command_still_execs():
    """Modal: ``test_sandbox_no_entrypoint``."""
    sandbox = modal.Sandbox.create(image=IMAGE, timeout=300)
    try:
        process = sandbox.exec("echo", "hi")
        process.wait()
        assert process.returncode == 0
        assert process.stdout.read() == "hi\n"
    finally:
        sandbox.terminate()


def test_a_second_read_returns_nothing(sandbox):
    """Modal: ``test_sandbox_exec_output_double_read``.

    Measured on Modal 1.6.1, on both backends: a StreamReader keeps one
    position, so the second ``read()`` returns empty. (1.5.x re-read from the
    first byte every time.)
    """
    process = sandbox.exec("sh", "-c", "echo hi")

    assert process.stdout.read() == "hi\n"
    assert process.stdout.read() == ""
    assert process.wait() == 0


def test_poll_reports_minus_one_once_an_exec_times_out(sandbox):
    """Modal: ``test_sandbox_exec_poll_timeout``."""
    process = sandbox.exec("sleep", "999", timeout=2)

    assert not process.poll()

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        time.sleep(0.2)

    assert process.poll() == -1


def test_partial_output_survives_an_exec_timeout(sandbox):
    """Modal: ``test_sandbox_exec_output_timeout``.

    What the command managed to emit before the deadline is still delivered;
    the timeout is reported as -1 rather than raised.
    """
    started = time.monotonic()
    process = sandbox.exec("sh", "-c", "echo hi; sleep 999", timeout=2)

    assert process.stdout.read() == "hi\n"
    assert process.wait() == -1
    assert time.monotonic() - started < 30


def test_create_exec_poll_wait_in_modals_order(sandbox):
    """Modal: ``test_sandbox_exec_wait``, the shape ported code relies on."""
    process = sandbox.exec("sh", "-c", "sleep 0.5 && exit 42")

    assert process.poll() is None

    started = time.monotonic()
    assert process.wait() == 42
    assert time.monotonic() - started > 0.2

    assert process.poll() == 42


# -- readiness probes ------------------------------------------------------


def test_a_probe_waits_for_setup_the_main_process_has_not_finished_yet():
    """The point of a probe: the sandbox is up long before it is *ready*, and
    wait_until_ready() spans the difference."""
    sandbox = modal.Sandbox.create(
        "sh",
        "-c",
        "sleep 3 && touch /tmp/ready && sleep 300",
        image=IMAGE,
        timeout=300,
        readiness_probe=modal.Probe.with_exec(
            "sh", "-c", "test -f /tmp/ready", interval_ms=100
        ),
    )
    try:
        # Not ready yet: the sandbox exists, but its setup has not run.
        assert sandbox.exec("sh", "-c", "test -f /tmp/ready").wait() != 0

        started = time.monotonic()
        sandbox.wait_until_ready(timeout=60)
        elapsed = time.monotonic() - started

        # It waited for the setup rather than returning early...
        assert elapsed > 1
        # ...and once it returns, the condition really does hold.
        assert sandbox.exec("sh", "-c", "test -f /tmp/ready").wait() == 0
        assert sandbox.poll() is None

        # Readiness is monotonic, so a second wait is immediate.
        started = time.monotonic()
        sandbox.wait_until_ready(timeout=60)
        assert time.monotonic() - started < 1
    finally:
        sandbox.terminate()


def test_a_hung_probe_attempt_does_not_block_readiness():
    """Measured on Modal: with an attempt that would hang for ten minutes, the
    sandbox was ready at ~11s. Each attempt is bounded, so the next one runs;
    unbounded, the first attempt held readiness until wait_until_ready timed
    out. The hung attempt is killed rather than left running."""
    sandbox = modal.Sandbox.create(
        "sh",
        "-c",
        "sleep 3 && touch /tmp/ready && sleep 300",
        image=IMAGE,
        timeout=300,
        readiness_probe=modal.Probe.with_exec(
            "sh", "-c", "test -f /tmp/ready || sleep 600"
        ),
    )
    try:
        started = time.monotonic()
        sandbox.wait_until_ready(timeout=60)
        assert time.monotonic() - started < 30
        survivors = sandbox.exec(
            "sh", "-c", "ps | grep 'sleep 60[0]' | wc -l"
        ).stdout.read()
        assert survivors.strip() == "0"
    finally:
        sandbox.terminate()


def test_a_probe_that_never_passes_times_out_and_leaves_the_sandbox_usable():
    sandbox = modal.Sandbox.create(
        "sleep",
        "300",
        image=IMAGE,
        timeout=300,
        readiness_probe=modal.Probe.with_exec("test", "-f", "/tmp/never"),
    )
    try:
        with pytest.raises(modal.TimeoutError) as excinfo:
            sandbox.wait_until_ready(timeout=2)
        # A probe that has not passed is not the sandbox exceeding its lifetime.
        assert not isinstance(excinfo.value, SandboxTimeoutError)

        assert run(sandbox, "echo", "hi") == "hi"
        assert sandbox.poll() is None
    finally:
        sandbox.terminate()


def test_probing_does_not_evict_the_output_of_the_callers_own_execs():
    """Each attempt is released rather than retired, so a fast probe cannot
    push a caller's finished execs out of the actor's retention window."""
    sandbox = modal.Sandbox.create(
        "sleep",
        "300",
        image=IMAGE,
        timeout=300,
        readiness_probe=modal.Probe.with_exec("test", "-f", "/tmp/never"),
    )
    try:
        process = sandbox.exec("sh", "-c", "echo mine")
        process.wait()
        # Long enough for a 100ms probe to cycle the whole retention window.
        time.sleep(8)
        assert process.stdout.read() == "mine\n"
    finally:
        sandbox.terminate()


def test_terminating_mid_probe_raises_rather_than_hanging():
    """A waiter parked on a probe must be woken by teardown, not left to sit
    until its own timeout."""
    sandbox = modal.Sandbox.create(
        "sleep",
        "300",
        image=IMAGE,
        timeout=300,
        readiness_probe=modal.Probe.with_exec("test", "-f", "/tmp/never"),
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        waiter = pool.submit(sandbox.wait_until_ready, timeout=120)
        time.sleep(2)
        sandbox.terminate()
        with pytest.raises(SandboxTerminatedError):
            waiter.result(timeout=30)


def test_a_sandbox_with_no_main_command_can_still_be_probed():
    """Nothing calls launch_main on this path, so the probe has to start on
    demand or the wait would never return."""
    sandbox = modal.Sandbox.create(
        image=IMAGE,
        timeout=300,
        readiness_probe=modal.Probe.with_exec("echo", "ready"),
    )
    try:
        sandbox.wait_until_ready(timeout=30)
    finally:
        sandbox.terminate()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
