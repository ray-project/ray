"""Integration tests for the Modal-compatible Sandbox API.

These need runsc and a Ray cluster, so they run only under ``TEST_SANDBOX=1``
(enforced by this directory's conftest).
"""

import asyncio
import concurrent.futures
import sys
import time

import pytest

import ray
from ray.experimental.sandbox import modal
from ray.experimental.sandbox.modal.exception import (
    InvalidError,
    SandboxTerminatedError,
    SandboxTimeoutError,
)

# Sandboxes here run on Modal's default network, network="public", which
# needs slirp4netns and a host that allows a per-sandbox network namespace.
pytestmark = pytest.mark.usefixtures("ensure_slirp4netns")

IMAGE = "busybox:latest"


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    """One Ray cluster for the whole module; starting one costs seconds."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield


@pytest.fixture
def sandbox():
    """An idle sandbox, torn down even if the test fails."""
    sb = modal.Sandbox.create(image=IMAGE, timeout=120)
    try:
        yield sb
    finally:
        sb.terminate()


# -- basics ----------------------------------------------------------------


def test_create_returns_a_sandbox_with_an_id(sandbox):
    assert isinstance(sandbox, modal.Sandbox)
    assert sandbox.object_id.startswith("ray-sandbox-")


def test_exec_captures_stdout(sandbox):
    process = sandbox.exec("echo", "hello world")
    assert process.stdout.read() == "hello world\n"
    assert process.wait() == 0


def test_exec_captures_stderr_separately(sandbox):
    process = sandbox.exec("sh", "-c", "echo out; echo err >&2")
    assert process.stdout.read() == "out\n"
    assert process.stderr.read() == "err\n"
    assert process.wait() == 0


def test_exec_reports_a_nonzero_exit_code(sandbox):
    process = sandbox.exec("sh", "-c", "exit 3")
    assert process.wait() == 3
    assert process.returncode == 3


def test_exec_reports_a_signal_as_128_plus_signal(sandbox):
    """Matches Modal's convention for signal-terminated processes."""
    process = sandbox.exec("sh", "-c", "kill -9 $$")
    assert process.wait() == 137


def test_returncode_before_wait_is_rejected(sandbox):
    process = sandbox.exec("echo", "hi")
    with pytest.raises(InvalidError, match="must call wait"):
        _ = process.returncode
    process.wait()


def test_poll_returns_none_while_running_then_the_exit_code(sandbox):
    process = sandbox.exec("sh", "-c", "sleep 1; exit 5")
    assert process.poll() is None
    assert process.wait() == 5
    assert process.poll() == 5


def test_env_variables_reach_the_command(sandbox):
    process = sandbox.exec("sh", "-c", "echo $GREETING", env={"GREETING": "hola"})
    assert process.stdout.read().strip() == "hola"


def test_workdir_applies_to_the_command(sandbox):
    process = sandbox.exec("pwd", workdir="/tmp")
    assert process.stdout.read().strip() == "/tmp"


@pytest.mark.parametrize("args", [(), (b"not-a-str",), ("ok", 5)])
def test_exec_rejects_bad_arguments(sandbox, args):
    with pytest.raises(InvalidError):
        sandbox.exec(*args)


# -- streaming -------------------------------------------------------------


def test_output_arrives_incrementally_rather_than_all_at_once():
    """The whole point of the streaming path: no waiting for exit."""
    sb = modal.Sandbox.create(image=IMAGE, timeout=120)
    try:
        process = sb.exec(
            "sh", "-c", "for i in 1 2 3; do echo line $i; sleep 0.4; done"
        )
        start = time.monotonic()
        arrivals = []
        for line in process.stdout:
            arrivals.append((line, time.monotonic() - start))
        process.wait()

        assert [line for line, _ in arrivals] == [
            "line 1\n",
            "line 2\n",
            "line 3\n",
        ]
        # The first line must land well before the command finishes.
        assert arrivals[0][1] < arrivals[-1][1] - 0.3
    finally:
        sb.terminate()


def test_iteration_is_line_buffered_by_default_on_sandbox_streams(sandbox):
    process = sandbox.exec("sh", "-c", "printf 'a\\nb\\nc\\n'", bufsize=1)
    assert list(process.stdout) == ["a\n", "b\n", "c\n"]
    process.wait()


def test_text_false_yields_bytes(sandbox):
    process = sandbox.exec("echo", "raw", text=False)
    assert process.stdout.read() == b"raw\n"
    process.wait()


def test_line_buffering_requires_text_mode(sandbox):
    with pytest.raises(ValueError, match="line-buffering"):
        sandbox.exec("echo", "x", text=False, bufsize=1)


@pytest.mark.parametrize("bufsize", [0, 2, 4096])
def test_unsupported_bufsize_values_are_rejected(sandbox, bufsize):
    with pytest.raises(InvalidError, match="bufsize"):
        sandbox.exec("echo", "x", bufsize=bufsize)


def test_devnull_discards_output_and_refuses_reads(sandbox):
    process = sandbox.exec("sh", "-c", "echo noisy", stdout=modal.StreamType.DEVNULL)
    with pytest.raises(ValueError, match="DEVNULL"):
        process.stdout.read()
    assert process.wait() == 0


def test_stdout_stream_type_prints_locally_and_refuses_reads(sandbox, capfd):
    process = sandbox.exec("echo", "printed-locally", stdout=modal.StreamType.STDOUT)
    assert process.wait() == 0
    with pytest.raises(InvalidError, match="PIPE"):
        process.stdout.read()
    # The pump runs on the synchronizer's loop; give it a moment to flush.
    time.sleep(0.5)
    assert "printed-locally" in capfd.readouterr().out


def test_stdin_round_trips(sandbox):
    process = sandbox.exec("cat")
    process.stdin.write(b"echoed back\n")
    process.stdin.write_eof()
    process.stdin.drain()
    assert process.stdout.read() == "echoed back\n"
    assert process.wait() == 0


def test_large_output_is_delivered_intact(sandbox):
    process = sandbox.exec(
        "sh", "-c", "i=0; while [ $i -lt 2000 ]; do echo $i; i=$((i+1)); done"
    )
    lines = process.stdout.read().splitlines()
    assert process.wait() == 0
    assert len(lines) == 2000
    assert lines[0] == "0" and lines[-1] == "1999"


def test_exec_timeout_yields_minus_one_without_raising(sandbox):
    """Modal reports an exec deadline as -1 rather than an exception."""
    process = sandbox.exec("sleep", "30", timeout=2)
    assert process.wait() == -1


def test_exec_timeout_stops_the_command_and_what_it_started(sandbox):
    """Modal kills a command at its deadline. Killing the runsc client, all
    that used to happen, left it running on with its side effects."""
    process = sandbox.exec(
        "sh",
        "-c",
        "(sleep 3; touch /tmp/ran-late) & sleep 3; touch /tmp/ran-late",
        timeout=1,
    )
    assert process.wait() == -1
    time.sleep(4)
    check = sandbox.exec("sh", "-c", "test -e /tmp/ran-late && echo yes || echo no")
    assert check.stdout.read().strip() == "no"


def test_many_sequential_execs_are_spawned_and_reaped(sandbox):
    """Guards the asyncio child watcher on the actor's non-main-thread loop.

    If child reaping misbehaves there, this fails or hangs rather than
    surfacing much later as a leak.
    """
    for i in range(25):
        process = sandbox.exec("echo", str(i))
        assert process.stdout.read() == f"{i}\n"
        assert process.wait() == 0


def test_concurrent_execs_do_not_block_each_other(sandbox):
    slow = sandbox.exec("sh", "-c", "sleep 2; echo slow")
    fast = sandbox.exec("echo", "fast")
    # The fast command must complete while the slow one is still running.
    assert fast.stdout.read() == "fast\n"
    assert fast.wait() == 0
    assert slow.poll() is None
    assert slow.wait() == 0


# -- sandbox lifecycle -----------------------------------------------------


def test_main_process_streams_belong_to_the_sandbox():
    sb = modal.Sandbox.create("echo", "from main", image=IMAGE, timeout=60)
    try:
        assert sb.stdout.read() == "from main\n"
        sb.wait()
        assert sb.returncode == 0
    finally:
        sb.terminate()


def test_terminate_reports_137(sandbox):
    assert sandbox.terminate(wait=True) == 137


def test_terminate_preserves_a_finished_exit_code():
    sb = modal.Sandbox.create("sh", "-c", "exit 7", image=IMAGE, timeout=60)
    sb.wait(raise_on_termination=False)
    assert sb.terminate(wait=True) == 7


def test_waiting_after_terminating_a_finished_sandbox_does_not_raise():
    """Terminating a sandbox that already exited must not rewrite its outcome.

    terminate() reclaims the actor, so a later wait() answers from cached
    state. That cache has to carry *why* the sandbox ended: reporting a clean
    exit as a termination contradicts what wait() says before terminate(), and
    what the actor recorded.
    """
    sb = modal.Sandbox.create("sh", "-c", "exit 0", image=IMAGE, timeout=60)
    sb.wait()
    sb.terminate()

    # Still finished-on-its-own, not terminated -- so no exception either way.
    sb.wait()
    sb.wait(raise_on_termination=False)
    assert sb.poll() == 0


def test_waiting_after_terminating_a_running_sandbox_still_raises():
    """The converse: a sandbox cut short really was terminated."""
    sb = modal.Sandbox.create("sleep", "infinity", image=IMAGE, timeout=60)
    sb.terminate()

    with pytest.raises(SandboxTerminatedError):
        sb.wait()
    sb.wait(raise_on_termination=False)


def test_terminate_is_idempotent(sandbox):
    sandbox.terminate()
    sandbox.terminate()


def test_wait_raises_on_external_termination():
    sb = modal.Sandbox.create("sleep", "60", image=IMAGE, timeout=60)
    sb.terminate()
    with pytest.raises(SandboxTerminatedError):
        sb.wait()
    assert sb.returncode == 137


def test_a_wait_parked_while_another_thread_terminates_reports_termination():
    """terminate() kills the actor as soon as it is done. wait() used to make a
    second call for the exit reason, which could land on the dead actor and
    report the sandbox gone instead of terminated."""
    sb = modal.Sandbox.create("sleep", "60", image=IMAGE, timeout=60)
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        waiting = pool.submit(sb.wait)
        time.sleep(1)
        sb.terminate()
        with pytest.raises(SandboxTerminatedError):
            waiting.result(timeout=30)
    assert sb.returncode == 137


def test_wait_can_be_told_not_to_raise_on_termination():
    sb = modal.Sandbox.create("sleep", "60", image=IMAGE, timeout=60)
    sb.terminate()
    sb.wait(raise_on_termination=False)
    assert sb.returncode == 137


def test_sandbox_timeout_raises_and_reports_124():
    sb = modal.Sandbox.create("sleep", "60", image=IMAGE, timeout=3)
    try:
        with pytest.raises(SandboxTimeoutError):
            sb.wait()
        assert sb.returncode == 124
    finally:
        sb.terminate()


# -- configuration ---------------------------------------------------------


@pytest.mark.parametrize("block_network", [True, False])
def test_block_network_produces_a_usable_sandbox(block_network):
    """Both network modes must still yield a sandbox commands can run in."""
    sb = modal.Sandbox.create(image=IMAGE, timeout=60, block_network=block_network)
    try:
        assert sb.exec("echo", "ok").stdout.read() == "ok\n"
    finally:
        sb.terminate()


def test_an_explicit_app_none_is_accepted():
    """Ported code passing app=None still runs."""
    sb = modal.Sandbox.create(image=IMAGE, app=None, name="named", timeout=60)
    try:
        assert sb.exec("echo", "ok").stdout.read() == "ok\n"
    finally:
        sb.terminate()


def test_a_looked_up_app_is_accepted():
    """Modal's canonical snippet -- which always passes an App -- runs as-is."""
    app = modal.App.lookup("sandbox-app", create_if_missing=True)
    sb = modal.Sandbox.create(image=IMAGE, app=app, timeout=60)
    try:
        assert sb.exec("echo", "ok").stdout.read() == "ok\n"
    finally:
        sb.terminate()


def test_exec_after_the_main_process_exits_is_refused():
    """Measured on Modal: the entrypoint exiting ends the Sandbox, so a later
    exec raises NotFoundError while the finished output stays readable."""
    sb = modal.Sandbox.create("echo", "done", image=IMAGE, timeout=60)
    try:
        sb.wait()
        assert sb.stdout.read() == "done\n"
        with pytest.raises(modal.NotFoundError):
            sb.exec("echo", "late")
    finally:
        sb.terminate()


def test_image_object_is_accepted():
    sb = modal.Sandbox.create(image=modal.Image.from_registry(IMAGE), timeout=60)
    try:
        assert sb.exec("echo", "ok").stdout.read() == "ok\n"
    finally:
        sb.terminate()


# -- async surface ---------------------------------------------------------


def test_the_async_surface_mirrors_the_blocking_one():
    async def main():
        sb = await modal.Sandbox.create.aio(image=IMAGE, timeout=60)
        try:
            process = await sb.exec.aio(
                "sh", "-c", "for i in 1 2 3; do echo line $i; done", bufsize=1
            )
            lines = [line async for line in process.stdout]
            assert lines == ["line 1\n", "line 2\n", "line 3\n"]
            assert await process.wait.aio() == 0

            await sb.filesystem.write_text.aio("async\n", "/tmp/async.txt")
            assert await sb.filesystem.read_text.aio("/tmp/async.txt") == "async\n"
        finally:
            await sb.terminate.aio()

    asyncio.run(main())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
