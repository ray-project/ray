"""Output retention, end to end on the gVisor backend.

Reading consumes, as Modal's client does from 1.6: each byte is delivered once,
what a reader has is freed, and what nobody reads is held in the sandbox
actor's memory up to a bound. The unit tests in ``modal/tests`` drive
``_OutputBuffer`` and the actor directly; these run real commands in a real
sandbox and read their output back through Ray, so the pipe, the pump, the
acknowledgements and the streaming generator are exercised together.

Needs runsc and a Ray cluster, so it runs only under ``TEST_SANDBOX=1``
(enforced by this directory's conftest).
"""

import subprocess
import sys
import time

import pytest

import ray
from ray.experimental.sandbox import modal
from ray.experimental.sandbox.modal._actor import (
    _EXEC_OUTPUT_CAP,
    _MAIN_OUTPUT_CAP,
    _SANDBOX_OUTPUT_CAP,
)

# Sandboxes here run on Modal's default network, network="public", which
# needs slirp4netns and a host that allows a per-sandbox network namespace.
pytestmark = pytest.mark.usefixtures("ensure_slirp4netns")

# GNU coreutils: its seq writes hundreds of MiB a second inside gVisor, where
# busybox's managed about 12, and prints the same bytes as the host's seq.
IMAGE = "python:3.13-slim"
_MIB = 1024 * 1024

# About 290 MiB: past the Sandbox's own 256 MiB window.
_MAIN_LINES = 35_000_000

# About 68 MiB: past an exec stream's 64 MiB.
_EXEC_LINES = 9_000_000


def seq_output(n: int) -> str:
    """What ``seq 1 n`` prints, from the host's GNU seq: numbered lines, so a
    gap or splice shows."""
    return subprocess.run(
        ["seq", "1", str(n)], check=True, capture_output=True, text=True
    ).stdout


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    if not ray.is_initialized():
        ray.init()
    yield


@pytest.fixture
def sandbox():
    """An idle sandbox, torn down even when the test fails."""
    sb = modal.Sandbox.create(image=IMAGE, timeout=300)
    try:
        yield sb
    finally:
        sb.terminate()


def on_actor(sandbox, fn):
    """Run ``fn(actor)`` inside the sandbox's actor and return its result."""
    return ray.get(sandbox._impl._actor.__ray_call__.remote(fn))


def test_the_sandbox_keeps_exactly_the_newest_256_mib_of_its_own_unread_output():
    """Measured on Modal's V2 backend: the newest 256 MiB, the rest reported.
    Read once, it is gone: a second read() returns nothing, as on Modal 1.6."""
    expected = seq_output(_MAIN_LINES)
    sandbox = modal.Sandbox.create(
        "seq", "1", str(_MAIN_LINES), image=IMAGE, timeout=300
    )
    try:
        sandbox.wait()
        assert sandbox.returncode == 0
        assert sandbox.stdout.read() == expected[-_MAIN_OUTPUT_CAP:]
        assert sandbox.stdout.truncated
        assert sandbox.stdout.bytes_lost == len(expected) - _MAIN_OUTPUT_CAP
        assert sandbox.stdout.read() == ""
    finally:
        sandbox.terminate()


def test_an_exec_nobody_reads_keeps_its_newest_64_mib(sandbox):
    expected = seq_output(_EXEC_LINES)
    process = sandbox.exec("seq", "1", str(_EXEC_LINES))
    assert process.wait() == 0

    assert process.stdout.read() == expected[-_EXEC_OUTPUT_CAP:]
    assert process.stdout.truncated
    assert process.stdout.bytes_lost == len(expected) - _EXEC_OUTPUT_CAP


def test_an_exec_read_while_it_runs_arrives_whole_however_large(sandbox):
    """What the reader has is freed as it goes, so the caps bound only what is
    unread: 290 MiB through a 64 MiB cap arrives whole."""
    expected = seq_output(_MAIN_LINES)
    process = sandbox.exec("seq", "1", str(_MAIN_LINES))

    assert process.stdout.read() == expected
    assert not process.stdout.truncated
    assert process.wait() == 0


def test_a_read_after_breaking_out_of_a_loop_continues_exactly(sandbox):
    """Measured on Modal 1.6.1: `break` after 10 lines, then read(), returned
    exactly from line 11 -- and nothing on a second read()."""
    expected = seq_output(200_000)
    process = sandbox.exec("seq", "1", "200000", bufsize=1)
    head = []
    for line in process.stdout:
        head.append(line)
        if len(head) == 10:
            break

    rest = process.stdout.read()

    assert "".join(head) + rest == expected
    assert process.stdout.read() == ""
    assert process.wait() == 0


def test_an_exec_read_to_the_end_is_released_and_still_reports_its_code(sandbox):
    process = sandbox.exec("sh", "-c", "echo out; echo err >&2; exit 3")
    assert process.stdout.read() == "out\n"
    assert process.stderr.read() == "err\n"
    exec_id = process._impl._exec_id

    # The acknowledgement is sent without waiting, so give it a moment.
    deadline = time.monotonic() + 10
    while on_actor(sandbox, lambda actor: exec_id in actor._execs):
        assert time.monotonic() < deadline, "the session was not released"
        time.sleep(0.1)
    assert process.wait() == 3


def test_unread_output_across_many_execs_stays_within_the_sandbox_cap(sandbox):
    """Ten commands of 100 MiB each, none read: the actor holds at most the
    sandbox-wide cap, oldest dropped first, and its memory says so."""
    processes = [
        sandbox.exec("head", "-c", str(100 * _MIB), "/dev/zero", text=False)
        for _ in range(10)
    ]
    for process in processes:
        assert process.wait() == 0

    held, rss = on_actor(
        sandbox,
        lambda actor: (
            actor._output_budget.used,
            int(
                next(
                    line.split()[1]
                    for line in open("/proc/self/status")
                    if line.startswith("VmRSS:")
                )
            )
            * 1024,
        ),
    )
    assert held <= _SANDBOX_OUTPUT_CAP
    # The cap plus the actor's own footprint, with room for allocator slack.
    assert rss < _SANDBOX_OUTPUT_CAP + 512 * _MIB

    # Run together, the ten compete for the sandbox-wide cap, so no single one
    # is promised its full window -- only that what comes back is within the
    # caps, intact, and that what went is reported.
    kept = [process.stdout.read() for process in processes]
    assert sum(len(output) for output in kept) <= _SANDBOX_OUTPUT_CAP
    assert all(len(output) <= _EXEC_OUTPUT_CAP for output in kept)
    assert all(output.count(0) == len(output) for output in kept)
    assert all(process.stdout.truncated for process in processes)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
