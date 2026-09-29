"""Output spilling, end to end on the gVisor backend.

The unit tests in ``modal/tests`` drive ``_OutputBuffer`` and ``_SpillStore``
directly. These run real commands in a real sandbox and read their output back
through Ray, so the pipe, the pump, the spill files and the streaming generator
are exercised together -- including the seam where a reader passes from spilled
output to what is still in memory.

Needs runsc and a Ray cluster, so it runs only under ``TEST_SANDBOX=1``
(enforced by this directory's conftest).
"""

import os
import shutil
import sys
import time

import pytest

import ray
from ray.experimental.sandbox import modal
from ray.experimental.sandbox.modal._actor import (
    _DEFAULT_BUFFER_LIMIT,
    _MAIN_OUTPUT_RETAIN,
    _RAY_DISK_FULL_FREE_FRACTION,
    _SPILL_MIN_FREE_ENV,
    _SPILL_ROOT,
    _SPILL_SEGMENT_BYTES,
)

IMAGE = "busybox:latest"
_MIB = 1024 * 1024

# About 37 MiB: several times what a stream holds in memory, so most of it has
# to be spilled, across more than one file.
_EXEC_LINES = 5_000_000

# About 290 MiB, past the Sandbox's own 256 MiB window.
_MAIN_LINES = 35_000_000

# Spill room the tests need at once. Each deletes its output before the next.
_ROOM_NEEDED = 320 * _MIB


def seq_output(n: int) -> str:
    """What ``seq 1 n`` prints: numbered lines, so a gap or splice shows."""
    return "\n".join(map(str, range(1, n + 1))) + "\n"


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    """A cluster whose sandboxes may spill down to just above Ray's own line.

    The default floor also keeps headroom for Ray's own object spilling, which
    on a developer machine with a nearly full disk leaves nothing to spill
    into. The floor is covered by the unit tests; what is under test here is
    the spilling itself, so this keeps only Ray's disk-full line, and a small
    margin, clear.

    The floor is read from the sandbox actor's environment, which a cluster
    started earlier would not carry, so this one starts its own.
    """
    os.makedirs(_SPILL_ROOT, exist_ok=True)
    total, _, free = shutil.disk_usage(_SPILL_ROOT)
    floor = int(total * _RAY_DISK_FULL_FREE_FRACTION) + 256 * _MIB
    if free - floor < _ROOM_NEEDED:
        pytest.skip(
            f"{_SPILL_ROOT} has {(free - floor) // _MIB} MiB free above Ray's "
            f"disk-full line, and these tests spill up to "
            f"{_ROOM_NEEDED // _MIB} MiB"
        )
    ray.shutdown()
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv(_SPILL_MIN_FREE_ENV, str(floor))
        ray.init()
        yield
        ray.shutdown()


@pytest.fixture
def sandbox():
    """An idle sandbox, torn down even when the test fails."""
    sb = modal.Sandbox.create(image=IMAGE, timeout=300)
    try:
        yield sb
    finally:
        sb.terminate()


def spill_directory(sandbox) -> str:
    return ray.get(
        sandbox._impl._actor.__ray_call__.remote(lambda actor: actor._spill.directory)
    )


def spill_file_sizes(directory: str):
    return [
        os.path.getsize(os.path.join(directory, name))
        for name in sorted(os.listdir(directory))
    ]


def test_output_nobody_reads_is_spilled_and_read_back_whole_twice():
    """Modal keeps an exec's whole output, and read() starts from byte 0.

    Nobody reads while the command runs, so everything past the memory tail
    has to be on disk by the time it exits -- and the command must not have
    been held back waiting for a reader. terminate() then deletes the files.
    """
    expected = seq_output(_EXEC_LINES)
    sandbox = modal.Sandbox.create(image=IMAGE, timeout=300)
    try:
        directory = spill_directory(sandbox)
        process = sandbox.exec("seq", "1", str(_EXEC_LINES))
        assert process.wait() == 0

        sizes = spill_file_sizes(directory)
        assert len(sizes) > 1
        assert max(sizes) <= _SPILL_SEGMENT_BYTES
        assert sum(sizes) >= len(expected) - 2 * _DEFAULT_BUFFER_LIMIT
        assert os.stat(directory).st_mode & 0o777 == 0o700

        assert process.stdout.read() == expected
        assert process.stdout.read() == expected
        assert not process.stdout.truncated
    finally:
        sandbox.terminate()
    assert not os.path.exists(directory)


def test_a_reader_that_falls_behind_crosses_from_disk_to_memory_without_a_gap(
    sandbox,
):
    """Iterating while the command runs, but slower than it writes.

    The pause lets the command get far enough ahead that its oldest output is
    spilled, so the rest of the pass reads from disk and then from memory.
    """
    directory = spill_directory(sandbox)
    process = sandbox.exec("seq", "1", str(_EXEC_LINES))
    chunks = iter(process.stdout)
    received = [next(chunks)]
    # The directory is created by the first spill.
    deadline = time.monotonic() + 60
    while not (os.path.isdir(directory) and os.listdir(directory)):
        assert time.monotonic() < deadline, "nothing was spilled"
        time.sleep(0.1)

    received.extend(chunks)
    assert "".join(received) == seq_output(_EXEC_LINES)
    assert not process.stdout.truncated
    assert process.wait() == 0


def test_the_sandbox_keeps_the_newest_256_mib_of_its_own_output():
    """Measured on Modal's V2 backend: the newest 256 MiB, the rest reported.

    The window moves while the main process writes, so this also covers
    spilled output being deleted from the front as it ages out.
    """
    expected = seq_output(_MAIN_LINES)
    sandbox = modal.Sandbox.create(
        "seq", "1", str(_MAIN_LINES), image=IMAGE, timeout=300
    )
    try:
        directory = spill_directory(sandbox)
        sandbox.wait()
        assert sandbox.returncode == 0
        # Files go whole, so the oldest can reach back one file past the window.
        spilled = sum(spill_file_sizes(directory))
        assert spilled <= _MAIN_OUTPUT_RETAIN + _SPILL_SEGMENT_BYTES

        # Twice: the window is retained, not consumed.
        for _ in range(2):
            assert sandbox.stdout.read() == expected[-_MAIN_OUTPUT_RETAIN:]
        assert sandbox.stdout.truncated
        assert sandbox.stdout.bytes_lost == len(expected) - _MAIN_OUTPUT_RETAIN
    finally:
        sandbox.terminate()
    assert not os.path.exists(directory)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
