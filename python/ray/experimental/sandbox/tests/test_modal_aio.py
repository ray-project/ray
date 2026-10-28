"""The ``.aio`` surface of the Modal-compatible API, against real sandboxes.

``.aio`` calls run on the same background loop as blocking ones, as Modal's
synchronicity runs them, so a Sandbox and its streams are no longer bound to
the event loop that first touched them. These tests hold that to account
where it matters: streams read across ``asyncio.run()`` calls and across the
two surfaces, concurrency from one loop and from many threads, cancellation in
the middle of a read, and a process's printing outliving the caller's loop.

They need runsc and a Ray cluster, so they run only under ``TEST_SANDBOX=1``
(enforced by this directory's conftest).
"""

import asyncio
import concurrent.futures
import os
import subprocess
import sys
import textwrap
import time

import pytest

import ray
from ray.experimental.sandbox import modal
from ray.experimental.sandbox.modal.stream_type import StreamType

pytestmark = pytest.mark.usefixtures("ensure_slirp4netns")

IMAGE = "busybox:latest"


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield


@pytest.fixture(scope="module")
def sandbox():
    sb = modal.Sandbox.create(image=IMAGE, timeout=600)
    try:
        yield sb
    finally:
        sb.terminate()


def _numbers(count, delay=0.0):
    """A command printing 1..count, one per line, optionally paced."""
    pause = f"; sleep {delay}" if delay else ""
    return [
        "sh",
        "-c",
        f"i=1; while [ $i -le {count} ]; do echo $i{pause}; i=$((i+1)); done",
    ]


def _expected(count):
    return "".join(f"{i}\n" for i in range(1, count + 1))


def test_a_sandbox_made_in_one_asyncio_run_is_used_in_the_next():
    sb = asyncio.run(modal.Sandbox.create.aio(image=IMAGE, timeout=120))
    try:

        async def use():
            process = await sb.exec.aio("echo", "second run")
            return await process.stdout.read.aio(), await process.wait.aio()

        assert asyncio.run(use()) == ("second run\n", 0)
    finally:
        asyncio.run(sb.terminate.aio())


def test_a_stream_read_part_way_resumes_in_a_later_asyncio_run(sandbox):
    """It used to be bound to the first run's loop: the second run raised."""
    process = sandbox.exec(*_numbers(300))

    async def first_ten():
        lines = []
        async for line in process.stdout:
            lines.append(line)
            if len(lines) == 10:
                break
        return lines

    head = asyncio.run(first_ten())

    async def rest():
        return await process.stdout.read.aio()

    assert "".join(head) + asyncio.run(rest()) == _expected(300)
    assert process.wait() == 0


def test_blocking_and_aio_reads_continue_one_stream(sandbox):
    process = sandbox.exec(*_numbers(400))
    lines = []
    for line in process.stdout:
        lines.append(line)
        if len(lines) == 50:
            break

    async def more():
        got = []
        async for line in process.stdout:
            got.append(line)
            if len(got) == 50:
                break
        return got

    lines += asyncio.run(more())
    lines.append(process.stdout.read())
    assert "".join(lines) == _expected(400)


def test_the_main_process_stream_spans_asyncio_runs():
    sb = modal.Sandbox.create(*_numbers(500), image=IMAGE, timeout=120)
    try:

        async def head():
            got = []
            async for line in sb.stdout:
                got.append(line)
                if len(got) == 100:
                    break
            return got

        first = asyncio.run(head())
        assert "".join(first) + asyncio.run(sb.stdout.read.aio()) == _expected(500)
    finally:
        sb.terminate()


def test_many_aio_execs_from_one_loop_run_concurrently(sandbox):
    async def one(i):
        process = await sandbox.exec.aio("sh", "-c", f"sleep 1; echo {i}; exit {i % 3}")
        return await process.stdout.read.aio(), await process.wait.aio()

    async def main():
        started = time.monotonic()
        results = await asyncio.gather(*(one(i) for i in range(8)))
        return results, time.monotonic() - started

    results, elapsed = asyncio.run(main())
    assert results == [(f"{i}\n", i % 3) for i in range(8)]
    # Concurrent, not one after another: eight one-second commands.
    assert elapsed < 6, elapsed


def test_blocking_and_aio_callers_on_many_threads_share_one_sandbox(sandbox):
    """Each worker runs its own commands; none sees another's bytes."""

    def blocking_worker(n):
        out = []
        for i in range(5):
            process = sandbox.exec("sh", "-c", f"seq 1 200 | sed 's/^/b{n}-{i}-/'")
            out.append(process.stdout.read())
            assert process.wait() == 0
        return out

    def aio_worker(n):
        async def main():
            out = []
            for i in range(5):
                process = await sandbox.exec.aio(
                    "sh", "-c", f"seq 1 200 | sed 's/^/a{n}-{i}-/'"
                )
                lines = [line async for line in process.stdout]
                out.append("".join(lines))
                assert await process.wait.aio() == 0
            return out

        return asyncio.run(main())

    with concurrent.futures.ThreadPoolExecutor(6) as pool:
        futures = {("b", n): pool.submit(blocking_worker, n) for n in range(3)} | {
            ("a", n): pool.submit(aio_worker, n) for n in range(3)
        }
        for (kind, n), future in futures.items():
            for i, text in enumerate(future.result(timeout=300)):
                assert text == "".join(f"{kind}{n}-{i}-{j}\n" for j in range(1, 201))


def test_cancelling_an_aio_read_mid_stream_keeps_its_place(sandbox):
    """The cancelled step's chunk is neither lost nor delivered twice."""
    process = sandbox.exec(*_numbers(60, delay=0.05))

    async def read_some():
        got = []

        async def reader():
            async for line in process.stdout:
                got.append(line)

        try:
            await asyncio.wait_for(reader(), 1.0)
        except asyncio.TimeoutError:
            pass
        return got

    head = asyncio.run(read_some())
    assert 0 < len(head) < 60
    rest = asyncio.run(process.stdout.read.aio())
    assert "".join(head) + rest == _expected(60)


def test_a_timeout_around_wait_leaves_the_command_and_sandbox_usable(sandbox):
    process = sandbox.exec("sh", "-c", "sleep 2; echo late")

    async def impatient():
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(process.wait.aio(), 0.3)

    asyncio.run(impatient())
    assert process.poll() is None
    assert process.stdout.read() == "late\n"
    assert process.wait() == 0
    assert sandbox.exec("echo", "still fine").stdout.read() == "still fine\n"


def test_stdin_through_aio(sandbox):
    async def main():
        process = await sandbox.exec.aio("cat")
        process.stdin.write(b"hello ")
        await process.stdin.drain.aio()
        process.stdin.write("world\n")
        process.stdin.write_eof()
        await process.stdin.drain.aio()
        return await process.stdout.read.aio(), await process.wait.aio()

    assert asyncio.run(main()) == ("hello world\n", 0)


def test_filesystem_calls_through_aio_run_concurrently(sandbox):
    async def one(i):
        path = f"/tmp/aio-fs/{i}.txt"
        await sandbox.filesystem.write_text.aio(f"payload {i}\n", path)
        return await sandbox.filesystem.read_text.aio(path)

    async def main():
        return await asyncio.gather(*(one(i) for i in range(12)))

    assert asyncio.run(main()) == [f"payload {i}\n" for i in range(12)]
    names = sorted(e.name for e in sandbox.filesystem.list_files("/tmp/aio-fs"))
    assert names == sorted(f"{i}.txt" for i in range(12))


def test_stdout_printing_started_through_aio_outlives_the_callers_loop(sandbox, capfd):
    """The printing task lives on the background loop now; the caller's
    asyncio.run() ending used to cancel it, losing the tail."""

    async def start():
        return await sandbox.exec.aio(
            "sh",
            "-c",
            "for i in 1 2 3 4 5; do echo printed-$i; sleep 0.2; done",
            stdout=StreamType.STDOUT,
        )

    process = asyncio.run(start())
    assert process.wait() == 0
    deadline = time.monotonic() + 10
    seen = ""
    while time.monotonic() < deadline:
        seen += capfd.readouterr().out
        if "printed-5" in seen:
            break
        time.sleep(0.1)
    assert [f"printed-{i}" in seen for i in range(1, 6)] == [True] * 5


_FIRST_CALL = textwrap.dedent(
    """
    import asyncio, threading
    import ray
    from ray.experimental.sandbox import modal

    async def main():
        sb = await modal.Sandbox.create.aio(image="busybox:latest", timeout=60)
        try:
            p = await sb.exec.aio("echo", "from a fresh process")
            print((await p.stdout.read.aio()).strip(), flush=True)
        finally:
            await sb.terminate.aio()

    assert not ray.is_initialized()
    asyncio.run(main())
    # Connected on this, the main thread, not the module's loop thread.
    print("connected", ray.is_initialized(), flush=True)
    """
)


def test_a_first_aio_call_connects_to_ray_on_the_callers_thread(tmp_path):
    """The call runs on the loop thread, so it hands the connect back to the
    caller -- here, a process that never called ray.init()."""
    script = tmp_path / "first_call.py"
    script.write_text(_FIRST_CALL)
    env = dict(os.environ, RAY_ADDRESS=ray.get_runtime_context().gcs_address)
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    assert result.stdout.splitlines()[-2:] == [
        "from a fresh process",
        "connected True",
    ], result.stderr[-3000:]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
