"""Unit tests for what survives an exec session being reclaimed.

The actor keeps a bounded number of finished sessions (``_MAX_RETAINED_EXECS``),
and bounds what their output holds in memory (``_FINISHED_OUTPUT_MEMORY``).
These drive the real ``_SandboxActor`` implementation class
with locally spawned processes standing in for ``runsc exec``, so they need
neither runsc nor a Ray cluster -- what is under test is the bookkeeping around
a session, not the container it would have run in.
"""

import asyncio
import os
import random
import sys
import time
import types

import pytest

from ray.experimental.sandbox.modal import _actor as actor_module
from ray.experimental.sandbox.modal._actor import (
    _MAX_RETAINED_EXITS,
    STDERR_FD,
    STDOUT_FD,
    _ExecSession,
    _OutputBuffer,
    _pump,
    _SandboxActor,
    _SpillStore,
)
from ray.experimental.sandbox.modal.exception import InvalidError, NotFoundError

requires_posix_shell = pytest.mark.skipif(
    sys.platform == "win32" or not os.path.exists("/bin/sh"),
    reason="These spawn /bin/sh to stand in for a command in the sandbox.",
)

# The plain class behind the @ray.remote decorator, so it can be driven
# directly without a cluster.
_ActorImpl = _SandboxActor.__ray_metadata__.modified_class


# Production keeps up to _MAX_RETAINED_EXECS finished sessions, a number set
# high because it bounds only bookkeeping. These tests exercise the count
# mechanism itself, at a size that keeps them quick.
_CAP = 16


@pytest.fixture(autouse=True)
def _small_retention_cap(monkeypatch):
    monkeypatch.setattr(actor_module, "_MAX_RETAINED_EXECS", _CAP)


def make_actor() -> "_ActorImpl":
    actor = _ActorImpl.__new__(_ActorImpl)
    _ActorImpl.__init__(actor, {}, None)
    return actor


async def start(actor, exec_id: str, script: str, **buffer_kwargs):
    """Attach a locally spawned process to ``actor`` as an exec session.

    Mirrors what ``exec_start`` does once it has an argv, minus the runsc
    argument building that would need a real sandbox. ``buffer_kwargs`` go to
    each stream's ``_OutputBuffer``.
    """
    process = await asyncio.create_subprocess_exec(
        "/bin/sh",
        "-c",
        script,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    session = _ExecSession(process)
    for fd, stream in ((STDOUT_FD, process.stdout), (STDERR_FD, process.stderr)):
        buffer = _OutputBuffer(**buffer_kwargs)
        session.buffers[fd] = buffer
        session.pump_tasks.append(asyncio.ensure_future(_pump(stream, buffer)))
    actor._execs[exec_id] = session
    session.reaper_task = asyncio.ensure_future(actor._reap(exec_id, session))
    return exec_id


async def drain(actor, exec_id: str, fd: int = STDOUT_FD):
    return [item async for item in actor.stream_output(exec_id, fd)]


async def run_and_retire(actor, count: int):
    """Run ``count`` commands to completion, oldest first.

    One at a time: sessions retire in the order they exit, and commands
    started together can exit in any order under load -- so the oldest was
    occasionally not the first evicted, and a test expecting it gone read its
    live output instead.
    """
    ids = []
    for i in range(count):
        exec_id = await start(actor, f"e{i}", f"echo out{i}; exit {i % 5}")
        await actor.exec_wait(exec_id, None)
        ids.append(exec_id)
    return ids


def run(coro):
    return asyncio.run(coro)


# -- the bound itself ------------------------------------------------------


@requires_posix_shell
def test_only_a_bounded_number_of_sessions_is_retained():
    async def scenario():
        actor = make_actor()
        await run_and_retire(actor, _CAP + 4)
        return len(actor._execs), len(actor._retired_execs)

    live, retired = run(scenario())
    assert live == _CAP
    assert retired == 4, "every evicted session leaves its exit code behind"


# -- what an evicted session still answers ---------------------------------


@requires_posix_shell
def test_wait_still_returns_the_exit_code_of_an_evicted_exec():
    """The pattern that used to break: launch a batch, then collect it.

    Past _CAP the oldest sessions are reclaimed, and asking one
    for its exit code raised KeyError -- reaching the caller as
    RayTaskError(KeyError), which is in no Modal except clause.
    """

    async def scenario():
        actor = make_actor()
        ids = await run_and_retire(actor, _CAP + 3)
        # ids[0] is certainly evicted; its script exits 0 % 5 == 0.
        return await actor.exec_wait(ids[0], None), await actor.exec_poll(ids[0])

    assert run(scenario()) == (0, 0)


@requires_posix_shell
def test_every_evicted_exit_code_is_the_one_its_command_returned():
    async def scenario():
        actor = make_actor()
        ids = await run_and_retire(actor, _CAP + 5)
        evicted = ids[:5]
        return [await actor.exec_poll(exec_id) for exec_id in evicted]

    assert run(scenario()) == [i % 5 for i in range(5)]


@requires_posix_shell
def test_reading_an_evicted_stream_reports_the_whole_stream_as_lost():
    """Not an error, and not a silently empty stream.

    The reader learns the size of the hole the same way a partial buffer
    overrun tells it: one item whose offset is past the reader's cursor.
    """

    async def scenario():
        actor = make_actor()
        ids = await run_and_retire(actor, _CAP + 1)
        return await drain(actor, ids[0])

    # "out0\n" is five bytes, none of which are still held.
    assert run(scenario()) == [(len(b"out0\n"), b"")]


@requires_posix_shell
def test_a_retained_session_still_serves_its_output():
    async def scenario():
        actor = make_actor()
        ids = await run_and_retire(actor, _CAP + 1)
        return await drain(actor, ids[-1])

    offset, chunk = run(scenario())[0]
    assert (offset, chunk) == (0, b"out%d\n" % (_CAP))


@requires_posix_shell
@pytest.mark.parametrize("call", ["write_stdin", "close_stdin"])
def test_writing_to_an_evicted_exec_is_discarded_rather_than_an_error(call):
    """Its process has exited, so its stdin is closed -- which is exactly what
    the live path does for a command that stopped reading."""

    async def scenario():
        actor = make_actor()
        ids = await run_and_retire(actor, _CAP + 1)
        if call == "write_stdin":
            await actor.write_stdin(ids[0], b"ignored")
        else:
            await actor.close_stdin(ids[0])

    run(scenario())


# -- ids that are genuinely unknown ----------------------------------------


@requires_posix_shell
@pytest.mark.parametrize(
    "call",
    [
        lambda actor: actor.exec_poll("never-issued"),
        lambda actor: actor.exec_wait("never-issued", None),
        lambda actor: drain(actor, "never-issued"),
        lambda actor: actor.write_stdin("never-issued", b"x"),
        lambda actor: actor.close_stdin("never-issued"),
    ],
)
def test_an_unknown_exec_id_is_an_invalid_error(call):
    """InvalidError, not KeyError: it has to stay inside the Modal hierarchy,
    since it crosses an actor boundary as RayTaskError(<cause>)."""

    async def scenario():
        with pytest.raises(InvalidError, match="Unknown exec session"):
            await call(make_actor())

    run(scenario())


@requires_posix_shell
def test_releasing_an_exec_drops_its_exit_code_too():
    """Releasing is the caller saying they are done with it.

    Keeping a record would also defeat the bound: the readiness probe releases
    an exec every interval_ms, and each would otherwise leave one behind.
    """

    async def scenario():
        actor = make_actor()
        exec_id = await start(actor, "one", "true")
        await actor.exec_wait(exec_id, None)
        await actor.exec_release(exec_id)
        with pytest.raises(InvalidError, match="Unknown exec session"):
            await actor.exec_poll(exec_id)
        return len(actor._retired_execs)

    assert run(scenario()) == 0


@requires_posix_shell
def test_retained_exit_codes_are_bounded():
    """A sandbox driving commands forever must not accumulate records."""

    async def scenario():
        actor = make_actor()
        for i in range(_MAX_RETAINED_EXITS + 40):
            actor._entomb(f"e{i}", _StubSession())
        return len(actor._retired_execs)

    assert run(scenario()) == _MAX_RETAINED_EXITS


class _StubSession:
    """The two attributes ``_entomb`` reads, with no process behind them."""

    def __init__(self):
        self.buffers = {}
        self.timed_out = False
        self.process = _StubProcess()


class _StubProcess:
    returncode = 0


# -- exec deadlines -----------------------------------------------------------


@requires_posix_shell
def test_the_actor_stops_a_command_at_its_own_deadline():
    """ContainerProcess never kills on a passed deadline -- Modal's client does
    not either -- so the actor's own enforcement is what stops the command:
    it reports -1, and what the command wrote first is still delivered."""

    async def scenario():
        actor = make_actor()
        exec_id = await start(actor, "e1", "echo before; exec sleep 10")
        session = actor._execs[exec_id]
        session.deadline_task = asyncio.ensure_future(
            actor._enforce_exec_deadline(session, 0.5)
        )
        started = time.monotonic()
        code = await actor.exec_wait(exec_id, 20)
        elapsed = time.monotonic() - started
        output = [chunk async for _, chunk in actor.stream_output(exec_id, STDOUT_FD)]
        return code, elapsed, b"".join(output)

    code, elapsed, output = run(scenario())
    assert code == -1
    assert elapsed < 8, "the command should have been stopped at 0.5s"
    assert output == b"before\n"


# -- stopping the command itself, not just its client -------------------------
#
# Killing the `runsc exec` client leaves the command running inside the
# sandbox: SIGKILL cannot be forwarded. Here a local process group stands in
# for the command, its leader's pid in the pid file runsc would write, and a
# plain `kill` for `runsc kill -pgid`.


class _LocalKillBackend:
    def kill_process_group_argv(self, sandbox_id, pid):
        return ["kill", "-KILL", "--", f"-{pid}"]

    def exec_argv(self, sandbox_id, command, **kwargs):
        # exec, so killing the process closes its pipes: asyncio's wait()
        # waits for those too, and a forked sleep would hold them for 30s.
        return ["/bin/sh", "-c", "exec sleep 30"]


@requires_posix_shell
@pytest.mark.parametrize("method", ["exec_start", "exec_collect"])
def test_a_command_spawned_while_the_sandbox_ends_is_stopped_and_refused(
    method, monkeypatch
):
    """Spawning awaits, and a teardown in that window never saw the process:
    it ran on against a container that was gone, and its failure came back as
    the command's own rather than the Sandbox having ended."""
    spawned = []
    real_spawn = asyncio.create_subprocess_exec

    async def scenario():
        actor = make_actor()
        actor._runtime = types.SimpleNamespace(backend=_LocalKillBackend())
        actor._instance_id = "sandbox"

        async def spawn_while_ending(*argv, **kwargs):
            process = await real_spawn(*argv, **kwargs)
            spawned.append(process)
            actor._exit_reason = "terminated"  # teardown began meanwhile
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn_while_ending)
        with pytest.raises(NotFoundError, match="terminated"):
            await getattr(actor, method)(["ignored"])
        return actor

    actor = run(scenario())
    assert actor._execs == {}
    assert spawned[0].returncode is not None, "the spawned command was left running"


async def start_in_sandbox(actor, exec_id: str, script: str, tmp_path) -> int:
    """Like start(), with the command leading its own process group."""
    actor._runtime = types.SimpleNamespace(backend=_LocalKillBackend())
    actor._instance_id = "sandbox"
    process = await asyncio.create_subprocess_exec(
        "/bin/sh",
        "-c",
        script,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    pid_file = tmp_path / f"{exec_id}.pid"
    pid_file.write_text(str(process.pid))
    session = _ExecSession(process, str(pid_file))
    for fd, stream in ((STDOUT_FD, process.stdout), (STDERR_FD, process.stderr)):
        buffer = _OutputBuffer()
        session.buffers[fd] = buffer
        session.pump_tasks.append(asyncio.ensure_future(_pump(stream, buffer)))
    actor._execs[exec_id] = session
    session.reaper_task = asyncio.ensure_future(actor._reap(exec_id, session))
    return process.pid


def group_is_gone(pgid: int, within: float = 5.0) -> bool:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


@requires_posix_shell
def test_releasing_a_running_command_kills_it_and_its_children(tmp_path):
    """exec_release used to kill the client alone; the command ran on."""

    async def scenario():
        actor = make_actor()
        pgid = await start_in_sandbox(actor, "e1", "sleep 30 & sleep 31", tmp_path)
        await asyncio.sleep(0.2)
        await actor.exec_release("e1")
        await asyncio.gather(*actor._stop_tasks)
        return pgid

    pgid = run(scenario())
    assert group_is_gone(pgid), "the command's process group outlived its release"
    assert not (tmp_path / "e1.pid").exists()


@requires_posix_shell
def test_a_deadline_kills_the_command_and_its_children(tmp_path):
    """A timed-out exec reported -1 while it carried on with its side effects."""
    marker = tmp_path / "ran-past-the-deadline"

    async def scenario():
        actor = make_actor()
        # The subshell outlives its parent shell, as a pipeline stage or a
        # backgrounded job does, unless the whole group is killed.
        pgid = await start_in_sandbox(
            actor, "e1", f"(sleep 1; touch {marker}) & sleep 30", tmp_path
        )
        session = actor._execs["e1"]
        session.deadline_task = asyncio.ensure_future(
            actor._enforce_exec_deadline(session, 0.3)
        )
        code = await actor.exec_wait("e1", 20)
        return pgid, code

    pgid, code = run(scenario())
    assert code == -1
    assert group_is_gone(pgid)
    time.sleep(1.5)
    assert not marker.exists(), "the command kept running past its deadline"


async def _spawn_unwatched(script: str, **kwargs):
    return await asyncio.create_subprocess_exec(
        "/bin/sh",
        "-c",
        script,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        **kwargs,
    )


@requires_posix_shell
def test_stopping_a_command_waits_for_runsc_to_record_its_pid(tmp_path):
    """runsc writes the pid file only once the command has started. A command
    stopped before then -- an upload cancelled at once -- had only its client
    killed, and ran on in the sandbox."""
    pid_file = tmp_path / "e1.pid"

    async def scenario():
        actor = make_actor()
        actor._runtime = types.SimpleNamespace(backend=_LocalKillBackend())
        actor._instance_id = "sandbox"
        process = await _spawn_unwatched("sleep 30 & sleep 31", start_new_session=True)

        async def record_pid_late():
            await asyncio.sleep(0.2)
            pid_file.write_text(str(process.pid))

        recorder = asyncio.ensure_future(record_pid_late())
        await actor._stop_command(_ExecSession(process, str(pid_file)))
        await recorder
        await process.wait()
        return process.pid

    pgid = run(scenario())
    assert group_is_gone(pgid), "only the client was killed; the command ran on"


@requires_posix_shell
def test_stopping_a_command_whose_client_exits_does_not_wait_for_its_pid(tmp_path):
    """A client that has exited will never record a pid: waiting out the
    grace period for one would only delay the release."""

    async def scenario():
        actor = make_actor()
        actor._runtime = types.SimpleNamespace(backend=_LocalKillBackend())
        actor._instance_id = "sandbox"
        process = await _spawn_unwatched("sleep 0.1")
        started = time.monotonic()
        await actor._stop_command(_ExecSession(process, str(tmp_path / "none.pid")))
        elapsed = time.monotonic() - started
        await process.wait()
        return elapsed

    assert run(scenario()) < actor_module._PID_FILE_WAIT_SECONDS / 2


# -- the main process ----------------------------------------------------------


@requires_posix_shell
def test_the_main_process_exiting_ends_the_sandbox():
    """Measured on Modal: once the entrypoint exits, exec and filesystem calls
    raise NotFoundError, while its output and exit code stay readable."""

    async def scenario():
        actor = make_actor()
        main = await start(actor, "main", "echo main-out; exit 3")
        actor._main_exec_id = main
        await actor.exec_wait(main, 30)
        state = await actor.get_state()
        refused = []
        for call in (
            lambda: actor.exec_start(["true"]),
            lambda: actor.exec_collect(["true"]),
        ):
            try:
                await call()
            except NotFoundError as exc:
                refused.append(exc)
        output = [chunk async for _, chunk in actor.stream_output(main, STDOUT_FD)]
        return state, len(refused), b"".join(output), await actor.exec_wait(main, None)

    state, refused, output, code = run(scenario())
    assert state["exit_reason"] == "completed"
    assert state["returncode"] == 3
    assert refused == 2
    assert output == b"main-out\n"
    assert code == 3


def test_waiting_on_a_sandbox_with_no_main_process_wakes_on_its_end():
    """Woken by the event, not a 200ms poll, and told the reason with the code."""

    async def scenario():
        actor = make_actor()
        timed_out = await actor.wait_sandbox(0.05)
        waiter = asyncio.ensure_future(actor.wait_sandbox(None))
        await asyncio.sleep(0.05)
        assert not waiter.done()
        actor._exit_reason = "terminated"
        actor._exit_event.set()
        return timed_out, await asyncio.wait_for(waiter, 1)

    timed_out, ended = run(scenario())
    assert timed_out == {"returncode": None, "exit_reason": None}
    assert ended == {"returncode": 137, "exit_reason": "terminated"}


@requires_posix_shell
def test_an_exec_exiting_does_not_end_the_sandbox():
    async def scenario():
        actor = make_actor()
        exec_id = await start(actor, "e1", "exit 0")
        await actor.exec_wait(exec_id, 30)
        return (await actor.get_state())["exit_reason"]

    assert run(scenario()) is None


# -- spilled output ----------------------------------------------------------
#
# Measured on Modal: a command that wrote 4 GiB with nobody reading ran to
# completion, and all of it came back. These hold the actor to the same:
# output past memory goes to disk rather than holding the command back or
# being dropped.

_MIB = 1024 * 1024


def attach_spill(actor, tmp_path, limit=1 << 30) -> _SpillStore:
    actor._spill = _SpillStore(
        str(tmp_path / "spill"),
        limit,
        segment_bytes=_MIB,
        min_free=0,
        relieve=actor._relieve_spill_pressure,
    )
    return actor._spill


async def stream_length(actor, exec_id: str) -> int:
    """Read one stream to the end, failing on any gap."""
    total = 0
    async for offset, chunk in actor.stream_output(exec_id, STDOUT_FD):
        assert offset == total, f"gap at {total}: resumed at {offset}"
        total += len(chunk)
    return total


@requires_posix_shell
def test_an_unread_chatty_command_runs_to_completion_and_loses_nothing(tmp_path):
    async def scenario():
        actor = make_actor()
        spill = attach_spill(actor, tmp_path)
        exec_id = await start(
            actor, "e1", "head -c 67108864 /dev/zero", limit=_MIB, spill=spill
        )
        # Nothing is reading, yet the command must be able to finish.
        await asyncio.wait_for(actor._execs[exec_id].process.wait(), 60)
        spilled = os.listdir(spill.directory)
        total = await stream_length(actor, exec_id)
        await actor.exec_release(exec_id)
        return spilled, total, os.listdir(spill.directory), spill.used

    spilled, total, after, used = run(scenario())
    assert spilled, "output past memory should have gone to disk"
    assert total == 64 * _MIB
    assert after == [] and used == 0, "releasing the exec frees its files"


@requires_posix_shell
def test_spill_pressure_evicts_a_finished_session_before_live_output(tmp_path):
    """A finished command's output goes before a running one loses a byte."""

    async def scenario():
        actor = make_actor()
        # Room for one command's spill, not for two.
        spill = attach_spill(actor, tmp_path, limit=12 * _MIB)
        old = await start(
            actor, "old", "head -c 8388608 /dev/zero; exit 3", limit=_MIB, spill=spill
        )
        await actor.exec_wait(old, 60)
        new = await start(
            actor, "new", "head -c 8388608 /dev/zero", limit=_MIB, spill=spill
        )
        await actor.exec_wait(new, 60)
        total = await stream_length(actor, new)
        return old in actor._execs, total, await actor.exec_wait(old, None)

    old_still_held, total, old_code = run(scenario())
    assert not old_still_held
    assert total == 8 * _MIB
    # Evicted for space, but its exit code still answers, as for any eviction.
    assert old_code == 3


@requires_posix_shell
def test_terminate_deletes_spilled_output(tmp_path):
    """The caller kills the actor next, so terminate() is the last chance."""

    async def scenario():
        actor = make_actor()
        spill = attach_spill(actor, tmp_path)
        exec_id = await start(
            actor, "e1", "head -c 8388608 /dev/zero", limit=_MIB, spill=spill
        )
        await actor.exec_wait(exec_id, 60)
        assert os.listdir(spill.directory)
        await actor.terminate()
        return os.path.exists(spill.directory)

    assert run(scenario()) is False


# -- finished output: bounded by memory, not by count ------------------------

_PRODUCTION_CAP = actor_module._MAX_RETAINED_EXECS


@requires_posix_shell
def test_a_batch_that_finishes_before_it_is_read_is_all_still_there(monkeypatch):
    """Start many, wait for all, then read each: the old cap of sixteen lost
    the first finishers' output, where Modal keeps every exec's."""
    monkeypatch.setattr(actor_module, "_MAX_RETAINED_EXECS", _PRODUCTION_CAP)

    async def scenario():
        actor = make_actor()
        ids = [await start(actor, f"e{i}", f"echo out{i}") for i in range(40)]
        for exec_id in ids:
            await actor.exec_wait(exec_id, None)
        return [await _stdout_of(actor, exec_id) for exec_id in ids]

    assert run(scenario()) == [b"out%d\n" % i for i in range(40)]


async def _settle_flushes(actor):
    """Wait for any move of finished output to disk to land."""
    for _ in range(500):
        busy = [
            buffer
            for session in actor._execs.values()
            for buffer in session.buffers.values()
            if buffer is not None and buffer._flush_task is not None
        ]
        if not busy:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("flushes did not settle")


@requires_posix_shell
def test_finished_output_past_the_memory_budget_moves_to_disk_whole(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(actor_module, "_FINISHED_OUTPUT_MEMORY", 256 * 1024)

    async def scenario():
        actor = make_actor()
        spill = attach_spill(actor, tmp_path)
        ids = []
        for i in range(6):
            # 128 KiB each: all of it fits the stream's own window, so only
            # the budget for finished output sends any of it to disk.
            ids.append(
                await start(actor, f"e{i}", "head -c 131072 /dev/zero", spill=spill)
            )
            await actor.exec_wait(ids[-1], None)
        await _settle_flushes(actor)
        in_memory = sum(
            actor_module._session_memory(actor._execs[exec_id]) for exec_id in ids
        )
        lengths = [await stream_length(actor, exec_id) for exec_id in ids]
        return in_memory, spill.used, lengths

    in_memory, spilled, lengths = run(scenario())
    assert in_memory <= 256 * 1024
    assert spilled >= 6 * 131072 - 256 * 1024
    assert lengths == [131072] * 6, "moved to disk, and still read back whole"


@requires_posix_shell
def test_without_room_to_spill_the_oldest_finished_sessions_go_whole(monkeypatch):
    """No spill store, as on a node too full to spill: the budget is kept by
    dropping whole sessions, oldest first, and their exit codes survive."""
    monkeypatch.setattr(actor_module, "_FINISHED_OUTPUT_MEMORY", 256 * 1024)

    async def scenario():
        actor = make_actor()
        ids = []
        for i in range(6):
            ids.append(
                await start(actor, f"e{i}", f"head -c 131072 /dev/zero; exit {i}")
            )
            await actor.exec_wait(ids[-1], None)
        kept = [exec_id for exec_id in ids if exec_id in actor._execs]
        codes = [await actor.exec_wait(exec_id, None) for exec_id in ids]
        return kept, codes

    kept, codes = run(scenario())
    assert kept == ["e4", "e5"], "the newest fit the budget; the oldest went"
    assert codes == list(range(6))


class _ShellBackend:
    """Runs exec_start's command locally, as it is given."""

    def exec_argv(self, sandbox_id, command, **kwargs):
        return list(command)


@requires_posix_shell
def test_a_finished_download_still_being_read_is_not_evicted_for_memory(monkeypatch):
    """A filesystem download's command exits with up to a buffer of output
    still unread. Paced output can never move to disk, so the memory trim
    evicted the whole session, and the transfer failed with its tail lost."""
    monkeypatch.setattr(actor_module, "_FINISHED_OUTPUT_MEMORY", 1024 * 1024)
    size = 2_000_000

    async def scenario():
        actor = make_actor()
        actor._runtime = types.SimpleNamespace(backend=_ShellBackend())
        actor._instance_id = "sandbox"
        exec_id = await actor.exec_start(
            ["/bin/sh", "-c", f"head -c {size} /dev/zero"],
            paced=True,
            client_released=True,
        )
        await actor.exec_wait(exec_id, None)
        return exec_id in actor._execs, await drain(actor, exec_id)

    kept, items = run(scenario())
    assert kept
    assert items[0][0] == 0, "the reader was handed a gap"
    assert sum(len(chunk) for _, chunk in items) == size


@requires_posix_shell
@pytest.mark.parametrize(
    "script,ended,expected",
    [
        ("exit 3", False, 3),
        ("exit 3", True, NotFoundError),
        ("exit 0", True, 0),
    ],
)
def test_require_live_reports_a_failure_the_sandbox_ending_caused(
    script, ended, expected
):
    """A transfer the sandbox ended under came back as a filesystem error with
    exit code 137, where exec_collect reports NotFoundError, as Modal does. A
    command that succeeded is still a success, whatever happened since."""

    async def scenario():
        actor = make_actor()
        exec_id = await start(actor, "e1", script)
        await actor.exec_wait(exec_id, None)
        if ended:
            actor._exit_reason = "terminated"
        return await actor.exec_wait(exec_id, None, require_live=True)

    if expected is NotFoundError:
        with pytest.raises(NotFoundError, match="terminated"):
            run(scenario())
    else:
        assert run(scenario()) == expected


# -- stdin ordering ----------------------------------------------------------


def _pieces(data: bytes, size: int):
    return [
        (offset, data[offset : offset + size]) for offset in range(0, len(data), size)
    ]


async def _stdout_of(actor, exec_id: str) -> bytes:
    await actor.exec_wait(exec_id, None)
    return b"".join(chunk for _, chunk in await drain(actor, exec_id))


@requires_posix_shell
@pytest.mark.parametrize("order", ["reversed", "shuffled"])
def test_stdin_writes_land_in_offset_order_whatever_order_they_run_in(order):
    """Ray runs an async actor's calls in no guaranteed order, which is how
    pipelined uploads used to land with their chunks scrambled. Here every
    write, and the close, runs out of order at once."""
    data = os.urandom(256 * 1024)
    pieces = _pieces(data, 8192)
    if order == "reversed":
        pieces.reverse()
    else:
        random.Random(7).shuffle(pieces)

    async def scenario():
        actor = make_actor()
        exec_id = await start(actor, "cat", "cat")
        await asyncio.gather(
            actor.close_stdin(exec_id, len(data)),
            *(actor.write_stdin(exec_id, chunk, offset) for offset, chunk in pieces),
        )
        return await _stdout_of(actor, exec_id)

    assert run(scenario()) == data


@requires_posix_shell
def test_repeated_and_overlapping_stdin_writes_are_written_once():
    """A retried write is harmless: whatever was already written is skipped."""
    data = b"0123456789abcdefghij"

    async def scenario():
        actor = make_actor()
        exec_id = await start(actor, "cat", "cat")
        await actor.write_stdin(exec_id, data[0:10], 0)
        await actor.write_stdin(exec_id, data[0:10], 0)
        await actor.write_stdin(exec_id, data[5:15], 5)
        await actor.write_stdin(exec_id, data[15:20], 15)
        await actor.close_stdin(exec_id, 20)
        return await _stdout_of(actor, exec_id)

    assert run(scenario()) == data


@requires_posix_shell
@pytest.mark.parametrize("ending", ["released", "exited"])
def test_a_write_waiting_on_a_missing_chunk_is_let_go(ending):
    """A predecessor that never arrives must not hang its successors: they are
    released when the session is dropped or the command exits."""

    async def scenario():
        actor = make_actor()
        script = "cat" if ending == "released" else "sleep 0.2"
        exec_id = await start(actor, "e", script)
        waiting = asyncio.ensure_future(actor.write_stdin(exec_id, b"later", 100))
        await asyncio.sleep(0.05)
        assert not waiting.done()
        if ending == "released":
            await actor.exec_release(exec_id)
        await asyncio.wait_for(waiting, 5)

    run(scenario())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
