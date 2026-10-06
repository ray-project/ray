"""The Ray actor that owns a gVisor sandbox and its running exec sessions.

One actor holds exactly one sandbox. It is an *async* actor: every method is a
coroutine, so Ray drives them all on a single event loop with no thread pool
to size and no blocking call that can stall a concurrent one. A long-lived
``stream_output`` therefore never blocks a ``write_stdin`` on the same process,
even while it is parked waiting for the client to consume.

The actor is deliberately a dumb byte pump. Text decoding, line buffering and
:class:`~ray.experimental.sandbox.modal.stream_type.StreamType` policy all live
client-side; nothing here knows about them.
"""

import asyncio
import collections
import itertools
import logging
import os
import shutil
import tempfile
import time
import uuid
from typing import Any, Deque, Dict, List, Optional, Set, Tuple, Union

import ray
from ray.experimental.sandbox.config import parse_memory_bytes
from ray.experimental.sandbox.modal.exception import InvalidError, NotFoundError
from ray.experimental.sandbox.modal.probe import Probe
from ray.experimental.sandbox.runtime import SandboxRuntime

logger = logging.getLogger(__name__)

# File descriptor numbers, matching the values a caller sees on FileInfo-style
# stream objects.
STDOUT_FD = 1
STDERR_FD = 2

# How many unread bytes one exec'd command's stdout or stderr may hold. Reading
# consumes, as Modal's client does from 1.6: each byte is delivered once, and
# what a reader has is freed, so a reader that keeps up holds next to nothing.
# Past this the oldest unread output is dropped, and the reader is told so.
# Modal's server keeps a whole unread exec output (measured at 4 GiB); here it
# lives in this actor's memory, so output nobody reads while it is produced
# keeps its newest 64 MiB.
_EXEC_OUTPUT_CAP = 64 * 1024 * 1024

# The same bound for the Sandbox's own stdout and stderr. Measured on Modal's
# V2 backend: a main process that wrote 512 MiB, 1 GiB or 2 GiB unread got back
# exactly its newest 256 MiB each time, the rest reported dropped -- a fixed
# window, not a fraction.
_MAIN_OUTPUT_CAP = 256 * 1024 * 1024

# Unread output across every stream of one sandbox. The caps above bound one
# stream; this bounds many at once, since all of it is in this actor's memory,
# which Ray's memory monitor counts against the actor. Past it, finished
# commands' oldest output goes first, then the oldest of the running stream
# holding the most.
_SANDBOX_OUTPUT_CAP = 512 * 1024 * 1024

# How far a paced stream -- one of the filesystem's transfers -- may run ahead
# of its reader before its command is held back. Above what the streaming
# window below can have in flight (8 chunks of up to 1 MiB): the reader's
# position only reaches the actor through that window, so a smaller cap could
# leave the pump waiting for an acknowledgement only more output would bring.
_PACED_OUTPUT_CAP = 16 * 1024 * 1024

# How many chunks a stream_output generator may run ahead of its consumer; the
# client passes it as `_generator_backpressure_num_objects` (see io_streams,
# which imports it from here so the two cannot disagree). Ray holds the
# generator until its consumer has taken all but this many of the chunks it
# yielded -- which is what lets stream_output free consumed output without the
# client acknowledging each chunk. It also bounds what a chatty process pushes
# into the caller's memory store: about 8 MiB in flight per stream.
_STREAM_BACKPRESSURE_OBJECTS = 8

# How many finished exec sessions stay readable. Only a bound on bookkeeping --
# a finished session with little output costs a few kilobytes -- and set high
# because Modal keeps an exec's output until it is read: a caller that starts a
# batch of commands, waits for all of them and then reads each, must find every
# output still there. A session read to the end is released at once (see
# _SandboxActor._retire_if_consumed); what unread ones hold is bounded by
# _SANDBOX_OUTPUT_CAP.
_MAX_RETAINED_EXECS = 1024

# How many *exit codes* outlive the sessions they belong to. An entry is two
# ints and a dict, so this costs kilobytes where the sessions it replaces cost
# megabytes -- which is the point: a client still holding a ContainerProcess for
# an evicted exec has to keep getting its exit code out of poll() and wait().
_MAX_RETAINED_EXITS = 1024

_READ_SIZE = 65536

# Small pipe reads are gathered into chunks of about this size. A process
# writing a line at a time hands the pump one line per read, and kept as its
# own chunk each made memory ~100k tiny objects for 8 MiB of unread output:
# 2-3x the payload in object overhead, and a scan to find a reader's cursor
# that cost ~7 ms of the actor's loop per read, measured.
_CHUNK_GATHER_BYTES = 64 * 1024

# Cap on a single yield from `stream_output`. Above Ray's inline limit
# (`max_direct_call_object_size`, 100 KiB) a value travels through the object
# store rather than in the reply -- and that still wins: every yield costs about
# a millisecond whatever it carries, so a stream moved ~93 MiB/s in 96 KiB
# yields and ~465 MiB/s in 1 MiB ones, measured. Output that arrives a little at
# a time is yielded as it comes, well under this, and stays inline.
_MAX_YIELD_BYTES = 1024 * 1024

# Coalescing a burst into one yield only works while chunks are queued, and a
# consumer that keeps up leaves at most one queued -- so a line-buffered
# process would pay an object and an RPC round trip per `printf`. These bound
# an adaptive hold-back that closes that gap. There is no fixed window: the
# hold-back opens only for a producer already outrunning _COALESCE_IDLE_GAP,
# and shuts the moment the stream goes quiet for that long, so a slow or
# interactive stream never waits for output that was not going to batch anyway.
_COALESCE_IDLE_GAP = 0.002

# Ceiling on the total hold-back for one yield, so an endlessly hot stream
# still makes forward progress at a bounded latency.
_COALESCE_MAX_WINDOW = 0.05

# Enough queued to be worth an object on its own: at or above this, yield
# immediately and add no latency to a high-throughput stream.
_COALESCE_TARGET_BYTES = 16 * 1024

# Modal runs a readiness probe for at most five minutes (documented); after
# that wait_until_ready() raises TimeoutError whatever timeout it was given.
# Each attempt is bounded too: measured, an attempt that would have hung for
# ten minutes was abandoned and readiness came at ~11s.
_PROBE_WINDOW_SECONDS = 300
_PROBE_ATTEMPT_TIMEOUT_SECONDS = 10

# How long stopping a command waits for runsc to record its pid, and how often
# it looks. runsc writes the pid file only once the command has started, so a
# command stopped in its first moments has none yet.
_PID_FILE_WAIT_SECONDS = 2
_PID_FILE_POLL_SECONDS = 0.02

# Exit codes Modal reports for sandbox-level outcomes.
_EXIT_CODE_TIMEOUT = 124

# What a single exec reports when it exceeded its own timeout. Modal's
# convention, and distinct from the sandbox-level 124 above.
_EXIT_CODE_EXEC_TIMEOUT = -1
_EXIT_CODE_TERMINATED = 137


def _unlink_quietly(path: Optional[str]) -> None:
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        pass


def _read_pid(path: Optional[str]) -> Optional[int]:
    """The pid runsc recorded in ``path``, or None if there is none yet."""
    if not path:
        return None
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


class _MemoryBudget:
    """The unread output held across one sandbox's streams, against its cap.

    Buffers add and subtract what they hold as they go, so the total is never
    recomputed by walking every stream. Past the cap, ``relieve`` is called
    with the excess and drops what it chooses.
    """

    def __init__(self, cap: int, relieve=None):
        self.cap = cap
        self.used = 0
        self._relieve = relieve

    def check(self) -> None:
        if self.used > self.cap and self._relieve is not None:
            self._relieve(self.used - self.cap)


class _OutputBuffer:
    """The unread output of one stream of a running process, in memory.

    A background task drains the OS pipe into this buffer, so the process never
    waits on a reader -- Modal's model: measured on Modal, a command left
    gigabytes of output unread and still ran to completion.

    Reading consumes, as Modal's client does from 1.6, where a StreamReader
    keeps one cursor and so delivers each byte once: :meth:`ack` frees what
    the reader has. What the buffer holds is therefore output nobody has read
    yet, bounded by ``cap``. Past the cap the oldest unread bytes are dropped,
    and never silently: offsets are absolute, so a reader whose cursor falls
    behind ``_base`` is handed bytes that begin later than it asked for, and
    sees the jump rather than a seamlessly spliced stream.

    A paced stream never drops. Its pump waits instead, until its one reader is
    within ``cap`` -- for the filesystem's transfers, which must arrive whole.
    """

    def __init__(
        self,
        cap: int = _EXEC_OUTPUT_CAP,
        paced: bool = False,
        budget: Optional[_MemoryBudget] = None,
        on_drop=None,
    ):
        # (start_offset, data) pairs, oldest first, for every byte still held.
        # Offsets rather than a running sum, so locating a cursor is
        # comparisons. Small reads are gathered into a growing bytearray at the
        # tail (see _append).
        self._chunks: Deque[Tuple[int, Union[bytes, bytearray]]] = collections.deque()
        self._cap = cap
        # Memory is dropped a whole chunk at a time, so a chunk must stay small
        # next to the cap or the cap stops holding: a sixteenth of it at most.
        self._gather_bytes = max(1, min(_CHUNK_GATHER_BYTES, cap // 16))
        # The oldest byte a reader can still get, past whatever was
        # acknowledged or dropped. It may fall inside the first chunk.
        self._base = 0
        self._written = 0
        # How far the reader has acknowledged, and what was dropped unread.
        self._acked = 0
        self._lost = 0
        self._eof = False
        self._closed = False
        self._event = asyncio.Event()
        self._paced = paced
        self._reader_moved = asyncio.Event()
        self._budget = budget
        # Told why, whenever unread output is dropped.
        self._on_drop = on_drop

    @property
    def written(self) -> int:
        """Total bytes ever fed into this buffer."""
        return self._written

    @property
    def lost(self) -> int:
        """Bytes dropped before the reader got them."""
        return self._lost

    @property
    def memory_bytes(self) -> int:
        """Bytes this stream currently holds."""
        return self._written - self._chunks[0][0] if self._chunks else 0

    @property
    def paced(self) -> bool:
        return self._paced

    @property
    def consumed(self) -> bool:
        """Whether the stream ended and its reader has acknowledged all of it."""
        return self._eof and self._acked >= self._written

    def feed(self, data: bytes) -> None:
        if not data or self._closed:
            return
        self._append(data)
        self._written += len(data)
        if self._budget is not None:
            self._budget.used += len(data)
        # Paced streams are bounded by pace() instead: the pump waits. Any
        # other keeps exactly its newest ``cap`` unread bytes, as Modal's V2
        # keeps exactly the newest 256 MiB of a Sandbox's own output.
        if not self._paced and self._written - self._base > self._cap:
            self._lose_until(
                self._written - self._cap,
                f"a stream's unread output passed its {self._cap // 2**20} MiB cap",
            )
        self._event.set()
        if self._budget is not None and not self._paced:
            self._budget.check()

    def _append(self, data: bytes) -> None:
        """Hold ``data`` in memory, gathering small reads into one chunk.

        A read smaller than the gather size extends a bytearray at the tail
        until it reaches that size; larger reads are kept as they came.
        Memory is then a few hundred chunks rather than one per line.
        """
        if len(data) >= self._gather_bytes:
            self._chunks.append((self._written, data))
            return
        if self._chunks:
            tail = self._chunks[-1][1]
            if type(tail) is bytearray and len(tail) < self._gather_bytes:
                tail += data
                return
        self._chunks.append((self._written, bytearray(data)))

    def feed_eof(self) -> None:
        self._eof = True
        self._event.set()

    async def pace(self) -> None:
        """Hold a paced stream's pump until its reader is within the cap.

        Every other stream returns at once: its process is never held back,
        and output past the cap is dropped instead, as feed() does.
        """
        if not self._paced:
            return
        while not self._closed and self._written - self._acked > self._cap:
            self._reader_moved.clear()
            await self._reader_moved.wait()

    def ack(self, offset: int) -> None:
        """Free everything before ``offset``, which the reader now has.

        Args:
            offset: The reader's position. Never moves backwards.
        """
        offset = min(offset, self._written)
        if offset <= self._acked:
            return
        self._acked = offset
        self._reader_moved.set()
        if offset > self._base:
            self._base = offset
            self._release()

    def drop_oldest(self, size: int, reason: str) -> int:
        """Drop at least ``size`` bytes of memory from the oldest unread output.

        Whole chunks only, so what is dropped is actually freed, and never the
        newest, which may still be growing. A paced stream gives up nothing.

        Args:
            size: How many bytes of memory to free.
            reason: Why, for the once-per-sandbox warning.

        Returns:
            The bytes of memory freed.
        """
        if self._paced or self._closed or size <= 0:
            return 0
        target, covered = self._base, 0
        newest = len(self._chunks) - 1
        for start, chunk in itertools.islice(self._chunks, max(newest, 0)):
            target = start + len(chunk)
            covered += len(chunk)
            if covered >= size:
                break
        return self._lose_until(target, reason)

    def _lose_until(self, offset: int, reason: str) -> int:
        """Drop the unread output before ``offset``, and say why.

        Args:
            offset: The first byte to keep.
            reason: Why, for the once-per-sandbox warning.

        Returns:
            The bytes of memory freed: chunks wholly behind ``offset``.
        """
        if offset <= self._base:
            return 0
        self._lost += offset - self._base
        self._base = offset
        if self._on_drop is not None:
            self._on_drop(reason)
        return self._release()

    def _release(self) -> int:
        """Free the chunks that now lie wholly behind the base.

        Returns:
            The bytes of memory freed.
        """
        freed = 0
        while (
            self._chunks and self._chunks[0][0] + len(self._chunks[0][1]) <= self._base
        ):
            _, chunk = self._chunks.popleft()
            freed += len(chunk)
        if freed and self._budget is not None:
            self._budget.used -= freed
        return freed

    def close(self) -> None:
        """Release everything this stream holds. Its reader sees the rest as lost."""
        if self._closed:
            return
        self._closed = True
        if self._budget is not None:
            self._budget.used -= self.memory_bytes
        self._chunks.clear()
        self._eof = True
        self._event.set()
        # A paced pump waiting for a reader that will never come.
        self._reader_moved.set()

    async def read_from(
        self, cursor: int, max_bytes: int = _MAX_YIELD_BYTES
    ) -> Optional[Tuple[int, bytes]]:
        """Return ``(start_offset, data)`` for bytes at or after ``cursor``.

        Returns None at end of stream. ``start_offset`` is where the returned
        bytes actually begin: it exceeds ``cursor`` exactly when dropping
        overtook this reader, which is how a caller detects a gap without a
        separate marker. The next cursor is ``start_offset + len(data)``.

        Coalescing means a burst of pipe reads costs one yield rather than one
        per read, which is what lets a whole stream be served by a single task.
        Reading does not free anything by itself; see :meth:`ack`.
        """
        loop = asyncio.get_running_loop()
        started = loop.time()
        while not self._closed and cursor >= self._written:
            if self._eof:
                return None
            self._event.clear()
            await self._event.wait()
        if not self._closed:
            await self._coalesce(cursor, waited=loop.time() - started)
        if self._closed:
            # Dropped along with its session. Report where the stream had
            # reached, so a reader part-way through sees the rest as lost
            # rather than as a clean end.
            return None if cursor >= self._written else (self._written, b"")
        return self._collect(cursor, max_bytes)

    async def _coalesce(self, cursor: int, waited: float) -> None:
        """Hold a small yield back to gather the rest of the burst behind it.

        How long the first chunk took to arrive is the signal for whether that
        is worth doing. Arriving within a gap means the producer is writing
        faster than the gap, so more is on its way and waiting buys a bigger
        payload for one object; taking longer means the producer is the slow
        side, and waiting would only add latency to output that was going to
        be a lone chunk regardless. That keeps interactive streams at their
        current latency and leaves bulk streams -- already past the target --
        untouched.
        """
        if self._eof or self._written - cursor >= _COALESCE_TARGET_BYTES:
            return
        if waited > _COALESCE_IDLE_GAP:
            return

        loop = asyncio.get_running_loop()
        deadline = loop.time() + _COALESCE_MAX_WINDOW
        while self._written - cursor < _COALESCE_TARGET_BYTES and not self._eof:
            gap = min(_COALESCE_IDLE_GAP, deadline - loop.time())
            if gap <= 0:
                return
            # No await between the size check and the clear, so a feed racing
            # this cannot be missed: it would set the event we are about to
            # wait on and wake us immediately.
            self._event.clear()
            try:
                await asyncio.wait_for(self._event.wait(), gap)
            except asyncio.TimeoutError:
                # The burst is over. Yield what we have rather than sit on it.
                return

    def _collect(self, cursor: int, max_bytes: int) -> Tuple[int, bytes]:
        # A cursor behind the base resumes at the oldest byte still held. The
        # caller sees the jump in the returned offset.
        start = max(cursor, self._base)
        pieces: List[bytes] = []
        total = 0
        for chunk_start, chunk in self._chunks:
            if chunk_start + len(chunk) <= start:
                continue
            piece = chunk if chunk_start >= start else chunk[start - chunk_start :]
            room = max_bytes - total
            if len(piece) >= room:
                # Truncate rather than overshoot: max_bytes sizes each object,
                # and with it how much the client's backpressure window holds
                # in flight. The rest is still here for the next read.
                pieces.append(piece[:room] if len(piece) > room else piece)
                total += min(len(piece), room)
                break
            pieces.append(piece)
            total += len(piece)
        # A whole chunk that arrived as one large read can be handed straight
        # back. Anything else -- several pieces, or part of a gathered
        # bytearray that is still growing -- is copied out into bytes.
        if len(pieces) == 1 and type(pieces[0]) is bytes:
            return start, pieces[0]
        return start, b"".join(pieces)


class _RetiredExec:
    """What is kept about an exec session after its buffers are reclaimed.

    A finished session is released once its output has been read, and its
    unread output is bounded, so only ``_MAX_RETAINED_EXECS`` of them survive. But a client holding a ``ContainerProcess`` still has to be able to
    ask for its exit code however many other commands have run since, which is
    what Modal does and what dropping the session outright used to break.

    ``written`` is how many bytes each stream ever produced, so a reader that
    arrives after the eviction is told the size of the hole instead of being
    handed a silently empty stream.
    """

    __slots__ = ("returncode", "written")

    def __init__(self, returncode: Optional[int], written: Dict[int, int]):
        self.returncode = returncode
        self.written = written


class _ExecSession:
    """A single command running inside the sandbox."""

    def __init__(
        self,
        process: "asyncio.subprocess.Process",
        pid_file: Optional[str] = None,
        client_released: bool = False,
    ):
        self.process = process
        # Where runsc records the command's pid inside the sandbox: `process`
        # is only the runsc client, and killing it leaves the command running.
        self.pid_file = pid_file
        # The client always calls exec_release for this session itself, as the
        # filesystem's transfers do. Such a session is never evicted for the
        # memory it holds: a download's command exits with up to a buffer of
        # output still unread, and evicting it then lost that tail.
        self.client_released = client_released
        self.buffers: Dict[int, Optional[_OutputBuffer]] = {}
        self.pump_tasks: List[asyncio.Task] = []
        self.stdin_closed = False
        # How many bytes have been handed to the command's stdin, and a signal
        # each time that grows or stdin closes. Ray runs an async actor's calls
        # in no guaranteed order, so each write names the offset it belongs at
        # and waits here for everything before it.
        self.stdin_offset = 0
        self.stdin_progress = asyncio.Event()
        # Set when this command was killed for exceeding its own timeout, so
        # its exit is reported as Modal's -1 rather than the 137 the SIGKILL
        # would otherwise produce.
        self.timed_out = False
        self.deadline_task: Optional[asyncio.Task] = None
        # Retires the session the moment its process exits, so reclamation does
        # not depend on anyone calling exec_wait.
        self.reaper_task: Optional[asyncio.Task] = None

    def close_stdin_for_good(self) -> None:
        """Mark stdin closed and wake every write still waiting for its turn.

        For when the command can no longer take input -- it exited, or its
        session is being dropped. A write waiting on an offset whose
        predecessor will now never arrive would otherwise wait forever.
        """
        self.stdin_closed = True
        self.stdin_progress.set()

    async def await_stdin_turn(self, offset: int) -> bool:
        """Wait until every byte before ``offset`` has been written.

        Args:
            offset: Where in the stdin stream the caller's bytes begin.

        Returns:
            True once it is the caller's turn, False if stdin closed first.
        """
        while self.stdin_offset < offset and not self.stdin_closed:
            # No await between the check and the clear, so a write landing in
            # between cannot be missed: it sets the event this then waits on.
            self.stdin_progress.clear()
            await self.stdin_progress.wait()
        return not self.stdin_closed


@ray.remote
class _SandboxActor:
    """Owns one gVisor sandbox and supervises the processes running in it."""

    def __init__(
        self,
        create_kwargs: Dict[str, Any],
        timeout: Optional[float],
        force_pull: bool = False,
        readiness_probe: Optional["Probe"] = None,
    ):
        self._create_kwargs = create_kwargs
        self._timeout = timeout
        self._force_pull = force_pull
        self._probe = readiness_probe
        self._runtime: Optional[SandboxRuntime] = None
        self._instance_id: Optional[str] = None
        self._execs: Dict[str, _ExecSession] = {}
        # Finished exec ids, oldest first. Bounds how much output the actor
        # holds on behalf of processes nobody is reading any more.
        self._finished_execs: Deque[str] = collections.deque()
        # Exit codes of sessions the line above evicted, oldest first. Outlives
        # the sessions themselves so poll() and wait() keep answering; see
        # _RetiredExec.
        self._retired_execs: "collections.OrderedDict[str, _RetiredExec]" = (
            collections.OrderedDict()
        )
        self._main_exec_id: Optional[str] = None
        # Unread output across every stream, against _SANDBOX_OUTPUT_CAP.
        self._output_budget = _MemoryBudget(
            _SANDBOX_OUTPUT_CAP, relieve=self._relieve_output_memory
        )
        self._output_drop_warned = False
        # Where runsc writes the pids of exec'd commands; see _pid_file.
        self._pid_dir: Optional[str] = None
        self._exit_reason: Optional[str] = None
        # Set alongside _exit_reason, so a waiter can wake on the sandbox
        # ending without polling for it.
        self._exit_event = asyncio.Event()
        self._deadline_task: Optional[asyncio.Task] = None
        # Kills in flight for commands whose session was dropped while they
        # ran. Held so the tasks, and the sessions they close over, live until
        # the command inside the sandbox is gone.
        self._stop_tasks: Set[asyncio.Task] = set()
        # Readiness-probe state. The task polls the sandbox from inside the
        # actor rather than from the client, so an interval_ms of 100 costs one
        # local `runsc exec` per attempt and no Ray RPC at all. The event is
        # what `wait_ready` blocks on, and is set exactly once -- on success,
        # or on teardown, so a waiter is never stranded.
        self._probe_task: Optional[asyncio.Task] = None
        self._probe_ready = asyncio.Event()
        self._probe_outcome: Optional[str] = None
        # Distinct from _deleted: this guards re-entry while a teardown runs,
        # whereas _deleted records that the container is actually gone. The
        # event lets a second caller wait for the first one's delete instead of
        # racing past it, and the owner is tracked so a re-entry from inside
        # the running teardown does not wait on itself.
        self._tearing_down = False
        self._teardown_done = asyncio.Event()
        self._teardown_owner: Optional[asyncio.Task] = None
        self._deleted = False

    # -- lifecycle ---------------------------------------------------------

    async def start(self, main_command: Optional[List[str]] = None) -> Dict[str, Any]:
        """Create the sandbox and start the clock.

        The main process is *not* launched here. A caller that has files to push
        into the sandbox first calls :meth:`launch_main` afterwards, so that
        ``add_local_*(copy=False)`` content is in place before anything runs.

        Args:
            main_command: Convenience for callers with no files to push; launches
                the main process immediately.

        Returns:
            ``{"instance_id": ..., "image_config": ...}``. The image config is
            returned here rather than left to a second :meth:`get_image_config`
            call because every caller needs both and it is only readable once
            this method has run: the image is pulled onto this node as part of
            creating the sandbox. Bundling them halves the round trips it takes
            to bring a sandbox up.
        """
        self._runtime = SandboxRuntime()
        # Ray's reservation is deliberately *not* copied onto the container
        # as a cgroup cap. On Modal a cpu/memory value is a request the
        # container may burst above; only an explicit (request, limit)
        # tuple caps it, and Sandbox.create passes that limit in
        # create_kwargs when there is one.

        # Dropping the cached image forces the create below to pull it again.
        # It is the only lever a caller has when a mutable tag has moved.
        if self._force_pull:
            await asyncio.to_thread(
                self._runtime.image_manager.invalidate_image,
                self._create_kwargs["image"],
            )

        # Creating the sandbox blocks until runsc reports it running, and the
        # pull it may trigger is slower still. Neither belongs on the loop.
        self._instance_id = await asyncio.to_thread(
            self._runtime.create, **self._create_kwargs
        )

        # Past this point the container exists, so anything that raises has to
        # reclaim it here. The caller cannot: it has no instance id yet, and
        # letting the actor handle drop only makes Ray kill the worker, which
        # strands the runsc container and its /tmp/ray/sandbox/<id> tree.
        try:
            if self._timeout is not None and self._timeout > 0:
                self._deadline_task = asyncio.ensure_future(self._enforce_deadline())

            if main_command:
                await self.launch_main(main_command)

            return {
                "instance_id": self._instance_id,
                "image_config": await self.get_image_config(),
            }
        except BaseException:
            try:
                await self._teardown()
            except Exception:
                logger.warning(
                    "Failed to clean up sandbox %s after a failed start",
                    self._instance_id,
                    exc_info=True,
                )
            raise

    async def get_image_config(self) -> Dict[str, Any]:
        """The base image's own config (Cmd, Entrypoint, Env, WorkingDir).

        Read here rather than on the client: the image cache lives on whichever
        node runs the sandbox, and until it is pulled the image does not even
        exist until :meth:`start` has run.
        """
        if self._runtime is None or not self._create_kwargs.get("image"):
            return {}
        # The config the backend kept when it created the sandbox, not the
        # cached image's as it is now: a force_build since then would have
        # swapped in an image whose Cmd could differ.
        config = self._runtime.backend.image_config(self._instance_id)
        if config is None:
            image_manager = self._runtime.image_manager
            config = await asyncio.to_thread(
                image_manager.get_image_config, self._create_kwargs["image"]
            )
        return config.get("config", {}) or {}

    async def launch_main(self, main_command: List[str]) -> Optional[str]:
        """Start the sandbox's main process and bind its streams.

        Its stdout and stderr hold up to ``_MAIN_OUTPUT_CAP`` unread bytes, the
        newest, as a Modal Sandbox's own streams keep their newest 256 MiB.

        Args:
            main_command: The command to run as the main process.

        Returns:
            The exec id of the main process, or None if no command was given.
        """
        if not main_command:
            return None
        self._main_exec_id = await self.exec_start(main_command, cap=_MAIN_OUTPUT_CAP)
        # Probe from the moment the main process is running: a readiness check
        # normally waits on something that process produces, so starting any
        # earlier only burns execs on a condition that cannot hold yet. Both
        # entry paths converge here -- start(main_command=...) calls this
        # method, and _Sandbox.create calls it after pushing startup files --
        # so no separate RPC is needed to get the probe going.
        self._ensure_probe_task()
        return self._main_exec_id

    def _ensure_probe_task(self) -> None:
        """Start the readiness-probe loop, at most once."""
        if self._probe is None or self._probe_task is not None:
            return
        if self._probe_ready.is_set():
            return
        self._probe_task = asyncio.ensure_future(self._run_probe())

    async def _run_probe(self) -> None:
        """Run the readiness check until it passes or the sandbox goes away.

        Each attempt is an ordinary short-lived exec in the same container as
        the main process, which this loop never inspects or waits on: a probe
        checks a long-running command from the outside, so the two are separate
        exec sessions throughout.
        """
        argv = list(self._probe.exec_argv)
        interval = self._probe.interval_ms / 1000
        loop = asyncio.get_running_loop()
        window_ends = loop.time() + _PROBE_WINDOW_SECONDS
        try:
            while True:
                if self._exit_reason is not None or self._deleted:
                    self._finish_probe("ended")
                    return
                remaining = window_ends - loop.time()
                if remaining <= 0:
                    # Stop, as Modal does: wait_until_ready() reports it as a
                    # timeout from now on, and a probe that was never going to
                    # pass stops costing an exec every interval.
                    self._finish_probe("timeout")
                    return
                exec_id = await self.exec_start(
                    argv,
                    stdout_devnull=True,
                    stderr_devnull=True,
                    # A hung attempt no longer blocks readiness for good; the
                    # exec deadline kills it and the next attempt runs.
                    timeout=min(_PROBE_ATTEMPT_TIMEOUT_SECONDS, remaining),
                )
                try:
                    returncode = await self.exec_wait(exec_id)
                finally:
                    # exec_wait only *retires* the session, which leaves it
                    # among the finished sessions the actor keeps, up to
                    # _MAX_RETAINED_EXECS of them. At a 100ms interval this loop
                    # would reach that bound within two minutes and start
                    # evicting the output of the caller's own execs. Nothing
                    # ever reads a probe's output, so release it.
                    await self.exec_release(exec_id)
                if returncode == 0:
                    self._finish_probe("ready")
                    return
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            # Teardown cancels this task and sets the outcome itself; re-raise
            # so the task ends as cancelled rather than looking like a result.
            raise
        except Exception:
            # A probe that cannot run at all -- a command missing from the
            # image, say -- must not leave wait_ready() blocked until its
            # timeout with nothing logged.
            logger.warning("Readiness probe failed; giving up", exc_info=True)
            self._finish_probe("ended")

    def _finish_probe(self, outcome: str) -> None:
        """Record the probe's outcome and release everyone waiting on it."""
        if self._probe_outcome is None:
            self._probe_outcome = outcome
        self._probe_ready.set()

    async def wait_ready(self, timeout: Optional[float]) -> Dict[str, Any]:
        """Block until the readiness probe passes.

        Args:
            timeout: Seconds to wait, or None to wait indefinitely.

        Returns:
            ``{"outcome": ..., "exit_reason": ...}`` where outcome is "ready",
            "ended" (the sandbox stopped first) or "timeout". The exit reason
            travels with it so a client handling "ended" can raise the right
            exception without a follow-up :meth:`get_state` call.
        """
        # Covers a sandbox with no main process, where launch_main never ran:
        # the probe is the only thing such a caller can be waiting on, and
        # starting it here costs nothing when the task already exists.
        self._ensure_probe_task()
        try:
            await asyncio.wait_for(self._probe_ready.wait(), timeout)
        except asyncio.TimeoutError:
            return {"outcome": "timeout", "exit_reason": self._exit_reason}
        return {
            "outcome": self._probe_outcome or "ended",
            "exit_reason": self._exit_reason,
        }

    async def _enforce_deadline(self) -> None:
        try:
            await asyncio.sleep(self._timeout)
        except asyncio.CancelledError:
            return
        if self._exit_reason is None:
            self._exit_reason = "timeout"
            self._exit_event.set()
        # Drop the handle before tearing down. _teardown cancels the deadline
        # task, and on this path that task is *this* one -- cancelling it here
        # would raise CancelledError at teardown's first await, before the
        # container is deleted, leaving it running and every waiter stranded.
        self._deadline_task = None
        await self._teardown()

    async def get_state(self) -> Dict[str, Any]:
        """Return the fields the client handle needs to compute its status."""
        main_returncode = None
        if self._main_exec_id is not None:
            session = self._execs.get(self._main_exec_id)
            if session is not None:
                main_returncode = _session_returncode(session)
        return {
            "instance_id": self._instance_id,
            "exit_reason": self._exit_reason,
            "has_main_process": self._main_exec_id is not None,
            "main_returncode": main_returncode,
            "returncode": self._sandbox_returncode(),
            "deleted": self._deleted,
        }

    async def main_exec_id(self) -> Optional[str]:
        return self._main_exec_id

    async def wait_sandbox(self, timeout: Optional[float]) -> Dict[str, Any]:
        """Block until the sandbox finishes.

        Args:
            timeout: Seconds to wait, or None to wait until it ends.

        Returns:
            ``{"returncode": ..., "exit_reason": ...}``, the returncode None if
            ``timeout`` ran out first. Both in one reply: a caller that had to
            ask for the reason separately could lose that second call to a
            concurrent terminate(), which kills this actor as soon as it is
            done -- and report the sandbox gone rather than terminated.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while self._exit_reason is None:
            session = (
                self._execs.get(self._main_exec_id)
                if self._main_exec_id is not None
                else None
            )
            if session is not None and session.process.returncode is not None:
                break
            remaining = _remaining(deadline)
            if remaining is not None and remaining <= 0:
                return {"returncode": None, "exit_reason": None}
            if session is not None:
                # Wake on either the process exiting or another task ending
                # the sandbox -- the deadline and terminate both set an exit
                # reason without necessarily reaping the process, so waiting
                # on the process alone would block past the sandbox's end.
                #
                # Raced rather than polled: asyncio appends a waiter to the
                # subprocess transport on every `process.wait()` and only
                # clears them when the process exits, so re-polling grew
                # that list without bound for the life of the sandbox.
                if not await self._wait_for_exit(session, remaining):
                    return {"returncode": None, "exit_reason": None}
            else:
                # No main process: only the deadline or terminate() ends it,
                # and both set the event.
                try:
                    await asyncio.wait_for(self._exit_event.wait(), remaining)
                except asyncio.TimeoutError:
                    return {"returncode": None, "exit_reason": None}
        return self._outcome()

    def _outcome(self) -> Dict[str, Any]:
        return {
            "returncode": self._sandbox_returncode(),
            "exit_reason": self._exit_reason,
        }

    def _require_live(self) -> None:
        """Refuse work once the container is gone.

        The main process exiting or a timeout tears the container down but
        leaves the actor running -- its output and exit codes stay readable
        until terminate() kills it -- so without this an exec issued afterwards
        runs against a deleted container and fails somewhere deep in runsc.

        NotFoundError whatever ended it: measured on Modal, both an exec and a
        filesystem call on a finished Sandbox raise NotFoundError, on either
        backend and whether or not the Sandbox had been used before.
        """
        if not self._ending():
            return
        why = {
            "completed": "its main process has exited",
            "timeout": "it exceeded its timeout",
            "terminated": "it was terminated",
        }.get(self._exit_reason, "it was torn down")
        raise NotFoundError(
            f"The Sandbox is unavailable: {why}. This Sandbox may have "
            f"already shut down."
        )

    def _ending(self) -> bool:
        """Whether the container is gone or going: ended, torn down, or in teardown."""
        return self._exit_reason is not None or self._deleted or self._tearing_down

    async def _refuse_if_ended_during_spawn(
        self, process: "asyncio.subprocess.Process", pid_file: Optional[str]
    ) -> None:
        """Stop a command whose sandbox ended while it was being spawned.

        Spawning awaits, and a teardown that runs in that window never sees the
        new process: it is not registered yet. It would then run against a
        container that is gone or going -- failing in runsc, or not at all -- and
        be reported as the command's own failure rather than the Sandbox
        having ended. Nothing awaits between this check and the caller
        registering the session, so a teardown cannot slip in after it.

        Args:
            process: The ``runsc exec`` client just spawned.
            pid_file: Where runsc records the command's pid in the sandbox.

        Raises:
            NotFoundError: The sandbox ended; nothing was left running.
        """
        if not self._ending():
            return
        await self._stop_command(_ExecSession(process, pid_file))
        # Bounded: asyncio's wait() also waits for the pipes to close, and a
        # descendant the kill did not reach could hold them open for as long
        # as it runs. Refusing the call must not wait on that.
        try:
            await asyncio.wait_for(process.wait(), 5)
        except asyncio.TimeoutError:
            pass
        _unlink_quietly(pid_file)
        self._require_live()

    async def _wait_for_exit(
        self, session: "_ExecSession", remaining: Optional[float]
    ) -> bool:
        """Block until the main process exits or the sandbox ends.

        Returns True if either happened, False if ``remaining`` ran out first.
        """
        waiters = [
            asyncio.ensure_future(self._exit_event.wait()),
            asyncio.ensure_future(session.process.wait()),
        ]
        try:
            done, _ = await asyncio.wait(
                waiters, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for waiter in waiters:
                waiter.cancel()
        return bool(done)

    def _sandbox_returncode(self) -> Optional[int]:
        if self._exit_reason == "timeout":
            return _EXIT_CODE_TIMEOUT
        if self._exit_reason == "terminated":
            return _EXIT_CODE_TERMINATED
        if self._main_exec_id is not None:
            session = self._execs.get(self._main_exec_id)
            if session is not None:
                return _session_returncode(session)
        return None

    async def terminate(self) -> Dict[str, Any]:
        """Terminate the sandbox.

        Returns:
            ``{"returncode": ..., "exit_reason": ...}``, together because the
            caller kills this actor next and cannot ask again.
        """
        # A sandbox whose main process already exited keeps that exit code;
        # only one still running is reported as terminated.
        if self._exit_reason is None and self._sandbox_returncode() is None:
            self._exit_reason = "terminated"
            self._exit_event.set()
        await self._teardown()
        # Nothing reads the output afterwards -- Modal answers NotFound for a
        # terminated sandbox's output too -- so free it now rather than when
        # the caller gets round to killing this actor.
        for session in self._execs.values():
            for buffer in session.buffers.values():
                if buffer is not None:
                    buffer.close()
        return self._outcome()

    async def _teardown(self) -> None:
        # Two distinct questions, and conflating them loses the container: is a
        # teardown already running (do not re-enter), and did the delete
        # actually succeed (may __del__ and a later terminate() stop trying)?
        # Setting `_deleted` up front answered both with "yes" the moment
        # teardown *started*, so a delete that failed disabled every retry.
        if self._deleted:
            return
        if self._tearing_down:
            # Wait for the in-flight teardown rather than returning. Returning
            # early let terminate() report a finished sandbox while another
            # task's `runsc delete` was still running -- and the client then
            # ray.kill()s this actor, killing the delete with it and orphaning
            # the container and its root filesystem.
            if self._teardown_owner is not asyncio.current_task():
                await self._teardown_done.wait()
            return
        self._tearing_down = True
        self._teardown_owner = asyncio.current_task()
        self._teardown_done.clear()
        try:
            # Never cancel the task we are running on: the deadline path calls
            # teardown from inside the deadline task itself, and cancelling it
            # would abort this coroutine at its next await -- before the
            # container is deleted.
            if self._deadline_task is not None:
                if self._deadline_task is not asyncio.current_task():
                    self._deadline_task.cancel()
                self._deadline_task = None

            # Same guard as above: _run_probe awaits exec_wait, so a teardown
            # reached from inside the probe task itself must not cancel it.
            # Recording the outcome before cancelling matters more -- a waiter
            # parked in wait_ready() would otherwise block until its own
            # timeout for a sandbox that is already gone.
            self._finish_probe("ended")
            if self._probe_task is not None:
                if self._probe_task is not asyncio.current_task():
                    self._probe_task.cancel()
                self._probe_task = None

            for session in list(self._execs.values()):
                if session.deadline_task is not None:
                    session.deadline_task.cancel()
                    session.deadline_task = None
                for task in session.pump_tasks:
                    task.cancel()
                session.close_stdin_for_good()
                if session.process.returncode is None:
                    try:
                        session.process.kill()
                    except ProcessLookupError:
                        pass

            if self._runtime is None or self._instance_id is None:
                # Nothing was ever created, so there is nothing to reclaim.
                self._deleted = True
                return

            try:
                await asyncio.to_thread(self._runtime.delete, self._instance_id)
                self._deleted = True
                self._remove_pid_dir()
            except Exception:
                # Leave `_deleted` unset so __del__ and any later terminate()
                # still try. Logged rather than swallowed: a container that
                # outlives its actor holds a whole root filesystem and its
                # runsc processes, and nothing else would report it.
                logger.warning(
                    "Failed to delete sandbox %s; teardown will retry",
                    self._instance_id,
                    exc_info=True,
                )
        finally:
            self._tearing_down = False
            self._teardown_owner = None
            self._teardown_done.set()

    def __del__(self):
        """Last-resort cleanup of the gVisor container.

        Ray reclaiming an actor kills its process without running _teardown, so
        a handle dropped without terminate() would strand a runsc container and
        its /tmp/ray/sandbox/<id> tree. The non-Modal Sandbox actor defends the
        same way. Synchronous and best-effort: there is no event loop to await
        on during interpreter teardown.
        """
        self._remove_pid_dir()
        if self._deleted or self._runtime is None or self._instance_id is None:
            return
        self._deleted = True
        try:
            self._runtime.delete(self._instance_id)
        except Exception:
            pass

    def _remove_pid_dir(self) -> None:
        if self._pid_dir is not None:
            shutil.rmtree(self._pid_dir, ignore_errors=True)
            self._pid_dir = None

    # -- exec sessions -----------------------------------------------------

    async def exec_start(
        self,
        command: Union[str, List[str]],
        *,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        stdout_devnull: bool = False,
        stderr_devnull: bool = False,
        timeout: Optional[float] = None,
        cap: int = _EXEC_OUTPUT_CAP,
        paced: bool = False,
        client_released: bool = False,
    ) -> str:
        """Launch a command and return its exec id.

        ``cap`` bounds each stream's unread output; past it the oldest unread
        bytes are dropped, and the command is never held back.

        ``paced`` makes stdout wait for its one reader instead: the command
        blocks once it is ``_PACED_OUTPUT_CAP`` ahead, and nothing is dropped.
        That is wrong for a user's command, which Modal never holds back, and
        right for the filesystem's own transfers, which must arrive whole and
        have exactly one reader.

        ``client_released`` marks a session its caller always releases itself;
        see :attr:`_ExecSession.client_released`.

        A ``timeout`` is enforced here rather than left to whoever calls
        ``exec_wait``: Modal kills the command when its deadline passes, so
        ``poll()`` reports it and the output streams reach EOF even if nobody
        is waiting. Bounding only the wait would leave the command running --
        and a caller reading its stdout blocked until the sandbox itself died.
        """
        self._require_live()
        exec_id = uuid.uuid4().hex
        pid_file = self._pid_file(exec_id)
        argv = self._runtime.backend.exec_argv(
            self._instance_id, command, cwd=cwd, env=env, shell=shell, pid_file=pid_file
        )
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL
            if stdout_devnull
            else asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
            if stderr_devnull
            else asyncio.subprocess.PIPE,
        )
        await self._refuse_if_ended_during_spawn(process, pid_file)

        session = _ExecSession(process, pid_file, client_released=client_released)
        for fd, stream, devnull in (
            (STDOUT_FD, process.stdout, stdout_devnull),
            (STDERR_FD, process.stderr, stderr_devnull),
        ):
            if devnull or stream is None:
                session.buffers[fd] = None
                continue
            if paced and fd == STDOUT_FD:
                buffer = _OutputBuffer(_PACED_OUTPUT_CAP, paced=True)
            else:
                buffer = _OutputBuffer(
                    cap,
                    budget=self._output_budget,
                    on_drop=self._warn_output_dropped,
                )
            session.buffers[fd] = buffer
            session.pump_tasks.append(asyncio.ensure_future(_pump(stream, buffer)))
        self._execs[exec_id] = session
        if timeout is not None and timeout > 0:
            session.deadline_task = asyncio.ensure_future(
                self._enforce_exec_deadline(session, timeout)
            )
        session.reaper_task = asyncio.ensure_future(self._reap(exec_id, session))
        return exec_id

    async def _reap(self, exec_id: str, session: "_ExecSession") -> None:
        """Retire a session as soon as its process exits.

        Retiring used to happen only at the tail of ``exec_wait``, so a process
        the caller merely polled -- or dropped without waiting at all -- kept
        its session, both output buffers and its pipe transports for the life
        of the sandbox. ``_MAX_RETAINED_EXECS`` did not bound that, because it
        bounds the retirement queue, which only ``exec_wait`` ever wrote to.
        """
        await session.process.wait()
        # Nothing reads stdin any more: release writes waiting their turn.
        session.close_stdin_for_good()
        _unlink_quietly(session.pid_file)
        # Let the pumps observe EOF so buffered output is not lost.
        # asyncio.wait rather than awaiting each pump: a pump cancelled by the
        # exec's deadline or by teardown has already fed EOF, and awaiting it
        # re-raised its CancelledError here -- ending this reaper as if *it*
        # had been cancelled, so the session was never retired and exec_wait
        # raised CancelledError instead of reporting the timeout as -1. A
        # cancellation of the reaper itself still propagates out of the wait.
        # Pumps still running after the grace period are left to finish.
        pending = [task for task in session.pump_tasks if not task.done()]
        if pending:
            await asyncio.wait(pending, timeout=5)
        self._retire(exec_id)
        if self._execs.get(exec_id) is session:
            self._retire_if_consumed(exec_id, session)
        if exec_id == self._main_exec_id and self._exit_reason is None:
            # The main process exiting *is* the Sandbox's end, as on Modal:
            # the container goes, taking any other running command with it,
            # and later execs and filesystem calls are refused. The actor --
            # and with it every stream's output and every exit code -- stays
            # until terminate(), which is what hands its reservation back.
            self._exit_reason = "completed"
            self._exit_event.set()
            await self._teardown()

    async def _enforce_exec_deadline(
        self, session: "_ExecSession", timeout: float
    ) -> None:
        """Kill one command once its own deadline passes.

        Killing the local ``runsc exec`` client is not enough to end its
        output: the process inside the container still holds the write end of
        the pipe, so a reader would block until the sandbox itself was torn
        down. Cancelling the pumps closes the streams -- each feeds EOF from
        its ``finally`` -- so whatever the command emitted before the deadline
        is delivered and the read then ends, which is what Modal does.
        """
        try:
            await asyncio.sleep(timeout)
        except asyncio.CancelledError:
            return
        if session.process.returncode is not None:
            return
        # A process exiting between the check above and this assignment is
        # reported as timed out despite having finished on its own. The window
        # is a single scheduling gap and the cost is a wrong exit code on a
        # command that was about to be killed anyway, so it is left unlocked.
        session.timed_out = True
        await self._stop_command(session)
        for task in session.pump_tasks:
            task.cancel()

    async def _stop_command(self, session: "_ExecSession") -> None:
        """Kill a command inside the sandbox, then its ``runsc exec`` client.

        Killing only the client, which is all that used to happen, left the
        command running: SIGKILL cannot be forwarded into the sandbox. A
        timed-out exec carried on with its side effects, and an upload
        abandoned part-way kept its ``cat`` waiting on stdin, ready to move a
        truncated file into place whenever that pipe closed. The client goes
        second, and ``session`` holds its pipes open until then, so the command
        never sees an EOF it did not get from the caller.
        """
        pid = await self._await_pid(session)
        if pid is not None and not self._deleted and session.process.returncode is None:
            try:
                argv = self._runtime.backend.kill_process_group_argv(
                    self._instance_id, pid
                )
                killer = await asyncio.create_subprocess_exec(
                    *argv,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                try:
                    await asyncio.wait_for(killer.wait(), 10)
                except asyncio.TimeoutError:
                    killer.kill()
                    await killer.wait()
            except Exception:
                # The command may already be gone, or the sandbox with it.
                # Killing the client below is still worth doing.
                logger.debug("Could not kill pid %s in the sandbox", pid, exc_info=True)
        if session.process.returncode is None:
            try:
                session.process.kill()
            except ProcessLookupError:
                pass
        _unlink_quietly(session.pid_file)

    async def _await_pid(self, session: "_ExecSession") -> Optional[int]:
        """The command's pid in the sandbox, waiting briefly for runsc to record it.

        A command stopped before runsc wrote its pid file -- an upload
        cancelled at once, a very short exec timeout -- used to have only its
        client killed. The command kept running, holding a stdin pipe whose
        eventual close would let an upload's `mv` commit a partial file.

        Waits only while the wait can pay off: a client that has exited will
        never write the file, and a sandbox that is ending takes the command
        with it.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _PID_FILE_WAIT_SECONDS
        while True:
            pid = _read_pid(session.pid_file)
            if (
                pid is not None
                or not session.pid_file
                or session.process.returncode is not None
                or self._ending()
                or loop.time() >= deadline
            ):
                return pid
            await asyncio.sleep(_PID_FILE_POLL_SECONDS)

    def _pid_file(self, exec_id: str) -> str:
        """Where runsc records one command's pid, in this actor's own directory.

        Private (mkdtemp's 0700) and never inside the sandbox's bundle: runsc
        writes the file from the host, so a directory the sandbox could write
        to would let it plant a symlink there. Under the node's Ray temp
        directory, made on first use and removed with the sandbox; an actor
        that is SIGKILLed leaves behind only the pid files of the commands it
        was running.
        """
        if self._pid_dir is None:
            root = (
                ray.get_runtime_context().get_temp_dir()
                if ray.is_initialized()
                else None
            )
            if root:
                os.makedirs(root, exist_ok=True)
            self._pid_dir = tempfile.mkdtemp(prefix="sandbox-pids-", dir=root)
        return os.path.join(self._pid_dir, f"{exec_id}.pid")

    async def stream_output(self, exec_id: str, fd: int, start_offset: int = 0):
        """Yield ``(offset, data)`` from one stream until end of stream.

        A Ray streaming generator, so one long-lived task serves the whole
        stream: reading a process's output costs a single RPC rather than one
        per chunk. The caller passes ``_STREAM_BACKPRESSURE_OBJECTS`` as
        ``_generator_backpressure_num_objects``.

        ``start_offset`` is where the caller's reader stands, and so also says
        that it has everything before it: that is freed. It can only be
        supplied here, at creation -- a streaming generator is pull-only, so a
        live one cannot be steered -- and resuming means a fresh call, as
        Modal's reader reopens its stream at its own offset.

        Output the reader has taken is freed as the stream goes. Ray holds this
        generator until its consumer has taken all but
        ``_STREAM_BACKPRESSURE_OBJECTS`` of the chunks yielded so far, so once
        the generator runs on after its n-th yield, chunks up to n - W are
        surely the reader's. The last W stay, so a reader that stops part-way
        -- a ``break``, an ``aclose()``, Ctrl-C -- and reopens at its own offset
        loses nothing that was still in flight.

        Each item carries the absolute offset its bytes begin at, so a reader
        overtaken by dropping sees the jump instead of a seamless splice.
        """
        session = self._execs.get(exec_id)
        if session is None:
            # The whole session was released. If the reader had not reached
            # its end, the rest is gone rather than empty: one zero-length item
            # at the byte count the stream reached says so in the same language
            # a dropped chunk does -- the reader compares the offset against
            # its own cursor and reports the gap on `truncated`/`bytes_lost`.
            retired = self._require_retired(exec_id)
            written = retired.written.get(fd, 0)
            if start_offset < written:
                yield written, b""
            return
        buffer = session.buffers.get(fd)
        if buffer is None:
            # The stream was configured DEVNULL: there is no output to serve.
            return
        buffer.ack(start_offset)
        cursor = start_offset
        # End offsets of the chunks yielded and possibly still in flight.
        in_flight: Deque[int] = collections.deque()
        while True:
            result = await buffer.read_from(cursor)
            if result is None:
                return
            offset, data = result
            cursor = offset + len(data)
            yield offset, data
            in_flight.append(cursor)
            if len(in_flight) > _STREAM_BACKPRESSURE_OBJECTS:
                buffer.ack(in_flight.popleft())

    async def stream_ack(self, exec_id: str, fd: int, offset: int) -> None:
        """Note that a stream's reader has everything before ``offset``.

        The client sends this when its reader reaches end of stream, without
        waiting for a reply: what stream_output kept back for chunks in flight
        is then freed, and a finished command whose every stream has been read
        is released at once rather than when the retention bounds get to it.
        Its exit code stays answerable, as for any released session.

        Args:
            exec_id: The exec whose stream was read.
            fd: Which stream.
            offset: How far the reader got.
        """
        session = self._execs.get(exec_id)
        if session is None:
            return
        buffer = session.buffers.get(fd)
        if buffer is None:
            return
        buffer.ack(offset)
        self._retire_if_consumed(exec_id, session)

    async def write_stdin(
        self, exec_id: str, data: bytes, offset: Optional[int] = None
    ) -> None:
        """Write ``data`` to a command's stdin, at byte ``offset`` of the stream.

        Ray runs an async actor's calls in whatever order they become ready,
        not the order they were sent, so a caller with several writes in
        flight -- an upload keeps a window of them -- cannot rely on arrival
        order: pipelined uploads used to land with their chunks scrambled.
        Each write therefore names where it belongs and waits for everything
        before it. Bytes already written, in whole or in part, are skipped,
        so a retried write is harmless -- as Modal's stdin offsets make it.

        Args:
            exec_id: The command's exec id.
            data: The bytes to write.
            offset: Where ``data`` begins in the stdin stream. None appends,
                for a caller that never has more than one write in flight.
        """
        session = self._execs.get(exec_id)
        if session is None:
            # A retired session's process has exited, so its stdin is closed;
            # discarding the write is what the live path below does for a
            # command that stopped reading.
            self._require_retired(exec_id)
            return
        if offset is None:
            offset = session.stdin_offset
        if not await session.await_stdin_turn(offset):
            return
        if session.process.stdin is None:
            return
        already_written = session.stdin_offset - offset
        if already_written >= len(data):
            return
        chunk = data[already_written:] if already_written else data
        # Synchronous, so the next write in order can follow it into the pipe
        # while this one waits on drain below: write() calls keep their order.
        session.process.stdin.write(chunk)
        session.stdin_offset += len(chunk)
        session.stdin_progress.set()
        try:
            await session.process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            # The command exited without consuming its input.
            session.close_stdin_for_good()

    async def close_stdin(self, exec_id: str, offset: Optional[int] = None) -> None:
        """Close a command's stdin once ``offset`` bytes have been written.

        Args:
            exec_id: The command's exec id.
            offset: The stream's total length, so the close cannot overtake a
                write still waiting its turn. None closes at once.
        """
        session = self._execs.get(exec_id)
        if session is None:
            self._require_retired(exec_id)
            return
        if offset is not None and not await session.await_stdin_turn(offset):
            return
        if session.stdin_closed or session.process.stdin is None:
            return
        session.close_stdin_for_good()
        try:
            session.process.stdin.close()
        except (BrokenPipeError, ConnectionResetError):
            pass

    async def exec_poll(self, exec_id: str) -> Optional[int]:
        session = self._execs.get(exec_id)
        if session is None:
            return self._retired_returncode(exec_id)
        return _session_returncode(session)

    async def exec_wait(
        self,
        exec_id: str,
        timeout: Optional[float] = None,
        *,
        require_live: bool = False,
    ) -> Optional[int]:
        """Wait for a process to exit. Returns None if ``timeout`` elapsed.

        The waiting and the retiring both belong to the session's own reaper,
        so this only watches it. Shielded, so a caller's timeout leaves the
        reaper running -- otherwise a timed-out wait would cancel the very task
        that reclaims the session.

        ``CancelledError`` is deliberately not caught: swallowing it would turn
        an external cancellation into a normal return, and cancellation is
        delivered once, so the caller could never stop this.

        Args:
            exec_id: The command's exec id.
            timeout: Seconds to wait, or None to wait until it exits.
            require_live: Raise NotFoundError, rather than return the exit
                code, when the command failed and the sandbox has ended: it
                failed because the sandbox went. For the filesystem's
                transfers, which report that the way ``exec_collect`` does.

        Returns:
            The exit code, or None if ``timeout`` ran out first.
        """
        session = self._execs.get(exec_id)
        if session is None:
            # Evicted while the caller held its handle. It has certainly
            # exited -- only a finished session is ever retired -- so its
            # recorded exit code is the whole answer, and waiting is a no-op.
            returncode = self._retired_returncode(exec_id)
        else:
            try:
                await asyncio.wait_for(asyncio.shield(session.reaper_task), timeout)
            except asyncio.TimeoutError:
                return None
            returncode = _session_returncode(session)
        if require_live and returncode != 0:
            self._require_live()
        return returncode

    def _retire(self, exec_id: str) -> None:
        """Note that an exec finished, and bound how many finished ones remain.

        Sessions cannot be released the moment they exit: their unread output
        stays readable afterwards, which is how Modal behaves and how most
        callers use exec. One whose output has all been read is released by
        _retire_if_consumed; the rest are bounded here by count, which is only
        bookkeeping and set high -- a batch of commands that all finish before
        any is read must all still be readable, as on Modal, where an old cap
        of sixteen lost the first finishers. The output they hold is bounded by
        _SANDBOX_OUTPUT_CAP.
        """
        if exec_id == self._main_exec_id:
            return
        if exec_id in self._finished_execs:
            self._finished_execs.remove(exec_id)
        self._finished_execs.append(exec_id)
        while len(self._finished_execs) > _MAX_RETAINED_EXECS:
            evicted = self._finished_execs.popleft()
            session = self._execs.pop(evicted, None)
            if session is not None:
                self._evict(evicted, session)

    def _evict(self, exec_id: str, session: "_ExecSession") -> None:
        """Drop a finished session that is already out of the tables.

        Its exit code is kept first, before _discard cancels the tasks it is
        read from: what the buffers held is gone either way, but a client
        still holding this exec's handle needs what the process exited with.
        """
        self._entomb(exec_id, session)
        self._discard(session)

    def _retire_if_consumed(self, exec_id: str, session: "_ExecSession") -> None:
        """Release a finished session whose every stream has been read to the end.

        Reading consumes, so nothing it holds can be asked for again; keeping it
        would only cost memory until the retention bounds came round to it. Its
        exit code is kept, as for any released session. The main process is
        never released (the sandbox's own exit code is read from it), nor is a
        session its client releases itself.
        """
        if (
            exec_id == self._main_exec_id
            or session.client_released
            or exec_id not in self._finished_execs
        ):
            return
        if not all(
            buffer is None or buffer.consumed for buffer in session.buffers.values()
        ):
            return
        self._finished_execs.remove(exec_id)
        del self._execs[exec_id]
        self._evict(exec_id, session)

    def _entomb(self, exec_id: str, session: "_ExecSession") -> None:
        """Keep an evicted session's exit code and stream sizes."""
        self._retired_execs[exec_id] = _RetiredExec(
            _session_returncode(session),
            {
                fd: buffer.written
                for fd, buffer in session.buffers.items()
                if buffer is not None
            },
        )
        self._retired_execs.move_to_end(exec_id)
        while len(self._retired_execs) > _MAX_RETAINED_EXITS:
            self._retired_execs.popitem(last=False)

    def _retired_returncode(self, exec_id: str) -> Optional[int]:
        return self._require_retired(exec_id).returncode

    def _require_retired(self, exec_id: str) -> _RetiredExec:
        """The record for an evicted session, or an error naming what is wrong.

        An id that was never issued, or one the caller already released, is a
        programming error rather than a lost buffer -- and reporting it as
        InvalidError keeps it inside the Modal exception hierarchy, where a bare
        KeyError travelling back as RayTaskError(KeyError) was not.
        """
        retired = self._retired_execs.get(exec_id)
        if retired is None:
            raise InvalidError(f"Unknown exec session: {exec_id}")
        return retired

    async def exec_kill(self, exec_id: str) -> None:
        session = self._execs.get(exec_id)
        if session is None or session.process.returncode is not None:
            return
        await self._stop_command(session)

    async def exec_release(self, exec_id: str) -> None:
        """Drop all state for a finished exec session.

        The main process is never released: the sandbox's own exit code is
        read from it for as long as the sandbox exists.
        """
        if exec_id == self._main_exec_id:
            return
        # Un-retire as well as drop. Leaving the id queued would let released
        # sessions count against _MAX_RETAINED_EXECS, so a probe exec'ing
        # every interval_ms would soon fill it and start evicting the caller's
        # real exec sessions -- exactly what releasing is meant to prevent.
        if exec_id in self._finished_execs:
            self._finished_execs.remove(exec_id)
        # No exit code is kept either. Releasing is the caller saying they are
        # done with this exec, which is exactly the case a tombstone exists to
        # serve -- keeping one would defeat the bound on the probe's execs for
        # the same reason as the line above.
        self._retired_execs.pop(exec_id, None)
        session = self._execs.pop(exec_id, None)
        if session is None:
            return
        stopping = self._discard(session)
        if stopping is not None:
            # Return once the command is dead, so a caller cleaning up after
            # it -- an aborted upload's temporary file -- is not racing it.
            await stopping

    def _discard(self, session: "_ExecSession") -> Optional[asyncio.Task]:
        """Drop every resource a session owns, wherever it left `_execs` from.

        Once a session is out of ``self._execs``, ``_teardown``'s kill loop can
        no longer see it -- so whatever removed it has to finish the job here,
        or the ``runsc exec`` child is orphaned for the life of the sandbox and
        its deadline task keeps a reference to the buffers alive.

        Args:
            session: The session being dropped.

        Returns:
            The task stopping the command, if it was still running.
        """
        for task in (session.deadline_task, session.reaper_task):
            if task is not None and task is not asyncio.current_task():
                task.cancel()
        for task in session.pump_tasks:
            task.cancel()
        session.close_stdin_for_good()
        stopping = None
        if session.process.returncode is None:
            # The task holds the session, so its pipes stay open until the
            # command inside the sandbox is dead; see _stop_command.
            stopping = asyncio.ensure_future(self._stop_command(session))
            self._stop_tasks.add(stopping)
            stopping.add_done_callback(self._stop_tasks.discard)
        else:
            _unlink_quietly(session.pid_file)
        for buffer in session.buffers.values():
            if buffer is not None:
                buffer.close()
        return stopping

    def _relieve_output_memory(self, excess: int) -> None:
        """Drop unread output until the sandbox is back within its cap.

        Finished commands' output goes first, oldest command first: it is the
        least likely to be read. Then the oldest output of whichever running
        stream holds the most. Paced streams and sessions their client releases
        itself are left alone; the filesystem's transfers must arrive whole,
        and their pacing bounds them already.

        Args:
            excess: How many bytes over the cap the sandbox is.
        """
        reason = (
            f"the sandbox's unread output passed {_SANDBOX_OUTPUT_CAP // 2**20} MiB"
        )
        for exec_id in list(self._finished_execs):
            session = self._execs.get(exec_id)
            if session is None or session.client_released:
                continue
            for buffer in session.buffers.values():
                if buffer is not None:
                    excess -= buffer.drop_oldest(excess, reason)
                    if excess <= 0:
                        return
        running = [
            buffer
            for session in self._execs.values()
            if not session.client_released
            for buffer in session.buffers.values()
            if buffer is not None and not buffer.paced
        ]
        for buffer in sorted(running, key=lambda b: b.memory_bytes, reverse=True):
            excess -= buffer.drop_oldest(excess, reason)
            if excess <= 0:
                return

    def _warn_output_dropped(self, reason: str) -> None:
        """Say once per sandbox why its output has started to be dropped.

        Readers only learn that bytes are gone; this is the one place that
        knows why.
        """
        if self._output_drop_warned:
            return
        self._output_drop_warned = True
        logger.warning(
            "Sandbox output is being dropped: %s. Reading consumes, so output "
            "read while it is produced is never dropped; what nobody reads is "
            "held in memory up to a bound, oldest unread output first to go, "
            "finished commands' before running ones'. Readers see the loss as "
            "truncated. Logged once per sandbox.",
            reason,
        )

    # -- one-shot helper ---------------------------------------------------

    async def exec_collect(
        self,
        command: Union[str, List[str]],
        *,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        stdin: Optional[bytes] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Run a command to completion and return its exit code and output.

        Used for the small, fixed-size filesystem operations, where one round
        trip beats opening a streaming session.
        """
        self._require_live()
        pid_file = self._pid_file(uuid.uuid4().hex)
        argv = self._runtime.backend.exec_argv(
            self._instance_id, command, cwd=cwd, env=env, shell=shell, pid_file=pid_file
        )
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await self._refuse_if_ended_during_spawn(process, pid_file)
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(input=stdin), timeout
            )
        except BaseException:
            # Any exit that is not a clean completion -- a timeout, but also
            # cancellation, which is a BaseException and so was previously
            # missed -- leaves the command running. Stop it here or nothing
            # will: it has no session, so the sandbox's own teardown never
            # sees it.
            if process.returncode is None:
                await self._stop_command(_ExecSession(process, pid_file))
                # Bounded for the reason _refuse_if_ended_during_spawn gives.
                try:
                    await asyncio.wait_for(process.wait(), 5)
                except asyncio.TimeoutError:
                    pass
            raise
        finally:
            _unlink_quietly(pid_file)
        returncode = _normalize_returncode(process.returncode)
        if returncode != 0:
            # A command the sandbox ended under failed because it went away,
            # not with an answer about the filesystem: say that instead.
            self._require_live()
        return {
            "returncode": returncode,
            "stdout": stdout,
            "stderr": stderr,
        }

    # -- internals ---------------------------------------------------------


async def _pump(stream: asyncio.StreamReader, buffer: _OutputBuffer) -> None:
    """Drain an OS pipe into a buffer until end of stream."""
    try:
        while True:
            chunk = await stream.read(_READ_SIZE)
            if not chunk:
                break
            buffer.feed(chunk)
            # Waits only for a paced stream's reader; any other returns at once.
            await buffer.pace()
    except asyncio.CancelledError:
        raise
    except Exception:
        # A closed transport just means the process is gone.
        pass
    finally:
        buffer.feed_eof()


def _session_returncode(session: "_ExecSession") -> Optional[int]:
    """The exit code a command reports, with Modal's timeout convention.

    A command we killed for exceeding its own deadline reports -1, not the
    ``128 + SIGKILL`` its exit status would otherwise map to: the timeout is
    the reason it ended, and Modal surfaces that rather than the mechanism.
    """
    if session.process.returncode is None:
        return None
    if session.timed_out:
        return _EXIT_CODE_EXEC_TIMEOUT
    return _normalize_returncode(session.process.returncode)


def _normalize_returncode(returncode: Optional[int]) -> Optional[int]:
    """Map a negative (signal) exit status to Modal's ``128 + signal``."""
    if returncode is None:
        return None
    if returncode < 0:
        return 128 + (-returncode)
    return returncode


def _remaining(deadline: Optional[float]) -> Optional[float]:
    if deadline is None:
        return None
    return deadline - time.monotonic()


def build_actor_options(
    cpu: Optional[float],
    memory: Optional[Union[str, int, float]],
    gpu_count: Optional[float],
    resources: Optional[Dict[str, float]],
) -> Dict[str, Any]:
    """Translate sandbox resource requests into Ray actor options."""
    options: Dict[str, Any] = {}
    if cpu is not None and cpu >= 0:
        options["num_cpus"] = cpu
    if memory is not None:
        parsed = parse_memory_bytes(memory)
        if parsed:
            options["memory"] = parsed
    if gpu_count:
        options["num_gpus"] = gpu_count
    if resources:
        options["resources"] = resources
    # The sandbox dies with the handle that created it, matching Modal's
    # behavior for a Sandbox whose creator exits without detaching.
    options["max_restarts"] = 0
    return options
