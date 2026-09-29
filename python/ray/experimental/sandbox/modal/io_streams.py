"""Client-side stream handles for Sandbox and container-process I/O.

The actor hands back raw bytes; everything about how those bytes are presented
-- UTF-8 decoding, line buffering, and
:class:`~ray.experimental.sandbox.modal.stream_type.StreamType` policy -- is
decided here.
"""

import asyncio
import codecs
import collections
import io
import logging
import sys
import time
from typing import AsyncGenerator, Deque, List, Optional, Tuple, Union

import ray
from ray.experimental.sandbox.modal._sync import synchronize_api
from ray.experimental.sandbox.modal.exception import (
    InvalidError,
    _sandbox_gone_as_not_found,
)
from ray.experimental.sandbox.modal.stream_type import StreamType

logger = logging.getLogger(__name__)

# Modal caps buffered stdin differently depending on the route the bytes take:
# a Sandbox's own stdin goes through their server, an exec's goes through the
# task command router, which allows eight times as much. Both are matched here
# so a write that Modal accepts is not rejected on this backend.
MAX_BUFFER_SIZE = 2 * 1024 * 1024
TASK_COMMAND_ROUTER_MAX_BUFFER_SIZE = 16 * 1024 * 1024

# Modal's name for the cap above is the one on the right; this spelling came
# first here and is kept so existing imports keep working.
EXEC_MAX_BUFFER_SIZE = TASK_COMMAND_ROUTER_MAX_BUFFER_SIZE

# How many chunks the actor may produce before the client has consumed them.
# Ray runs a streaming generator eagerly, so without this a chatty process
# would push its whole output into the caller's memory store as fast as it can
# write. At this bound the actor parks on an asyncio.Event until the client
# catches up. Chunks run up to 1 MiB for bulk output (_MAX_YIELD_BYTES), so
# this keeps at most ~8 MiB in flight per stream: enough to hide the
# round-trip latency, little enough to stay bounded for an endless stream.
_STREAM_BACKPRESSURE_OBJECTS = 8

_DEVNULL_MESSAGE = "{} is not supported for a stream configured with StreamType.DEVNULL"
_STDOUT_MESSAGE = "Output can only be retrieved using the PIPE stream type."


async def iter_stream(
    actor, exec_id: str, file_descriptor: int, start_offset: int = 0
) -> AsyncGenerator[Tuple[int, bytes], None]:
    """Yield ``(offset, chunk)`` from one long-lived actor streaming task.

    The transport primitive behind every read of sandbox output: a single
    streaming-generator task serves the whole stream, so the cost is one RPC
    rather than one per chunk.

    ``start_offset`` resumes a stream that was cut short. It is passed at
    creation because that is the only way in: a Ray streaming generator is
    pull-only, so the cursor cannot be handed to one already running.
    """
    gen = actor.stream_output.options(
        _generator_backpressure_num_objects=_STREAM_BACKPRESSURE_OBJECTS,
    ).remote(exec_id, file_descriptor, start_offset)
    reached_eof = False
    try:
        # A streaming generator yields ObjectRefs, not values, so each one has
        # to be awaited. Rebinding `ref` every iteration drops the previous
        # reference, which is what evicts the chunk before it from the local
        # in-memory store -- consuming a chunk does not free it, releasing its
        # ref does. Never accumulate these refs: holding them pins every chunk
        # in this process for the life of the stream.
        async for ref in gen:
            # Empty items are passed through rather than filtered here: an
            # evicted session reports the size of the hole it left as a
            # zero-length chunk at the offset its stream reached, and dropping
            # it would turn a lost stream into a silently empty one. Consumers
            # account for the offset first and skip the bytes after; see
            # _StreamReader._raw.
            yield await ref
        reached_eof = True
    finally:
        # Only when the consumer stopped early. Reaching end of stream means
        # the actor task already returned, so there is nothing to cancel.
        if not reached_eof:
            _cancel_stream(gen)


def _cancel_stream(gen) -> None:
    """Stop the actor task behind a stream that is no longer being read.

    Dropping an ObjectRefGenerator releases the caller's unconsumed refs but
    leaves the task running to end of stream, so an abandoned stream would keep
    an actor task alive. The generator has no ``aclose``; cancelling is the
    supported way to stop it.
    """
    if isinstance(gen, ray.ObjectRefGenerator):
        ray.cancel(gen)


class _StreamReader:
    """Reads one output stream of a Sandbox or container process.

    Treat this as an *iterable*, not an iterator: the generator behind
    ``__aiter__`` is created once and memoized, so a second full iteration
    yields nothing.
    """

    def __init__(
        self,
        actor,
        exec_id: str,
        file_descriptor: int,
        *,
        stream_type: StreamType = StreamType.PIPE,
        text: bool = True,
        by_line: bool = False,
        deadline: Optional[float] = None,
    ):
        if by_line and not text:
            raise ValueError("line-buffering is only supported when text=True")

        self._actor = actor
        # When the exec's timeout runs out, as a time.monotonic() value; None
        # for no timeout, and for a Sandbox's own streams. Measured on Modal,
        # an exec's output is readable only until then: a read in progress
        # ends at the deadline -- a 256 MiB read with timeout=8 stopped at 8s
        # with 113 MiB delivered -- and one started later returns nothing,
        # silently in both cases, whether or not the command had finished.
        self._deadline = deadline
        self._exec_id = exec_id
        self._file_descriptor = file_descriptor
        self._stream_type = stream_type
        self._text = text
        self._by_line = by_line
        self._read_gen: Optional["_Flatten"] = None
        self._print_task: Optional[asyncio.Task] = None
        # Absolute position in the stream, so a generator that is cancelled or
        # replaced resumes exactly. Modal keeps the same state as
        # `last_entry_id`.
        self._offset = 0
        # Private, with properties below: synchronize_api forwards only
        # class-level entries, so a plain instance attribute would be invisible
        # on the public blocking StreamReader.
        self._truncated = False
        self._bytes_lost = 0
        self._gap_logged = False

        if stream_type == StreamType.STDOUT:
            self._print_task = asyncio.ensure_future(self._print_all())

    @property
    def file_descriptor(self) -> int:
        """The file descriptor this stream reads: 1 for stdout, 2 for stderr."""
        return self._file_descriptor

    @property
    def truncated(self) -> bool:
        """Whether output was evicted before this reader reached it."""
        return self._truncated

    @property
    def bytes_lost(self) -> int:
        """Total bytes evicted before this reader reached them."""
        return self._bytes_lost

    async def read(self) -> Union[str, bytes]:
        """Read the stream from its start to end of output and return it.

        Every call reads the whole retained stream again, as Modal's does, and
        never disturbs an ``async for`` in progress: the two have separate
        cursors.
        """
        self._check_readable("read")
        buffer = io.StringIO() if self._text else io.BytesIO()
        # Line splitting only moves where chunk boundaries fall, and reading to
        # end of stream joins every chunk back together -- so for this one
        # caller it is re-chunking whose result is discarded. Skipping it is
        # byte-for-byte identical, since a newline cannot occur inside a
        # multi-byte UTF-8 sequence.
        async for batch in self._decoded_batches(by_line=False, start=0):
            for chunk in batch:
                buffer.write(chunk)
        return buffer.getvalue()

    def __aiter__(self) -> "_Flatten":
        self._check_readable("__aiter__")
        if self._read_gen is None:
            self._read_gen = _Flatten(self._decoded_batches())
        return self._read_gen

    async def __anext__(self) -> Union[str, bytes]:
        # Checked here as well as in __aiter__, so a DEVNULL stream names the
        # method the caller actually used. (Modal's goes through __aiter__ and
        # names that; the exception type is the same either way.)
        self._check_readable("__anext__")
        return await self.__aiter__().__anext__()

    async def aclose(self) -> None:
        """Release the stream. Safe to call more than once, whatever its type.

        Modal's public ``aclose()`` only ever closes an iteration generator
        that exists, so on a DEVNULL stream -- which never has one -- it is a
        silent no-op rather than the refusal its read methods give.
        """
        if self._print_task is not None:
            self._print_task.cancel()
            self._print_task = None
        if self._read_gen is not None:
            await self._read_gen.aclose()
            self._read_gen = None

    # -- internals ---------------------------------------------------------

    def _check_readable(self, method: str) -> None:
        if self._stream_type == StreamType.DEVNULL:
            raise ValueError(_DEVNULL_MESSAGE.format(method))
        if self._stream_type == StreamType.STDOUT:
            raise InvalidError(_STDOUT_MESSAGE)

    async def _raw(self, start: Optional[int] = None) -> AsyncGenerator[bytes, None]:
        """Yield raw chunks from the actor, tracking position and gaps.

        With ``start`` None this continues the reader's own cursor, which is
        what iteration uses. Otherwise it is an independent pass from
        ``start``, as ``read()`` makes.
        """
        if self._deadline_passed():
            # Answered here, without the actor, as Modal's client answers it.
            return
        cursor = self._offset if start is None else start
        pass_lost = 0
        stream = iter_stream(self._actor, self._exec_id, self._file_descriptor, cursor)
        try:
            with _sandbox_gone_as_not_found():
                async for offset, chunk in stream:
                    if self._deadline_passed():
                        logger.debug(
                            "Stopped reading fd %d of exec %s at its timeout, as "
                            "Modal does.",
                            self._file_descriptor,
                            self._exec_id,
                        )
                        return
                    if offset > cursor:
                        # The actor dropped bytes this pass had not reached yet.
                        pass_lost += offset - cursor
                        self._note_gap(offset - cursor, pass_lost)
                    cursor = offset + len(chunk)
                    if start is None:
                        self._offset = cursor
                    # After the accounting above, never before it: a zero-length
                    # chunk exists only to carry an offset, and has nothing for a
                    # decoder or a line splitter to do.
                    if chunk:
                        yield chunk
        finally:
            # Closed here rather than left to the collector, so stopping early
            # -- at the deadline, or because the caller did -- cancels the
            # actor's streaming task straight away.
            await stream.aclose()

    def _deadline_passed(self) -> bool:
        return self._deadline is not None and time.monotonic() >= self._deadline

    def _note_gap(self, lost: int, pass_lost: int) -> None:
        """Record output dropped before this reader reached it.

        Never raises: Modal reports a Sandbox's dropped output with a warning
        and carries on, and raising here would crash a program that runs clean
        there. ``bytes_lost`` is the most any single pass lost, so reading the
        same stream twice does not count one hole twice. Logged once rather
        than per gap: a reader that stays behind gaps on every read, and the
        figure is on ``bytes_lost``.
        """
        self._truncated = True
        self._bytes_lost = max(self._bytes_lost, pass_lost)
        if not self._gap_logged:
            self._gap_logged = True
            logger.warning(
                "Sandbox output was truncated: %d bytes on fd %d were no "
                "longer retained when this reader reached them. A Sandbox's "
                "own output keeps only its newest 256 MiB, as on Modal; an "
                "exec's output is kept whole unless the sandbox had no room "
                "to spill it (RAY_SANDBOX_OUTPUT_SPILL_LIMIT, or the node's "
                "disk nearing full -- the sandbox's log says which), when the "
                "oldest output goes to bound memory, finished commands' first. "
                "Further gaps on this stream are counted in .bytes_lost but "
                "not logged.",
                lost,
                self._file_descriptor,
            )

    async def _decoded_batches(
        self, by_line: Optional[bool] = None, start: Optional[int] = None
    ) -> AsyncGenerator[List[Union[str, bytes]], None]:
        """Decode and line-split the raw stream, one chunk's items per list.

        Items are produced a whole chunk at a time -- a 1 MiB chunk of short
        lines is thousands of them -- rather than one async step each: per
        line, a generator hop and, for blocking iteration, a task and a loop
        turn used to cost ~16 us, capping iteration near 60k lines/s.
        :class:`_Flatten` hands them out singly again.

        ``by_line`` overrides the reader's own setting, for a caller whose
        output does not depend on where the chunk boundaries fall. ``start``
        makes an independent pass; see :meth:`_raw`.
        """
        if by_line is None:
            by_line = self._by_line
        decoder = (
            codecs.getincrementaldecoder("utf-8")(errors="strict")
            if self._text
            else None
        )
        splitter = _LineSplitter() if by_line else None
        source = self._raw(start)
        try:
            async for chunk in source:
                if splitter is None:
                    item = decoder.decode(chunk) if decoder else chunk
                    if item:
                        yield [item]
                    continue
                items: List[Union[str, bytes]] = []
                try:
                    # Split as bytes, then decode each line: the same lines,
                    # since a newline cannot occur inside a multi-byte UTF-8
                    # sequence, and invalid UTF-8 still fails at its own line
                    # with every line before it delivered first.
                    for line in splitter.feed(chunk):
                        items.append(decoder.decode(line) if decoder else line)
                except UnicodeDecodeError:
                    if items:
                        yield items
                    raise
                if items:
                    yield items
            # What is held back -- an unfinished line, bytes of a character
            # cut off mid-sequence -- goes out only on normal completion: if
            # the consumer stopped early, yielding here would fire inside a
            # closing generator.
            rest = splitter.finish() if splitter is not None else b""
            tail = decoder.decode(rest, final=True) if decoder else rest
            if tail:
                yield [tail]
        finally:
            await source.aclose()

    async def _print_all(self) -> None:
        """Pump the stream to the local stdout, for StreamType.STDOUT.

        Nothing ever awaits this task, so it has to consume its own failures.
        Left to propagate, the ordinary end of a sandbox -- terminate() killing
        the actor mid-stream -- would reach the caller as asyncio's "Task
        exception never retrieved" on the shared daemon loop at GC time, with
        no context and no way to catch it.
        """
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        try:
            async for chunk in self._raw():
                sys.stdout.write(decoder.decode(chunk))
                sys.stdout.flush()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug(
                "Stopped forwarding fd %d to stdout",
                self._file_descriptor,
                exc_info=True,
            )


class _LineSplitter:
    """Cuts a byte stream into complete lines, a chunk at a time.

    Only ``\\n`` ends a line, as on Modal. Linear in the input: each chunk is
    split once, in C, and an unfinished line is carried in a bytearray, so
    neither many lines in a chunk nor one line across many chunks is copied
    over and over.
    """

    def __init__(self):
        self._partial = bytearray()

    def feed(self, chunk: bytes) -> List[bytes]:
        """The lines ``chunk`` completes, each ending in its newline."""
        parts = chunk.split(b"\n")
        if len(parts) == 1:
            self._partial += chunk
            return []
        if self._partial:
            self._partial += parts[0]
            parts[0] = bytes(self._partial)
        self._partial = bytearray(parts.pop())
        return [part + b"\n" for part in parts]

    def finish(self) -> bytes:
        """Whatever trails the last newline: an unterminated final line."""
        rest = bytes(self._partial)
        self._partial = bytearray()
        return rest


class _Flatten:
    """Hands out a batch generator's items one at a time.

    ``take_ready`` gives up the items already produced without waiting, all at
    once. The blocking iterator uses it (see ``_sync._BlockingIterator``) to
    cross to its event loop once per batch instead of once per item.
    """

    def __init__(self, batches: AsyncGenerator[List, None]):
        self._batches = batches
        self._ready: Deque = collections.deque()

    def __aiter__(self) -> "_Flatten":
        return self

    async def __anext__(self):
        while not self._ready:
            self._ready.extend(await self._batches.__anext__())
        return self._ready.popleft()

    def take_ready(self, limit: int) -> List:
        """Up to ``limit`` items that are already here, oldest first."""
        count = min(limit, len(self._ready))
        return [self._ready.popleft() for _ in range(count)]

    async def aclose(self) -> None:
        self._ready.clear()
        await self._batches.aclose()


class _StreamWriter:
    """Writes to the stdin of a Sandbox or container process.

    ``write`` only buffers; call ``drain`` to actually send.
    """

    def __init__(self, actor, exec_id: str, max_buffer_size: int = MAX_BUFFER_SIZE):
        self._actor = actor
        self._exec_id = exec_id
        self._max_buffer_size = max_buffer_size
        self._buffer = bytearray()
        self._is_closed = False
        # Where the buffer's first byte sits in the stdin stream: everything
        # before it has been written. Sent with each write, so the actor can
        # order writes and drop a repeated one.
        self._offset = 0

    def write(self, data: Union[bytes, bytearray, memoryview, str]) -> None:
        """Buffer data to be sent on the next ``drain``."""
        if self._is_closed:
            raise ValueError("Stdin is closed. Cannot write to it.")
        if isinstance(data, str):
            data = data.encode("utf-8")
        elif isinstance(data, (bytearray, memoryview)):
            data = bytes(data)
        elif not isinstance(data, bytes):
            raise TypeError(
                f"data argument must be a bytes-like object, not "
                f"{type(data).__name__}"
            )
        if len(self._buffer) + len(data) > self._max_buffer_size:
            raise BufferError(
                "Buffer size exceed limit. Call drain to flush the buffer."
            )
        self._buffer.extend(data)

    def write_eof(self) -> None:
        """Mark the end of input. The close is sent on the next ``drain``."""
        self._is_closed = True

    async def drain(self) -> None:
        """Flush the buffer to the process, and close stdin if ``write_eof`` was called."""
        with _sandbox_gone_as_not_found():
            if self._buffer:
                data = bytes(self._buffer)
                offset = self._offset
                await self._actor.write_stdin.remote(self._exec_id, data, offset)
                # Only what was sent is dropped, and only after it was written,
                # so a failed drain can simply be retried. Not clear(): a
                # write() made while this drain was in flight belongs to the
                # next one. And only if no other drain already accounted for
                # these bytes -- two concurrent drains send the same offset,
                # and the actor writes it once.
                if self._offset == offset:
                    del self._buffer[: len(data)]
                    self._offset = offset + len(data)
            if self._is_closed:
                await self._actor.close_stdin.remote(self._exec_id, self._offset)


StreamReader = synchronize_api(_StreamReader)
StreamWriter = synchronize_api(_StreamWriter)
