"""Client-side stream handles for Sandbox and container-process I/O.

The actor hands back raw bytes; everything about how those bytes are presented
-- UTF-8 decoding, line buffering, and
:class:`~ray.experimental.sandbox.modal.stream_type.StreamType` policy -- is
decided here.
"""

import asyncio
import codecs
import collections
import logging
import sys
import time
from typing import AsyncGenerator, Deque, List, Optional, Tuple, Union

import ray
from ray.experimental.sandbox.modal._actor import (
    _EXEC_OUTPUT_CAP,
    _MAIN_OUTPUT_CAP,
    _SANDBOX_OUTPUT_CAP,
    _STREAM_BACKPRESSURE_OBJECTS,
)
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
            # it would turn a lost stream into a silently empty one. The reader
            # accounts for the offset first and skips the bytes after; see
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


# What _StreamReader._pop_ready returns when nothing is decoded and waiting.
_NOTHING_READY = object()


class _StreamReader:
    """Reads one output stream of a Sandbox or container process.

    One position per reader, as Modal's StreamReader keeps from 1.6: every
    ``read()`` and every loop over it draws from the same place, so each byte
    is delivered once. Past end of output ``read()`` returns empty and a loop
    yields nothing. Stopping part-way -- a ``break``, ``aclose()`` -- keeps the
    position, and the next read or loop resumes exactly there. Measured on
    Modal 1.6.1, the same on its V1 and V2 backends.
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
        # Bytes taken from the actor so far: where the next stream opens, and
        # what the actor may free.
        self._offset = 0
        self._eof = False
        # Pulls take turns, so a loop and a read(), or two tasks, share one
        # stream instead of colliding on it.
        self._lock = asyncio.Lock()
        # The open stream of decoded batches, whether it splits lines, and its
        # items not handed out yet.
        self._batches: Optional[AsyncGenerator[List, None]] = None
        self._batches_split = True
        self._ready: Deque[Union[str, bytes]] = collections.deque()
        # Decoding state that outlives any one stream, so a character or a
        # line cut where one stream closed carries on in the next.
        self._decoder = (
            codecs.getincrementaldecoder("utf-8")(errors="strict") if text else None
        )
        self._splitter = _LineSplitter() if by_line else None
        # UTF-8 continuation bytes still to skip after a gap; see
        # _decoded_batches.
        self._skip = 0
        self._ack_task: Optional[asyncio.Task] = None
        self._print_task: Optional[asyncio.Task] = None
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
        """Whether output was dropped before this reader got it."""
        return self._truncated

    @property
    def bytes_lost(self) -> int:
        """Total bytes dropped before this reader got them."""
        return self._bytes_lost

    async def read(self) -> Union[str, bytes]:
        """Read from where this reader stands to the end of output.

        What an earlier ``read()`` or loop took is not read again, as on Modal:
        a second ``read()`` after the end returns empty.
        """
        self._check_readable("read")
        async with self._lock:
            # Popped one at a time rather than copied and cleared: a blocking
            # loop on another thread may pop from the same deque meanwhile.
            parts = []
            while (item := self._pop_ready()) is not _NOTHING_READY:
                parts.append(item)
            while True:
                # Unsplit: everything is joined back together anyway, and
                # splitting a Sandbox's own line-buffered output cost a 256
                # MiB read about 25 seconds.
                batch = await self._next_batch(split=False)
                if batch is None:
                    break
                parts.extend(batch)
        return ("" if self._text else b"").join(parts)

    def __aiter__(self) -> "_StreamIteration":
        self._check_readable("__aiter__")
        return _StreamIteration(self)

    async def __anext__(self) -> Union[str, bytes]:
        # Checked here as well as in __aiter__, so a DEVNULL stream names the
        # method the caller actually used. (Modal's goes through __aiter__ and
        # names that; the exception type is the same either way.)
        self._check_readable("__anext__")
        return await self._next_item()

    async def aclose(self) -> None:
        """Close the stream behind this reader. Safe to call more than once.

        The position is kept, as on Modal: a later read or loop reopens the
        stream where this one stopped. On a DEVNULL stream, which never has
        one, this is a silent no-op rather than the refusal its reads give.
        """
        if self._print_task is not None:
            self._print_task.cancel()
            self._print_task = None
        await self._close_stream()

    # -- internals ---------------------------------------------------------

    def _check_readable(self, method: str) -> None:
        if self._stream_type == StreamType.DEVNULL:
            raise ValueError(_DEVNULL_MESSAGE.format(method))
        if self._stream_type == StreamType.STDOUT:
            raise InvalidError(_STDOUT_MESSAGE)

    async def _next_item(self) -> Union[str, bytes]:
        if self._ready:
            return self._ready.popleft()
        async with self._lock:
            while not self._ready:
                batch = await self._next_batch()
                if batch is None:
                    raise StopAsyncIteration
                self._ready.extend(batch)
            return self._ready.popleft()

    def _pop_ready(self):
        """An item decoded and waiting, or _NOTHING_READY. Never waits.

        Safe from another thread: a deque's pops and appends are atomic. The
        blocking iterator takes items this way, so between its trips to the
        event loop they stay here -- where a later read() finds what a loop
        left behind.
        """
        try:
            return self._ready.popleft()
        except IndexError:
            return _NOTHING_READY

    async def _next_batch(
        self, split: bool = True
    ) -> Optional[List[Union[str, bytes]]]:
        """The next decoded items from this reader's position; None at the end.

        Called with the lock held. Opens a stream at the position if none is
        open. ``split`` asks for lines, if this reader splits them at all;
        read() asks for none. A stream open in the other mode is closed and
        reopened at the same position.
        """
        while not self._eof:
            if self._batches is not None and self._batches_split != split:
                batches, self._batches = self._batches, None
                await batches.aclose()
            if self._batches is None:
                self._batches = self._decoded_batches(split)
                self._batches_split = split
            try:
                return await self._batches.__anext__()
            except StopAsyncIteration:
                self._batches = None
            except BaseException:
                # The stream is spent. The position stays where it got to, so
                # a later read reopens there.
                self._batches = None
                raise
        return None

    async def _close_stream(self) -> None:
        # A stream another pull is reading from is left for that pull to
        # finish, as Modal's aclose() leaves it.
        if self._lock.locked():
            return
        async with self._lock:
            batches, self._batches = self._batches, None
            if batches is not None:
                await batches.aclose()

    async def _raw(self, mark_gaps: bool) -> AsyncGenerator[Union[bytes, object], None]:
        """Yield raw chunks from this reader's position, moving it as they go.

        Ends at end of stream or at the exec's deadline. ``mark_gaps`` yields
        :data:`_GAP` just before the first bytes after each gap, for a decoder
        that must not join the bytes on either side of one.
        """
        if self._deadline_passed():
            # Answered here, without the actor, as Modal's client answers it.
            return
        cursor = self._offset
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
                        # The actor dropped bytes this reader had not got yet.
                        self._note_gap(offset - cursor)
                        self._offset = cursor = offset
                        if mark_gaps:
                            yield _GAP
                    # Moved before the bytes are handed on, never after: the
                    # consumer finishes with a chunk before it can be closed,
                    # and a reopen must start past what it got.
                    cursor = offset + len(chunk)
                    self._offset = cursor
                    # A zero-length chunk exists only to carry an offset, and
                    # has nothing for a decoder or a line splitter to do.
                    if chunk:
                        yield chunk
        finally:
            # Closed here rather than left to the collector, so stopping early
            # -- at the deadline, or because the caller did -- cancels the
            # actor's streaming task straight away.
            await stream.aclose()

    def _deadline_passed(self) -> bool:
        return self._deadline is not None and time.monotonic() >= self._deadline

    def _note_gap(self, lost: int) -> None:
        """Record output dropped before this reader got it.

        Never raises: Modal reports a Sandbox's dropped output with a warning
        and carries on, and raising here would crash a program that runs clean
        there. Logged once rather than per gap: a reader that stays behind gaps
        on every read, and the running total is on ``bytes_lost``.
        """
        self._truncated = True
        self._bytes_lost += lost
        if not self._gap_logged:
            self._gap_logged = True
            logger.warning(
                "Sandbox output was truncated: %d bytes on fd %d were dropped "
                "before this reader got them. Output read as it is produced is "
                "never dropped; output nobody reads is held in memory up to a "
                "bound -- the newest %d MiB of an exec'd command's stream, %d "
                "MiB of the Sandbox's own, %d MiB across the whole sandbox -- "
                "and the oldest goes first. Further gaps on this stream are "
                "counted in .bytes_lost but not logged.",
                lost,
                self._file_descriptor,
                _EXEC_OUTPUT_CAP // 2**20,
                _MAIN_OUTPUT_CAP // 2**20,
                _SANDBOX_OUTPUT_CAP // 2**20,
            )

    async def _decoded_batches(
        self, split: bool = True
    ) -> AsyncGenerator[List[Union[str, bytes]], None]:
        """Decode and line-split the raw stream, one chunk's items per list.

        Items are produced a whole chunk at a time -- a 1 MiB chunk of short
        lines is thousands of them -- rather than one async step each: per
        line, a generator hop and, for blocking iteration, a task and a loop
        turn used to cost ~16 us, capping iteration near 60k lines/s.

        The decoder and line splitter are the reader's own, so what one stream
        leaves half-done -- a character, a line -- the next one finishes. At a
        clean end of stream the reader is marked done and the actor told.

        Without ``split``, chunks are decoded whole; a line an earlier loop
        left unfinished goes out first.
        """
        decoder = self._decoder
        splitter = self._splitter if split else None
        if not split and self._splitter is not None:
            partial = decoder.decode(self._splitter.finish())
            if partial:
                yield [partial]
        # Only text needs to know where the gaps are: bytes join up anyway.
        source = self._raw(mark_gaps=decoder is not None)
        try:
            async for chunk in source:
                if chunk is _GAP:
                    # The bytes on either side of a gap do not join up. A
                    # character can be cut at either edge -- a drop at a pipe
                    # read's boundary -- and the strict decoder raised on the
                    # halves, failing the read instead of returning what is
                    # still there.
                    items: List[Union[str, bytes]] = []
                    if splitter is not None:
                        # An unfinished line ends at the gap: what follows it
                        # is not its continuation.
                        before = decoder.decode(splitter.finish())
                        if before:
                            items.append(before)
                    held, _ = decoder.getstate()
                    decoder.reset()
                    if held:
                        self._note_gap(len(held))
                    # UTF-8 resynchronizes: at most three continuation bytes
                    # lead into the next character's first byte.
                    self._skip = 3
                    if items:
                        yield items
                    continue
                if self._skip:
                    cut = _continuation_prefix(chunk, self._skip)
                    if cut:
                        chunk = chunk[cut:]
                        self._note_gap(cut)
                    self._skip = 0 if chunk else self._skip - cut
                    if not chunk:
                        continue
                if splitter is None:
                    item = decoder.decode(chunk) if decoder else chunk
                    if item:
                        yield [item]
                    continue
                items = []
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
            # cut off mid-sequence -- goes out only on a clean end: if the
            # consumer stopped early, yielding here would fire inside a
            # closing generator, and the next stream carries it on instead.
            rest = splitter.finish() if splitter is not None else b""
            tail = decoder.decode(rest, final=True) if decoder else rest
            self._eof = True
            self._send_ack()
            if tail:
                yield [tail]
        finally:
            await source.aclose()

    def _send_ack(self) -> None:
        """Tell the actor this stream was read to the end, without waiting.

        It frees what it kept back for chunks in flight, and a finished
        command read to the end is released. Best-effort: an actor already
        gone has nothing left to free.
        """

        async def ack():
            try:
                await self._actor.stream_ack.remote(
                    self._exec_id, self._file_descriptor, self._offset
                )
            except Exception:
                pass

        try:
            self._ack_task = asyncio.ensure_future(ack())
        except RuntimeError:
            # No running loop to send it from.
            pass

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
            async for chunk in self._raw(mark_gaps=False):
                sys.stdout.write(decoder.decode(chunk))
                sys.stdout.flush()
            # A character cut off by the end of the stream is held in the
            # decoder; without this it was dropped rather than printed as the
            # replacement character `errors="replace"` promises.
            tail = decoder.decode(b"", final=True)
            if tail:
                sys.stdout.write(tail)
                sys.stdout.flush()
            self._eof = True
            self._send_ack()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug(
                "Stopped forwarding fd %d to stdout",
                self._file_descriptor,
                exc_info=True,
            )


class _StreamIteration:
    """One ``for`` or ``async for`` over a StreamReader, at its shared position.

    Every loop gets one of these, and all of them draw from the reader, so a
    loop that stopped part-way is continued by the next loop or read().
    ``pop_ready`` lets the blocking iterator take items already decoded
    without a trip to the event loop.
    """

    __slots__ = ("_reader",)

    # What pop_ready returns when nothing is waiting; _sync compares to it.
    NOTHING_READY = _NOTHING_READY

    def __init__(self, reader: _StreamReader):
        self._reader = reader

    def __aiter__(self) -> "_StreamIteration":
        return self

    async def __anext__(self) -> Union[str, bytes]:
        return await self._reader._next_item()

    def pop_ready(self):
        return self._reader._pop_ready()

    async def aclose(self) -> None:
        await self._reader._close_stream()


# Yielded by _StreamReader._raw, when asked, just before the first bytes after
# a gap in the stream.
_GAP = object()


def _continuation_prefix(chunk: bytes, limit: int) -> int:
    """How many of ``chunk``'s first bytes, at most ``limit``, are UTF-8
    continuation bytes (``0b10xxxxxx``) -- the rest of a character whose
    start was lost."""
    count = 0
    while count < limit and count < len(chunk) and 0x80 <= chunk[count] <= 0xBF:
        count += 1
    return count


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
