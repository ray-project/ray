"""Unit tests for the Modal-compatible stream handles.

These drive :class:`_StreamReader` / :class:`_StreamWriter` against a fake
actor, so they need neither runsc nor a Ray cluster.
"""

import asyncio
import functools
import sys
import time
import types
from typing import Dict, List, Optional

import pytest

import ray
from ray.experimental.sandbox.modal import _actor, _sync
from ray.experimental.sandbox.modal._actor import (
    _COALESCE_IDLE_GAP,
    _COALESCE_MAX_WINDOW,
    _COALESCE_TARGET_BYTES,
    _MAX_YIELD_BYTES,
    STDERR_FD,
    STDOUT_FD,
    _MemoryBudget,
    _OutputBuffer,
)
from ray.experimental.sandbox.modal.exception import (
    InvalidError,
    NotFoundError,
    SandboxFilesystemError,
)
from ray.experimental.sandbox.modal.io_streams import (
    MAX_BUFFER_SIZE,
    StreamReader,
    _LineSplitter,
    _StreamReader,
    _StreamWriter,
)
from ray.experimental.sandbox.modal.stream_type import StreamType


class _FakeRef:
    """Stands in for an ObjectRef: a value that has to be awaited."""

    def __init__(self, value):
        self._value = value

    def __await__(self):
        async def _get():
            return self._value

        return _get().__await__()


class _RemoteShim:
    """Gives a plain coroutine the ``.remote()`` call shape of an actor method.

    ``options`` returns self, so the real code's
    ``method.options(...).remote(...)`` works unchanged against the fake.
    """

    def __init__(self, fn):
        self._fn = fn

    def options(self, **kwargs):
        return self

    def remote(self, *args, **kwargs):
        return self._fn(*args, **kwargs)


class FakeActor:
    """Stands in for ``_SandboxActor``, replaying canned output."""

    def __init__(
        self,
        streams: Optional[Dict[int, List[bytes]]] = None,
        base_offset: int = 0,
    ):
        self._streams = {fd: list(chunks) for fd, chunks in (streams or {}).items()}
        # Bytes the actor has already evicted, as if a slow reader had been
        # overtaken. Reads below this resume at it, and the caller sees the jump.
        self._base_offset = base_offset
        self.stream_calls: List[int] = []
        self.stdin_writes: List[bytes] = []
        self.stdin_offsets: List[Optional[int]] = []
        self.stdin_close_offset: Optional[int] = None
        self.stdin_closed = False
        # (fd, offset) for every stream_ack received.
        self.acks: List[tuple] = []

    def __getattr__(self, name):
        impl = getattr(type(self), f"_{name}", None)
        if impl is None:
            raise AttributeError(name)
        return _RemoteShim(functools.partial(impl, self))

    async def _stream_output(self, exec_id, fd, start_offset=0):
        """Mirror the real streaming generator: yield refs of (offset, chunk).

        Offset-addressed, matching the real ``stream_output``: a reader that
        reopens at its own position continues exactly there.
        """
        self.stream_calls.append(start_offset)
        chunks = self._streams.get(fd)
        if chunks is None:
            return
        floor = max(start_offset, self._base_offset)
        offset = 0
        for chunk in chunks:
            if isinstance(chunk, float):
                # A pause in the output, as a slow process would leave.
                await asyncio.sleep(chunk)
                continue
            end = offset + len(chunk)
            if end > floor:
                begin = max(offset, floor)
                yield _FakeRef((begin, chunk[begin - offset :]))
            offset = end

    async def _stream_ack(self, exec_id, fd, offset):
        self.acks.append((fd, offset))

    async def _write_stdin(self, exec_id, data, offset=None):
        self.stdin_writes.append(data)
        self.stdin_offsets.append(offset)

    async def _close_stdin(self, exec_id, offset=None):
        self.stdin_closed = True
        self.stdin_close_offset = offset


class GoneActor:
    """An actor that has gone away: every call fails the way Ray reports it."""

    def __getattr__(self, name):
        if name == "stream_output":
            return _RemoteShim(self._gone_stream)
        return _RemoteShim(self._gone)

    async def _gone(self, *args, **kwargs):
        raise ray.exceptions.RayActorError()

    async def _gone_stream(self, *args, **kwargs):
        raise ray.exceptions.RayActorError()
        yield  # pragma: no cover -- makes this an async generator


def make_reader(chunks: List[bytes], fd: int = STDOUT_FD, **kwargs) -> _StreamReader:
    return _StreamReader(FakeActor({fd: chunks}), "exec-1", fd, **kwargs)


def run(coro):
    return asyncio.run(coro)


# -- reading ---------------------------------------------------------------


@pytest.mark.parametrize(
    "chunks,text,expected",
    [
        ([b"hello ", b"world"], True, "hello world"),
        ([b"hello ", b"world"], False, b"hello world"),
        ([], True, ""),
        ([], False, b""),
    ],
)
def test_read_returns_whole_stream(chunks, text, expected):
    reader = make_reader(chunks, text=text)
    assert run(reader.read()) == expected


@pytest.mark.parametrize(
    "chunks,expected",
    [
        ([b"one\ntwo\n", b"three\n"], "one\ntwo\nthree\n"),
        ([b"no trailing", b" newline"], "no trailing newline"),
        ([b"caf\xc3", b"\xa9\nau lait\n"], "café\nau lait\n"),
    ],
)
def test_read_ignores_line_buffering(chunks, expected):
    """Reading to end of stream joins every line, so line buffering cannot
    change what read() returns."""
    assert run(make_reader(chunks, by_line=True).read()) == expected
    assert run(make_reader(chunks, by_line=False).read()) == expected


def test_read_decodes_multibyte_split_across_chunks():
    # "é" is two bytes; split it so a naive per-chunk decode would fail.
    reader = make_reader([b"caf\xc3", b"\xa9 au lait"], text=True)
    assert run(reader.read()) == "café au lait"


def test_iteration_yields_chunks_unbuffered():
    reader = make_reader([b"a", b"b", b"c"], text=True)

    async def collect():
        return [chunk async for chunk in reader]

    assert run(collect()) == ["a", "b", "c"]


@pytest.mark.parametrize(
    "chunks,expected",
    [
        ([b"one\ntwo\n"], ["one\n", "two\n"]),
        ([b"one\ntw", b"o\n"], ["one\n", "two\n"]),
        # A trailing partial line is still delivered at end of stream.
        ([b"one\ntwo"], ["one\n", "two"]),
        ([b"no newline at all"], ["no newline at all"]),
    ],
)
def test_line_buffering_splits_on_newlines(chunks, expected):
    reader = make_reader(chunks, text=True, by_line=True)

    async def collect():
        return [line async for line in reader]

    assert run(collect()) == expected


def test_line_buffering_requires_text_mode():
    with pytest.raises(ValueError, match="line-buffering is only supported"):
        make_reader([b"x"], text=False, by_line=True)


@pytest.mark.parametrize(
    "chunks,expected",
    [
        # A line across several chunks, and lines ending exactly at a chunk's end.
        ([b"lo", b"ng li", b"ne\nnext\n"], ["long line\n", "next\n"]),
        ([b"a\n", b"b\n"], ["a\n", "b\n"]),
        ([b"\n\n"], ["\n", "\n"]),
        # Only \n ends a line, as on Modal: \r and the other separators
        # str.splitlines() honours stay inside the line.
        ([b"a\r\nb\x0bc\x1cd\n"], ["a\r\n", "b\x0bc\x1cd\n"]),
        # A multi-byte character cut across chunks, inside a line.
        ([b"caf\xc3", b"\xa9\n"], ["café\n"]),
    ],
)
def test_line_buffering_across_chunk_boundaries(chunks, expected):
    reader = make_reader(chunks, text=True, by_line=True)

    async def collect():
        return [line async for line in reader]

    assert run(collect()) == expected


def test_invalid_utf8_fails_at_its_own_line_after_the_lines_before_it():
    """Lines are decoded a chunk at a time now, but still one by one: the
    lines ahead of a bad byte arrive, as they did when each was yielded
    singly."""
    reader = make_reader(
        [b"good\nalso good\nbad \xff\nnever\n"], text=True, by_line=True
    )

    async def collect():
        seen = []
        with pytest.raises(UnicodeDecodeError):
            async for line in reader:
                seen.append(line)
        return seen

    assert run(collect()) == ["good\n", "also good\n"]


def test_blocking_line_iteration_crosses_to_the_loop_per_batch_not_per_line(
    monkeypatch,
):
    """Per line, a task and a loop turn used to cap iteration near 60k lines/s."""
    lines = [b"%06d\n" % i for i in range(20000)]
    reader = make_reader(
        [b"".join(lines[i : i + 5000]) for i in range(0, 20000, 5000)],
        text=True,
        by_line=True,
    )
    crossings = []
    real_run = _sync._loop_thread.run

    def counting_run(coro):
        crossings.append(1)
        return real_run(coro)

    monkeypatch.setattr(_sync._loop_thread, "run", counting_run)

    got = list(StreamReader._from_impl(reader))

    assert got == [line.decode() for line in lines]
    assert len(crossings) <= 20


def test_a_second_loop_after_the_end_yields_nothing():
    """Matches Modal 1.6: one position per reader, so nothing comes twice."""
    reader = make_reader([b"a", b"b"], text=True)

    async def collect_twice():
        first = [chunk async for chunk in reader]
        second = [chunk async for chunk in reader]
        return first, second

    first, second = run(collect_twice())
    assert first == ["a", "b"]
    assert second == []


def test_iteration_resumes_from_its_cursor_after_a_partial_pass():
    """The resume path: a replacement generator starts where the last stopped."""
    actor = FakeActor({STDOUT_FD: [b"one\n", b"two\n", b"three\n"]})
    reader = _StreamReader(actor, "exec-1", STDOUT_FD, by_line=True)

    async def stop_early():
        async for line in reader:
            return line

    async def the_rest():
        return [line async for line in reader]

    assert run(stop_early()) == "one\n"
    # The first loop's stream closed with its event loop; the next one opens
    # where it stopped rather than replaying.
    assert run(the_rest()) == ["two\n", "three\n"]
    assert actor.stream_calls == [0, len("one\n")]
    assert reader.truncated is False


@pytest.mark.parametrize("text,empty", [(True, ""), (False, b"")])
def test_a_second_read_returns_nothing_without_asking_the_actor(text, empty):
    """Modal 1.6: read() consumes, so after the end there is nothing left."""
    actor = FakeActor({STDOUT_FD: [b"hello ", b"world"]})
    reader = _StreamReader(actor, "exec-1", STDOUT_FD, text=text)

    async def read_twice():
        return await reader.read(), await reader.read()

    first, second = run(read_twice())
    assert first == ("hello world" if text else b"hello world")
    assert second == empty
    assert actor.stream_calls == [0]


def test_read_after_a_partial_loop_returns_the_rest():
    """Lines an interrupted `async for` had decoded but not handed out go to
    read(), and nothing it handed out comes again."""
    actor = FakeActor({STDOUT_FD: [b"a\nb\nc\n", b"d\n"]})
    reader = _StreamReader(actor, "exec-1", STDOUT_FD, by_line=True)

    async def first_then_read():
        async for line in reader:
            return line, await reader.read()

    assert run(first_then_read()) == ("a\n", "b\nc\nd\n")


@pytest.mark.parametrize(
    "chunks,first,rest",
    [
        # The loop stops with half a line held back for its newline.
        ([b"a\nb", b"c\nd"], "a\n", "bc\nd"),
        # ...and with half of a character held back too.
        ([b"a\ncaf\xc3", b"\xa9\nend"], "a\n", "café\nend"),
    ],
)
def test_read_after_a_loop_keeps_what_the_loop_held_back(chunks, first, rest):
    """read() decodes unsplit, for speed; the line a loop left unfinished must
    still lead what it returns, whole."""
    actor = FakeActor({STDOUT_FD: chunks})
    reader = _StreamReader(actor, "exec-1", STDOUT_FD, by_line=True)

    async def first_then_read():
        async for line in reader:
            return line, await reader.read()

    assert run(first_then_read()) == (first, rest)


def test_a_hole_is_counted_once():
    actor = FakeActor({STDOUT_FD: [b"lost bytes", b"kept bytes"]}, base_offset=10)
    reader = _StreamReader(actor, "exec-1", STDOUT_FD)

    async def read_twice():
        return await reader.read(), await reader.read()

    assert run(read_twice()) == ("kept bytes", "")
    assert reader.bytes_lost == 10


def test_reaching_the_end_acknowledges_the_stream():
    """So the actor can free what it kept for chunks in flight, and release a
    finished command read to the end."""
    actor = FakeActor({STDOUT_FD: [b"hello ", b"world"]})
    reader = _StreamReader(actor, "exec-1", STDOUT_FD)

    async def read_and_settle():
        text = await reader.read()
        await reader._ack_task
        return text

    assert run(read_and_settle()) == "hello world"
    assert actor.acks == [(STDOUT_FD, len(b"hello world"))]


def test_stopping_part_way_does_not_acknowledge():
    actor = FakeActor({STDOUT_FD: [b"one\n", b"two\n"]})
    reader = _StreamReader(actor, "exec-1", STDOUT_FD, by_line=True)

    async def first_line():
        async for line in reader:
            return line

    assert run(first_line()) == "one\n"
    assert actor.acks == []


def test_concurrent_reads_take_turns_and_deliver_each_byte_once():
    actor = FakeActor({STDOUT_FD: [b"a" * 10, 0.01, b"b" * 10, 0.01, b"c" * 10]})
    reader = _StreamReader(actor, "exec-1", STDOUT_FD)

    async def two_readers():
        return await asyncio.gather(reader.read(), reader.read())

    results = run(two_readers())
    assert sorted(results, key=len) == ["", "a" * 10 + "b" * 10 + "c" * 10]


def test_a_reader_overtaken_by_eviction_reports_the_gap(caplog):
    """Loss is surfaced, not spliced -- and never raised."""
    actor = FakeActor({STDOUT_FD: [b"lost bytes", b"kept bytes"]}, base_offset=10)
    reader = _StreamReader(actor, "exec-1", STDOUT_FD)

    with caplog.at_level("WARNING"):
        assert run(reader.read()) == "kept bytes"

    assert reader.truncated is True
    assert reader.bytes_lost == 10
    assert "truncated" in caplog.text


def test_a_gap_is_logged_once_but_counted_every_time(caplog):
    """A persistently behind reader must not spam a line per read."""
    reader = _StreamReader(FakeActor(), "exec-1", STDOUT_FD)

    with caplog.at_level("WARNING"):
        # Two gaps: 10 bytes, then 25 more.
        reader._note_gap(10)
        reader._note_gap(25)

    assert reader.bytes_lost == 35
    assert caplog.text.count("truncated") == 1


def test_a_reader_that_keeps_up_reports_no_truncation():
    reader = make_reader([b"all", b" here"])
    assert run(reader.read()) == "all here"
    assert reader.truncated is False
    assert reader.bytes_lost == 0


class ItemsActor:
    """Serves a stream as exactly these ``(offset, data)`` items, holes and all."""

    def __init__(self, items):
        self._items = items
        self.stream_output = _RemoteShim(self._stream_output)

    async def _stream_output(self, exec_id, fd, start_offset=0):
        for offset, data in self._items:
            if offset + len(data) > start_offset:
                yield _FakeRef((offset, data))


@pytest.mark.parametrize(
    "items,text,by_line,expected,lost",
    [
        # Retained output starts inside a character -- a window moving byte by
        # byte: the rest of that character is skipped.
        ([(1, b"\xa9" + "éé\n".encode())], True, False, "éé\n", 2),
        # A character cut off by a gap is dropped rather than decoded together
        # with whatever follows the gap.
        ([(0, b"ab\xc3"), (10, b"cd\n")], True, False, "abcd\n", 8),
        # A character's tail spread over several tiny chunks.
        ([(1, b"\xa9"), (2, b"\xa9"), (3, b"ok\n")], True, False, "ok\n", 3),
        # By line, an unfinished line ends at the gap instead of being spliced
        # onto the unrelated text after it -- with a character cut at both
        # edges, and with plain ASCII.
        (
            [(0, b"one\nab\xc3"), (20, b"\xa9cd\n")],
            True,
            True,
            ["one\n", "ab", "cd\n"],
            # The 13-byte gap, the cut-off lead byte, and its skipped tail.
            15,
        ),
        (
            [(0, b"one\ntw"), (20, b"x\nthree\n")],
            True,
            True,
            ["one\n", "tw", "x\n", "three\n"],
            14,
        ),
        # Bytes are bytes: nothing is skipped.
        ([(1, b"\xa9abc")], False, False, b"\xa9abc", 1),
    ],
)
def test_a_gap_that_cuts_a_character_does_not_fail_the_read(
    items, text, by_line, expected, lost
):
    """A strict UTF-8 decoder raised on the halves of a character a gap had
    cut, failing the whole read instead of returning what was still there."""
    reader = _StreamReader(
        ItemsActor(items), "exec-1", STDOUT_FD, text=text, by_line=by_line
    )

    async def collect():
        if by_line:
            return [line async for line in reader]
        return await reader.read()

    assert run(collect()) == expected
    assert reader.truncated is True
    assert reader.bytes_lost == lost


class _FsActor(FakeActor):
    """A FakeActor with the exec lifecycle ``copy_to_local`` drives."""

    async def _exec_start(self, command, **kwargs):
        return "exec-1"

    async def _exec_wait(self, exec_id, timeout, **kwargs):
        return 0

    async def _exec_release(self, exec_id):
        return None


def _filesystem(actor):
    from ray.experimental.sandbox.modal.sandbox_fs import _SandboxFilesystem

    return _SandboxFilesystem(
        types.SimpleNamespace(_actor=actor, _ensure_attached=lambda: None)
    )


def test_copy_to_local_writes_the_streamed_file(tmp_path):
    actor = _FsActor({STDOUT_FD: [b"file ", b"contents"], STDERR_FD: []})
    destination = tmp_path / "out.bin"

    run(_filesystem(actor).copy_to_local("/remote/file", destination))

    assert destination.read_bytes() == b"file contents"


def test_copy_to_local_refuses_to_write_a_file_with_a_hole(tmp_path):
    """A gap here is corruption, not a cosmetic loss, so it must not pass."""
    actor = _FsActor(
        {STDOUT_FD: [b"lost bytes", b"kept bytes"], STDERR_FD: []}, base_offset=10
    )
    destination = tmp_path / "out.bin"

    with pytest.raises(SandboxFilesystemError, match="lost 10 bytes"):
        run(_filesystem(actor).copy_to_local("/remote/file", destination))

    # Atomic write: the caller is not left with a truncated destination.
    assert not destination.exists()


# -- the public blocking surface -------------------------------------------
#
# These drive `StreamReader`, not `_StreamReader`. Everything above tests the
# implementation directly, which is exactly why a documented attribute could
# ship unreachable: synchronize_api forwards only class-level entries, so a
# plain instance attribute is invisible on the wrapper.


def public_reader(chunks, base_offset=0, **kwargs):
    impl = _StreamReader(
        FakeActor({STDOUT_FD: chunks}, base_offset=base_offset),
        "exec-1",
        STDOUT_FD,
        **kwargs,
    )
    return StreamReader._from_impl(impl)


@pytest.mark.parametrize("attribute", ["truncated", "bytes_lost", "file_descriptor"])
def test_documented_attributes_survive_the_sync_wrapper(attribute):
    """__init__.py points callers at these, so they must exist on the wrapper."""
    assert hasattr(public_reader([b"x"]), attribute)


def test_truncation_is_reported_through_the_public_reader():
    reader = public_reader([b"lost", b"kept"], base_offset=4)

    assert reader.read() == "kept"
    assert reader.truncated is True
    assert reader.bytes_lost == 4


def test_the_public_reader_is_its_own_iterator():
    """Modal keeps __anext__ on StreamReader, so next() must work here too."""
    reader = public_reader([b"a\nb\n"], by_line=True)

    assert next(reader) == "a\n"
    assert next(reader) == "b\n"
    with pytest.raises(StopIteration):
        next(reader)


@pytest.mark.parametrize(
    "chunks",
    [
        # Everything decoded at once: the loop must leave what it did not hand
        # out with the reader, not in a look-ahead of its own.
        [b"start\nmiddle\ndone\n"],
        # A pause after the first line: a fetch can be left in flight.
        [b"start\n", 0.2, b"middle\ndone\n"],
    ],
)
def test_read_after_breaking_out_of_a_loop_returns_the_rest(chunks):
    """Measured on Modal 1.6.1: `break` after 10 lines, then read(), returned
    exactly from line 11."""
    reader = public_reader(chunks, by_line=True)
    for line in reader:
        break
    assert line == "start\n"
    assert reader.read() == "middle\ndone\n"
    assert reader.read() == ""


@pytest.mark.parametrize(
    "chunks,rest",
    [
        # Everything ready at once: the first loop pulls it all ahead.
        ([b"a\nb\nc\n"], ["b\n", "c\n"]),
        # A pause after the first line: the first loop leaves a fetch in
        # flight inside the generator.
        ([b"a\n", 0.2, b"b\n"], ["b\n"]),
    ],
)
def test_a_second_loop_resumes_where_a_broken_one_stopped(chunks, rest):
    """What one loop read ahead belongs to the next, not to the one that
    broke off: nothing is dropped, and no second fetch collides with one
    still running ("anext(): asynchronous generator is already running")."""
    reader = public_reader(chunks, by_line=True)
    for line in reader:
        break
    assert line == "a\n"
    assert list(reader) == rest


def test_the_public_reader_closes_under_both_modal_spellings():
    """Modal's synchronizer turns one `aclose` into a pair on the blocking class.

    `close()` is the blocking half and `aclose()` the coroutine, rather than the
    `aclose()`/`aclose.aio()` shape every other method gets. Ported code reaches
    for both, and under the generic rule `close()` was an AttributeError and
    `await reader.aclose()` awaited the None the blocking call had returned.
    """
    reader = public_reader([b"x"])
    assert reader.close() is None

    async def close_the_async_way():
        await public_reader([b"x"]).aclose()

    asyncio.run(close_the_async_way())


def test_the_public_reader_accepts_modals_generic_subscript():
    """`StreamReader[str]` appears in ported annotations and typing.cast calls."""
    assert StreamReader[str] is StreamReader
    assert StreamReader[bytes] is StreamReader


def test_file_descriptor_is_reported():
    assert make_reader([], fd=STDOUT_FD).file_descriptor == STDOUT_FD
    assert make_reader([], fd=STDERR_FD).file_descriptor == STDERR_FD


# -- stream types ----------------------------------------------------------


@pytest.mark.parametrize("method", ["read", "aiter", "anext"])
def test_devnull_streams_raise_value_error(method):
    reader = make_reader([b"x"], stream_type=StreamType.DEVNULL)
    with pytest.raises(ValueError, match="DEVNULL"):
        if method == "read":
            run(reader.read())
        elif method == "aiter":
            reader.__aiter__()
        else:
            run(reader.__anext__())


@pytest.mark.parametrize("method", ["read", "__aiter__", "__anext__"])
def test_devnull_message_names_the_method_the_caller_used(method):
    """__anext__ used to report __aiter__, which it delegates through."""
    reader = make_reader([b"x"], stream_type=StreamType.DEVNULL)
    with pytest.raises(ValueError) as excinfo:
        if method == "read":
            run(reader.read())
        elif method == "__aiter__":
            reader.__aiter__()
        else:
            run(reader.__anext__())
    assert str(excinfo.value).startswith(f"{method} is not supported")


def test_closing_a_devnull_stream_is_a_silent_no_op():
    """Modal's public aclose() only closes a generator that exists, and a
    DEVNULL stream never has one -- so generic cleanup code that closes both
    streams must not trip over the one that was discarded."""
    reader = make_reader([b"x"], stream_type=StreamType.DEVNULL)
    run(reader.aclose())
    run(reader.aclose())
    public = StreamReader._from_impl(reader)
    assert public.close() is None


@pytest.mark.parametrize("method", ["read", "aiter"])
def test_stdout_streams_raise_invalid_error(method):
    """DEVNULL and STDOUT deliberately raise different exception types."""

    async def attempt():
        reader = make_reader([b"x"], stream_type=StreamType.STDOUT)
        with pytest.raises(InvalidError, match="PIPE"):
            if method == "read":
                await reader.read()
            else:
                reader.__aiter__()
        await reader.aclose()

    run(attempt())


def test_stdout_stream_prints_to_local_stdout(capsys):
    async def attempt():
        reader = make_reader([b"printed\n"], stream_type=StreamType.STDOUT)
        # Let the background pump run to completion.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await reader.aclose()

    run(attempt())
    assert "printed" in capsys.readouterr().out


def test_stdout_stream_prints_a_character_cut_off_at_the_end(capsys):
    """The decoder held the cut-off bytes and nothing flushed it at end of
    stream: the character vanished instead of printing as U+FFFD."""

    async def attempt():
        reader = make_reader([b"done \xe2\x82"], stream_type=StreamType.STDOUT)
        await reader._print_task

    run(attempt())
    assert capsys.readouterr().out == "done �"


# -- output buffer: cursor addressing --------------------------------------


def drain(buffer: _OutputBuffer, cursor: int = 0) -> bytes:
    """Read a buffer to end of stream from `cursor`, ignoring gaps."""

    async def collect():
        out = bytearray()
        position = cursor
        while True:
            result = await buffer.read_from(position)
            if result is None:
                return bytes(out)
            offset, data = result
            out += data
            position = offset + len(data)

    return run(collect())


def test_read_from_coalesces_queued_chunks():
    """A burst of pipe reads becomes one yield, not one yield per read."""
    buffer = _OutputBuffer()
    buffer.feed(b"one ")
    buffer.feed(b"two ")
    buffer.feed(b"three")
    assert run(buffer.read_from(0)) == (0, b"one two three")


def test_read_from_returns_none_at_end_of_stream():
    buffer = _OutputBuffer()
    buffer.feed_eof()
    assert run(buffer.read_from(0)) is None


def test_read_from_waits_for_the_first_chunk():
    """With nothing queued it blocks rather than returning empty."""

    async def attempt():
        buffer = _OutputBuffer()

        async def feed_later():
            await asyncio.sleep(0)
            buffer.feed(b"late")

        _, result = await asyncio.gather(feed_later(), buffer.read_from(0))
        return result

    assert run(attempt()) == (0, b"late")


def test_read_from_caps_each_yield():
    """Large enough to amortize a yield's fixed cost, bounded all the same."""
    buffer = _OutputBuffer()
    buffer.feed(b"a" * _MAX_YIELD_BYTES)
    buffer.feed(b"b" * _MAX_YIELD_BYTES)
    offset, first = run(buffer.read_from(0))
    assert offset == 0
    assert first == b"a" * _MAX_YIELD_BYTES


def test_a_reader_behind_gets_large_yields_and_one_keeping_up_small_ones():
    """Bulk output moves in ~1 MiB yields, measured five times faster than
    96 KiB ones; a trickle is yielded as it comes and stays inline."""
    buffer = _OutputBuffer()
    for _ in range(48):
        buffer.feed(b"z" * 65536)  # 3 MiB, as the pump reads a busy pipe
    _, behind = run(buffer.read_from(0))
    assert len(behind) == 1024 * 1024

    buffer.feed(b"one line\n")
    _, keeping_up = run(buffer.read_from(buffer.written - len(b"one line\n")))
    assert keeping_up == b"one line\n"


def test_read_from_splits_an_oversized_chunk_without_losing_bytes():
    """The overflow stays readable at the next cursor, in order."""
    buffer = _OutputBuffer()
    buffer.feed(b"a" * (_MAX_YIELD_BYTES + 10))
    buffer.feed_eof()
    assert drain(buffer) == b"a" * (_MAX_YIELD_BYTES + 10)


@pytest.mark.parametrize("cursor", [0, 4, 13])
def test_a_reader_resumes_at_its_cursor(cursor):
    buffer = _OutputBuffer()
    buffer.feed(b"shared output")
    buffer.feed_eof()
    assert drain(buffer, cursor) == b"shared output"[cursor:]


def test_a_reader_that_keeps_up_never_loses_output():
    """However much passes through a small cap, a reader that acknowledges as
    it goes never sees a gap, and the buffer holds next to nothing.

    Driven inside one event loop: the buffer's asyncio.Event binds to the first
    loop it is awaited on, and in production that is the actor's single loop.
    """

    async def attempt():
        buffer = _OutputBuffer(cap=1024)
        cursor = 0
        for _ in range(200):
            buffer.feed(b"x" * 512)
            offset, data = await buffer.read_from(cursor)
            assert offset == cursor, "a reader at the tail must not be overtaken"
            cursor = offset + len(data)
            buffer.ack(cursor)
        return cursor, buffer.lost, buffer.memory_bytes

    cursor, lost, held = run(attempt())
    assert cursor == 200 * 512
    assert lost == 0
    assert held == 0


def test_a_cursor_behind_the_kept_output_reports_the_gap():
    """Overrun resumes at the oldest byte still held and says so."""
    buffer = _OutputBuffer(cap=1024)
    for _ in range(10):
        buffer.feed(b"x" * 512)

    offset, data = run(buffer.read_from(0))

    # Nothing is spliced: the jump is visible in the offset itself.
    assert offset == buffer.lost > 0
    assert offset + len(data) == 10 * 512


# -- the exec deadline -----------------------------------------------------
#
# Measured on Modal (both backends): an exec's output is readable only until
# its timeout runs out. A later read returns nothing at once, even for a
# command that finished in time; a read crossing the deadline stops there.


@pytest.mark.parametrize("text,empty", [(True, ""), (False, b"")])
def test_reading_after_the_deadline_returns_nothing_without_asking(text, empty):
    actor = FakeActor({STDOUT_FD: [b"finished in time\n"]})
    reader = _StreamReader(
        actor, "exec-1", STDOUT_FD, text=text, deadline=time.monotonic() - 1
    )
    assert run(reader.read()) == empty
    assert actor.stream_calls == [], "answered without the actor, as Modal does"


def test_iterating_after_the_deadline_yields_nothing():
    actor = FakeActor({STDOUT_FD: [b"l1\n", b"l2\n"]})
    reader = _StreamReader(
        actor, "exec-1", STDOUT_FD, by_line=True, deadline=time.monotonic() - 1
    )

    async def collect():
        return [line async for line in reader]

    assert run(collect()) == []


def test_a_read_in_progress_ends_at_the_deadline():
    """Silently, as on Modal: no exception, and nothing marked truncated."""
    actor = FakeActor({STDOUT_FD: [b"before ", 0.5, b"after"]})

    async def attempt():
        reader = _StreamReader(
            actor, "exec-1", STDOUT_FD, deadline=time.monotonic() + 0.2
        )
        return await reader.read(), reader.truncated

    assert run(attempt()) == ("before ", False)


def test_a_stream_without_a_deadline_is_read_whenever():
    actor = FakeActor({STDOUT_FD: [b"later\n"]})
    reader = _StreamReader(actor, "exec-1", STDOUT_FD, deadline=None)
    assert run(reader.read()) == "later\n"


# -- unread output: bounded in memory --------------------------------------
#
# The process is never made to wait for a reader, as on Modal. What the reader
# has acknowledged is freed; what nobody reads is kept up to the stream's cap,
# and past it the oldest goes -- reported, never spliced.

_TAIL = 4096
# Patterned, so a byte out of place or a spliced gap shows as a mismatch.
_PAYLOAD = bytes(i % 251 for i in range(100_000))


async def pump_into(buffer, payload, piece=1000):
    """Feed ``payload`` the way _pump does: a chunk, then pace."""
    for i in range(0, len(payload), piece):
        buffer.feed(payload[i : i + piece])
        await buffer.pace()
    buffer.feed_eof()


async def read_all(buffer, cursor=0):
    """Read to end of stream, acknowledging as it goes, as the actor's
    stream_output does on its reader's behalf. Returns where the data began,
    and the data."""
    out = bytearray()
    first = None
    while True:
        result = await buffer.read_from(cursor)
        if result is None:
            return first, bytes(out)
        offset, data = result
        if first is None:
            first = offset
        out += data
        cursor = offset + len(data)
        buffer.ack(cursor)


def test_past_its_cap_a_stream_drops_its_oldest_unread_output_and_says_why():
    reasons = []
    buffer = _OutputBuffer(_TAIL, on_drop=reasons.append)
    for i in range(0, len(_PAYLOAD), 1000):
        buffer.feed(_PAYLOAD[i : i + 1000])
    buffer.feed_eof()

    first, data = run(read_all(buffer))

    assert len(data) <= _TAIL
    assert first + len(data) == len(_PAYLOAD), "the newest output, to the end"
    assert data == _PAYLOAD[first:]
    assert buffer.lost == first
    assert reasons and "cap" in reasons[0]


def test_acknowledged_output_is_freed():
    buffer = _OutputBuffer()
    buffer.feed(b"x" * 100_000)
    buffer.feed(b"y" * 100_000)
    held_before = buffer.memory_bytes

    buffer.ack(100_000)

    assert held_before == 200_000
    assert buffer.memory_bytes == 100_000, "the first chunk is gone"
    assert buffer.lost == 0, "freeing what was read is not loss"
    assert run(buffer.read_from(0)) == (100_000, b"y" * 100_000)


def test_acknowledging_never_moves_backwards_or_past_the_end():
    buffer = _OutputBuffer()
    buffer.feed(b"abc")
    buffer.ack(10)
    buffer.ack(1)
    buffer.feed(b"def")
    assert run(buffer.read_from(3)) == (3, b"def")


def test_a_stream_is_consumed_once_ended_and_acknowledged_to_its_end():
    buffer = _OutputBuffer()
    buffer.feed(b"abc")
    buffer.ack(3)
    assert not buffer.consumed, "it has not ended"
    buffer.feed_eof()
    assert buffer.consumed


def test_a_paced_stream_holds_its_producer_a_buffer_ahead_of_the_reader():
    """The filesystem's transfers: whole, in bounded memory.

    A copy used to run its command flat out, and a reader more than a buffer
    behind lost bytes and the copy failed.
    """

    async def attempt():
        buffer = _OutputBuffer(_TAIL, paced=True)

        async def produce():
            await pump_into(buffer, _PAYLOAD)

        producer = asyncio.ensure_future(produce())
        await asyncio.sleep(0.05)
        held, waiting = buffer.written, not producer.done()
        first, data = await read_all(buffer)
        await producer
        return held, waiting, first, data, buffer

    held, waiting, first, data, buffer = run(attempt())
    assert waiting, "nobody was reading, so the producer should be waiting"
    assert held <= _TAIL + 1000
    assert (first, data) == (0, _PAYLOAD)
    assert buffer.lost == 0


def test_a_paced_stream_never_drops():
    buffer = _OutputBuffer(_TAIL, paced=True)
    for i in range(0, len(_PAYLOAD), 1000):
        buffer.feed(_PAYLOAD[i : i + 1000])
    assert buffer.drop_oldest(len(_PAYLOAD), "test") == 0
    assert buffer.lost == 0
    assert buffer.memory_bytes == len(_PAYLOAD)


def test_closing_a_paced_stream_releases_a_waiting_producer():
    """exec_release on a transfer the caller abandoned."""

    async def attempt():
        buffer = _OutputBuffer(_TAIL, paced=True)
        buffer.feed(b"x" * (2 * _TAIL))
        waiter = asyncio.ensure_future(buffer.pace())
        await asyncio.sleep(0.02)
        assert not waiter.done()
        buffer.close()
        await asyncio.wait_for(waiter, 1)

    run(attempt())


@pytest.mark.parametrize("chunks", [0, 1])
def test_a_stream_with_nothing_droppable_gives_up_nothing(chunks):
    """Relief walks every stream, empty ones included; one with no chunks to
    spare raised from inside feed() and silently stopped another's pump."""
    buffer = _OutputBuffer()
    for _ in range(chunks):
        buffer.feed(b"x" * 100_000)
    assert buffer.drop_oldest(10, "test") == 0
    assert buffer.lost == 0


def test_buffers_account_what_they_hold_to_the_sandbox_budget():
    budget = _MemoryBudget(1 << 30)
    first = _OutputBuffer(budget=budget)
    second = _OutputBuffer(budget=budget)
    first.feed(b"x" * 100_000)
    second.feed(b"y" * 50_000)
    assert budget.used == 150_000

    first.ack(100_000)
    assert budget.used == 50_000
    second.close()
    assert budget.used == 0


def test_past_the_sandbox_cap_the_excess_is_handed_to_relief():
    asked = []
    budget = _MemoryBudget(150_000, relieve=asked.append)
    buffer = _OutputBuffer(budget=budget)
    buffer.feed(b"x" * 100_000)
    assert asked == []
    buffer.feed(b"y" * 100_000)
    assert asked == [50_000]


# -- a line-at-a-time producer ---------------------------------------------
#
# The common slow producer: every pipe read is one short line. Measured before
# gathering, 8 MiB of such a stream was ~100k tiny chunks, and each read
# scanned them from the head (~7 ms of the actor's loop).

_LINE = b"y" * 79 + b"\n"
_MIB = 1024 * 1024


def test_small_reads_are_gathered_rather_than_kept_one_per_chunk():
    buffer = _OutputBuffer()
    for _ in range(8 * _MIB // len(_LINE)):
        buffer.feed(_LINE)

    # A few hundred chunks for 8 MiB, not one per line.
    assert len(buffer._chunks) <= 8 * _MIB // _actor._CHUNK_GATHER_BYTES + 2

    tail = buffer.written - 3 * len(_LINE)
    offset, data = run(buffer.read_from(tail))
    assert (offset, data) == (tail, _LINE * 3)
    assert type(data) is bytes, "a gathered bytearray must not leak out"


def test_reading_from_a_gone_sandbox_reports_not_found():
    """Modal answers NotFoundError; a raw RayActorError used to escape."""
    reader = _StreamReader(GoneActor(), "exec-1", STDOUT_FD)
    with pytest.raises(NotFoundError, match="unavailable"):
        run(reader.read())


def test_draining_stdin_to_a_gone_sandbox_reports_not_found():
    writer = _StreamWriter(GoneActor(), "exec-1")
    writer.write(b"x")
    with pytest.raises(NotFoundError, match="unavailable"):
        run(writer.drain())


# -- writing ---------------------------------------------------------------


def test_write_buffers_until_drain():
    actor = FakeActor()
    writer = _StreamWriter(actor, "exec-1")
    writer.write(b"hello ")
    writer.write("world")
    assert actor.stdin_writes == []

    run(writer.drain())
    assert actor.stdin_writes == [b"hello world"]
    assert not actor.stdin_closed


def test_write_eof_closes_stdin_on_drain():
    actor = FakeActor()
    writer = _StreamWriter(actor, "exec-1")
    writer.write(b"data")
    writer.write_eof()
    run(writer.drain())
    assert actor.stdin_writes == [b"data"]
    assert actor.stdin_closed


def test_write_after_eof_is_rejected():
    writer = _StreamWriter(FakeActor(), "exec-1")
    writer.write_eof()
    with pytest.raises(ValueError, match="Stdin is closed"):
        writer.write(b"more")


@pytest.mark.parametrize("data", [1, 3.5, None, ["a"]])
def test_write_rejects_non_bytes_like(data):
    writer = _StreamWriter(FakeActor(), "exec-1")
    with pytest.raises(TypeError, match="bytes-like"):
        writer.write(data)


@pytest.mark.parametrize("data", [b"bytes", bytearray(b"bytearray"), "text"])
def test_write_accepts_bytes_like_and_str(data):
    actor = FakeActor()
    writer = _StreamWriter(actor, "exec-1")
    writer.write(data)
    run(writer.drain())
    assert actor.stdin_writes[0] == (
        data.encode("utf-8") if isinstance(data, str) else bytes(data)
    )


def test_write_beyond_buffer_limit_raises():
    writer = _StreamWriter(FakeActor(), "exec-1")
    with pytest.raises(BufferError, match="Call drain"):
        writer.write(b"x" * (MAX_BUFFER_SIZE + 1))


class OffsetStdinActor:
    """Applies stdin writes the way the actor does: at their offset, once each.

    ``delay`` holds each write for a round trip, so a test can act while one is
    in flight. ``failures`` makes that many writes raise, before or after their
    bytes land -- a reply lost on the way back is the case a retry must not
    duplicate.
    """

    def __init__(self, delay=0.0, failures=0, fail_after_write=False):
        self.received = bytearray()
        self.closed_at: Optional[int] = None
        self._delay = delay
        self._failures = failures
        self._fail_after_write = fail_after_write
        self.write_stdin = _RemoteShim(self._write)
        self.close_stdin = _RemoteShim(self._close)

    async def _write(self, exec_id, data, offset):
        await asyncio.sleep(self._delay)
        failing = self._failures > 0
        self._failures -= failing
        if failing and not self._fail_after_write:
            raise RuntimeError("transient")
        assert offset <= len(self.received), "the writer skipped bytes"
        self.received += data[len(self.received) - offset :]
        if failing:
            raise RuntimeError("transient")

    async def _close(self, exec_id, offset):
        self.closed_at = offset


def test_a_write_made_while_a_drain_is_in_flight_is_delivered():
    """It used to be cleared along with what that drain had sent, and lost."""
    actor = OffsetStdinActor(delay=0.05)
    writer = _StreamWriter(actor, "exec-1")

    async def scenario():
        writer.write(b"first\n")
        in_flight = asyncio.ensure_future(writer.drain())
        await asyncio.sleep(0.01)
        writer.write(b"second\n")
        await in_flight
        await writer.drain()

    run(scenario())
    assert bytes(actor.received) == b"first\nsecond\n"


def test_concurrent_drains_deliver_every_byte_exactly_once():
    actor = OffsetStdinActor(delay=0.02)
    writer = _StreamWriter(actor, "exec-1")

    async def scenario():
        writer.write(b"abc")
        first = asyncio.ensure_future(writer.drain())
        await asyncio.sleep(0)
        writer.write(b"def")
        second = asyncio.ensure_future(writer.drain())
        await asyncio.gather(first, second)
        writer.write_eof()
        await writer.drain()

    run(scenario())
    assert bytes(actor.received) == b"abcdef"
    assert actor.closed_at == 6


@pytest.mark.parametrize("fail_after_write", [False, True])
def test_a_failed_drain_is_retried_without_duplicating_bytes(fail_after_write):
    actor = OffsetStdinActor(failures=1, fail_after_write=fail_after_write)
    writer = _StreamWriter(actor, "exec-1")
    writer.write(b"payload")

    with pytest.raises(RuntimeError, match="transient"):
        run(writer.drain())
    run(writer.drain())

    assert bytes(actor.received) == b"payload"


# -- coalescing behaviour and cost -----------------------------------------


async def _drive(chunk: bytes, count: int):
    """Run a hot producer against a consumer, counting the yields it takes.

    The producer hands control back between writes but never sleeps, which is
    the regime that matters: a consumer that keeps up leaves one chunk queued,
    so without the hold-back every write becomes its own Ray object.
    """
    buffer = _OutputBuffer()
    yields = 0

    async def produce():
        for _ in range(count):
            buffer.feed(chunk)
            await asyncio.sleep(0)
        buffer.feed_eof()

    async def consume():
        nonlocal yields
        total = 0
        cursor = 0
        while True:
            result = await buffer.read_from(cursor)
            if result is None:
                return total
            offset, data = result
            cursor = offset + len(data)
            yields += 1
            total += len(data)

    _, total = await asyncio.gather(produce(), consume())
    return yields, total


def test_coalescing_keeps_objects_per_megabyte_bounded():
    """A chatty producer must not cost one object per write."""
    chunk = b"x" * 64
    count = (1024 * 1024) // len(chunk)

    yields, total = run(_drive(chunk, count))

    assert total == count * len(chunk)
    # Un-coalesced this is `count` objects for the megabyte; the hold-back
    # should land near one per _COALESCE_TARGET_BYTES, so ~64.
    assert yields < 200, f"{yields} objects for 1 MiB -- coalescing regressed"


def test_a_slow_producer_is_not_held_back():
    """The window stays shut when the producer, not the consumer, is slow.

    This is what keeps the hold-back adaptive: waiting here would add latency
    to output that was never going to batch with anything.
    """

    async def attempt():
        buffer = _OutputBuffer()

        async def feed_apart():
            await asyncio.sleep(_COALESCE_IDLE_GAP * 5)
            buffer.feed(b"first")
            await asyncio.sleep(_COALESCE_IDLE_GAP * 5)
            buffer.feed(b"second")

        task = asyncio.ensure_future(feed_apart())
        first = await buffer.read_from(0)
        await task
        return first

    assert run(attempt()) == (0, b"first")


def test_coalescing_stops_at_the_target_without_waiting_for_more():
    """Enough queued to fill an object is reason to send it, not to wait."""
    buffer = _OutputBuffer()
    buffer.feed(b"x" * _COALESCE_TARGET_BYTES)

    started = time.perf_counter()
    _, got = run(buffer.read_from(0))

    assert len(got) == _COALESCE_TARGET_BYTES
    assert time.perf_counter() - started < _COALESCE_MAX_WINDOW


def _time_split(payload: bytes) -> float:
    best = float("inf")
    for _ in range(5):
        started = time.perf_counter()
        _LineSplitter().feed(payload)
        best = min(best, time.perf_counter() - started)
    return best


def test_line_splitting_scales_linearly_with_chunk_size():
    """Guards the index scan against a return to re-slicing per line.

    Dropping the consumed head with ``buffer = buffer[n:]`` copies the rest of
    the chunk on every newline, which is quadratic in the lines it holds --
    invisible while chunks were one line each, and milliseconds per chunk now
    that the actor coalesces bursts into full-sized yields.
    """
    line = b"x" * 49 + b"\n"
    ratio = _time_split(line * 4000) / _time_split(line * 500)

    # Linear predicts ~8 for 8x the lines, quadratic ~64. The bound sits well
    # clear of both so CI noise cannot tip it either way.
    assert ratio < 20, f"line splitting scaled {ratio:.1f}x for 8x the lines"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
