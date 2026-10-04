"""Unit tests for the streaming filesystem transfers.

These drive ``_SandboxFilesystem`` against a fake actor that models the one
behaviour these paths get wrong most easily: ``exec_release`` really does drop
the session, so anything read from it afterwards fails. They need neither runsc
nor a Ray cluster.
"""

import asyncio
import functools
import os
import re
import sys
import types
from typing import Dict, List, Optional

import pytest

from ray.experimental.sandbox.modal import _fs_commands as _fs, sandbox_fs as fs_mod
from ray.experimental.sandbox.modal._actor import STDERR_FD
from ray.experimental.sandbox.modal.exception import (
    ClientClosed,
    InvalidError,
    NotFoundError,
    SandboxFilesystemError,
    SandboxFilesystemFileTooLargeError,
    SandboxFilesystemPermissionError,
)
from ray.experimental.sandbox.modal.sandbox_fs import _SandboxFilesystem


class _Ref:
    """Stands in for an ObjectRef: a value that has to be awaited."""

    def __init__(self, coro, on_await=None):
        self._coro = coro
        self._on_await = on_await

    def __await__(self):
        async def _get():
            try:
                return await self._coro
            finally:
                if self._on_await is not None:
                    self._on_await()

        return _get().__await__()


class _Shim:
    def __init__(self, actor, fn):
        self._actor = actor
        self._fn = fn

    def options(self, **kwargs):
        return self

    def remote(self, *args, **kwargs):
        return self._fn(*args, **kwargs)


class UploadActor:
    """The exec lifecycle an upload drives, with a released session really gone.

    ``exec_release`` dropping the session is the point: a drain issued after it
    must fail, which is what makes the ordering inside ``_upload`` testable at
    all.
    """

    def __init__(self, returncode: int = 0, stderr: bytes = b""):
        self._returncode = returncode
        self._streams: Dict[int, bytes] = {STDERR_FD: stderr}
        self._released = False
        self.writes: List[bytes] = []
        self.offsets: List[Optional[int]] = []
        self.close_offset: Optional[int] = None
        self.stdin_closed = False
        self.command: Optional[List[str]] = None
        self.start_kwargs: Optional[dict] = None
        self.wait_kwargs: Optional[dict] = None
        self.pending = 0
        self.max_pending = 0

    def __getattr__(self, name):
        impl = getattr(type(self), f"_{name}", None)
        if impl is None:
            raise AttributeError(name)
        return _Shim(self, functools.partial(impl, self))

    def _require_live(self):
        if self._released:
            raise InvalidError("Unknown exec session: exec-1")

    async def _exec_start(self, command, **kwargs):
        self.command = command
        self.start_kwargs = kwargs
        return "exec-1"

    def _write_stdin(self, exec_id, data, offset=None):
        # Counted at call time rather than inside the coroutine: what is under
        # test is how many the caller lets pile up before awaiting any.
        self.pending += 1
        self.max_pending = max(self.max_pending, self.pending)

        async def _do():
            self._require_live()
            self.writes.append(data)
            self.offsets.append(offset)

        return _Ref(_do(), on_await=self._settled)

    def _settled(self):
        self.pending -= 1

    async def _close_stdin(self, exec_id, offset=None):
        self._require_live()
        self.stdin_closed = True
        self.close_offset = offset

    async def _exec_wait(self, exec_id, timeout, **kwargs):
        self._require_live()
        self.wait_kwargs = kwargs
        return self._returncode

    async def _exec_release(self, exec_id):
        self._released = True

    async def _stream_output(self, exec_id, fd, start_offset=0):
        self._require_live()
        data = self._streams.get(fd, b"")
        if data:
            yield _Ref(_value((0, data)))


async def _value(value):
    return value


def filesystem(actor, attached: bool = True) -> _SandboxFilesystem:
    def ensure_attached():
        if not attached:
            raise ClientClosed("Unable to perform operation on a detached sandbox")

    return _SandboxFilesystem(
        types.SimpleNamespace(_actor=actor, _ensure_attached=ensure_attached)
    )


def run(coro):
    return asyncio.run(coro)


# -- the failure path ------------------------------------------------------


def test_a_failing_upload_reports_the_filesystem_error(tmp_path):
    """The drain that builds the message has to happen before the release.

    Releasing first dropped the session, so the drain raised a lookup error and
    every failing copy_from_local reported that instead of what went wrong --
    and from outside the translation block, so nothing mapped it either.
    """
    source = tmp_path / "payload.bin"
    source.write_bytes(b"content")
    actor = UploadActor(returncode=_fs.EXIT_PERMISSION_DENIED)

    with pytest.raises(SandboxFilesystemPermissionError, match="permission denied"):
        run(filesystem(actor).copy_from_local(source, "/locked/file"))


def test_a_failing_write_bytes_reports_the_filesystem_error():
    actor = UploadActor(returncode=_fs.EXIT_IS_A_DIRECTORY)

    with pytest.raises(Exception) as excinfo:
        run(filesystem(actor).write_bytes(b"content", "/some/dir"))

    assert "expected a file path" in str(excinfo.value)


def test_the_session_is_released_even_when_the_upload_fails(tmp_path):
    source = tmp_path / "payload.bin"
    source.write_bytes(b"content")
    actor = UploadActor(returncode=_fs.EXIT_PERMISSION_DENIED)

    with pytest.raises(SandboxFilesystemPermissionError):
        run(filesystem(actor).copy_from_local(source, "/locked/file"))

    assert actor._released, "a failed upload must not strand its exec session"


# -- the success path ------------------------------------------------------


def test_an_upload_sends_the_whole_file_and_closes_stdin(tmp_path):
    source = tmp_path / "payload.bin"
    source.write_bytes(b"abcdefghij")
    actor = UploadActor()

    run(filesystem(actor).copy_from_local(source, "/dest/file"))

    assert b"".join(actor.writes) == b"abcdefghij"
    assert actor.stdin_closed
    assert actor._released


def test_write_bytes_sends_the_payload():
    actor = UploadActor()
    run(filesystem(actor).write_bytes(b"hello", "/dest/file"))
    assert b"".join(actor.writes) == b"hello"


def test_an_empty_write_still_creates_the_file():
    actor = UploadActor()
    run(filesystem(actor).write_bytes(b"", "/dest/file"))
    assert actor.writes == []
    assert actor.stdin_closed, "the command needs its EOF to create the file"


# -- the in-flight window --------------------------------------------------


def test_an_upload_bounds_how_many_writes_are_outstanding(tmp_path, monkeypatch):
    """Unbounded, a large file queued one actor task per chunk.

    The window still has to be wide enough to hide the round trip, so this
    checks it is used, not that it is 1.
    """
    monkeypatch.setattr(fs_mod, "_COPY_CHUNK_SIZE", 4)
    monkeypatch.setattr(fs_mod, "_COPY_WINDOW", 3)
    source = tmp_path / "payload.bin"
    source.write_bytes(b"x" * 400)
    actor = UploadActor()

    run(filesystem(actor).copy_from_local(source, "/dest/file"))

    assert len(actor.writes) == 100, "the file is chunked, not sent whole"
    assert actor.max_pending <= 3
    assert b"".join(actor.writes) == b"x" * 400


@pytest.mark.parametrize("upload", ["copy_from_local", "write_bytes"])
def test_every_chunk_names_its_offset_and_the_close_the_total(
    tmp_path, monkeypatch, upload
):
    """Ray runs an async actor's calls in no guaranteed order, so arrival order
    proves nothing -- this fake runs them in order, which is how scrambled
    uploads once passed here. What orders the bytes is each write's offset,
    which the actor waits on (see test_modal_exec_retention.py)."""
    monkeypatch.setattr(fs_mod, "_COPY_CHUNK_SIZE", 4)
    payload = bytes(range(62))
    actor = UploadActor()

    if upload == "copy_from_local":
        source = tmp_path / "payload.bin"
        source.write_bytes(payload)
        run(filesystem(actor).copy_from_local(source, "/dest/file"))
    else:
        run(filesystem(actor).write_bytes(payload, "/dest/file"))

    assert actor.offsets == list(range(0, 62, 4))
    assert all(
        payload[offset : offset + len(chunk)] == chunk
        for offset, chunk in zip(actor.offsets, actor.writes)
    )
    assert actor.close_offset == 62


def test_an_aborted_upload_settles_its_writes_before_releasing(tmp_path, monkeypatch):
    """A local read failing part-way through a copy.

    The writes still in flight used to be abandoned; they then ran against the
    released session, and each logged an unhandled "Unknown exec session".
    """
    monkeypatch.setattr(fs_mod, "_COPY_CHUNK_SIZE", 4)
    pending_at_release = []
    events = []

    class RecordingActor(UploadActor):
        async def _exec_release(self, exec_id):
            pending_at_release.append(self.pending)
            events.append("release")
            await super()._exec_release(exec_id)

        async def _exec_collect(self, command, stdin=None):
            events.append(("collect", command))
            return {"returncode": 0, "stdout": b"", "stderr": b""}

    actor = RecordingActor()

    async def failing_chunks():
        for _ in range(3):
            yield b"abcd"
        raise OSError("local disk failed")

    with pytest.raises(OSError, match="local disk failed"):
        run(filesystem(actor)._upload(failing_chunks(), "/dest/file"))

    assert pending_at_release == [0]
    assert not actor.stdin_closed, "an aborted upload must never send its EOF"
    # The killed command's temporary file goes too -- after the release, which
    # returns only once the command is dead.
    temp_path = actor.command[-1]
    assert re.fullmatch(r"/dest/\.ray-sandbox-tmp-[A-Za-z0-9]{6}", temp_path)
    assert events == ["release", ("collect", _fs.make_remove_temp_command(temp_path))]


# -- no write cap ----------------------------------------------------------


def test_a_write_has_no_size_limit(monkeypatch):
    """Modal streams a write whatever its size, and so does this now."""
    monkeypatch.setattr(fs_mod, "_COPY_CHUNK_SIZE", 4)
    actor = UploadActor()
    payload = bytes(range(256)) * 4
    run(filesystem(actor).write_bytes(memoryview(bytearray(payload)), "/dest/file"))
    assert b"".join(actor.writes) == payload
    assert all(isinstance(chunk, bytes) for chunk in actor.writes)


# -- reads -----------------------------------------------------------------


class ReadActor:
    """``exec_collect`` for a one-reply read, and a stream for the rest."""

    def __init__(
        self,
        content: bytes,
        collect_returncode: int = 0,
        collect_stderr: bytes = b"",
        collect_error: Optional[BaseException] = None,
    ):
        self._content = content
        self._collect_returncode = collect_returncode
        self._collect_stderr = collect_stderr
        self._collect_error = collect_error
        self.collect_command: Optional[List[str]] = None
        self.stream_command: Optional[List[str]] = None
        self.stream_kwargs: Optional[dict] = None
        self.wait_kwargs: Optional[dict] = None
        self.released = False

    def __getattr__(self, name):
        impl = getattr(type(self), f"_{name}", None)
        if impl is None:
            raise AttributeError(name)
        return _Shim(self, functools.partial(impl, self))

    async def _exec_collect(self, command, stdin=None):
        self.collect_command = command
        if self._collect_error is not None:
            raise self._collect_error
        stdout = self._content if self._collect_returncode == 0 else b""
        return {
            "returncode": self._collect_returncode,
            "stdout": stdout,
            "stderr": self._collect_stderr,
        }

    async def _exec_start(self, command, **kwargs):
        self.stream_command = command
        self.stream_kwargs = kwargs
        return "exec-2"

    async def _stream_output(self, exec_id, fd, start_offset=0):
        if fd != STDERR_FD:
            for start in range(0, len(self._content), 3):
                yield _Ref(_value((start, self._content[start : start + 3])))

    async def _exec_wait(self, exec_id, timeout, **kwargs):
        self.wait_kwargs = kwargs
        return 0

    async def _exec_release(self, exec_id):
        self.released = True


def test_a_small_file_comes_back_in_one_reply():
    actor = ReadActor(b"hello")
    assert run(filesystem(actor).read_bytes("/some/file")) == b"hello"
    assert actor.stream_command is None
    script = actor.collect_command[2]
    # Sized up front against both limits, and read with one byte to spare.
    assert f"-le {fs_mod.MAX_READ_FILE_BYTES} " in script
    assert f"-le {fs_mod._INLINE_READ_BYTES} " in script
    assert f"head -c {fs_mod._INLINE_READ_BYTES + 1} " in script


def test_a_file_past_the_inline_size_is_streamed_paced():
    content = bytes(range(200))
    actor = ReadActor(content, collect_returncode=_fs.EXIT_NOT_INLINE)
    assert run(filesystem(actor).read_bytes("/some/file")) == content
    assert actor.stream_kwargs == {"paced": True, "client_released": True}
    script = actor.stream_command[2]
    # Refused up front past Modal's limit, then read with cat: busybox's
    # `head -c` is syscall-bound under gVisor.
    assert f"-le {fs_mod.MAX_READ_FILE_BYTES} " in script
    assert "head -c" not in script and 'cat "$p"' in script
    assert actor.released


def test_a_file_whose_size_stat_under_reports_is_streamed(monkeypatch):
    """/proc files report 0 bytes; one byte past the inline size says otherwise."""
    monkeypatch.setattr(fs_mod, "_INLINE_READ_BYTES", 4)
    actor = ReadActor(b"0123456789")
    assert run(filesystem(actor).read_bytes("/proc/something")) == b"0123456789"
    assert actor.stream_command is not None


def test_a_file_over_modals_limit_is_refused_in_modals_words():
    actor = ReadActor(
        b"",
        collect_returncode=_fs.EXIT_FILE_TOO_LARGE,
        collect_stderr=b"6442450944\n",
    )
    with pytest.raises(SandboxFilesystemFileTooLargeError) as excinfo:
        run(filesystem(actor).read_text("/tmp/sparse"))
    assert str(excinfo.value) == (
        "file is 6442450944 bytes, which exceeds the 5368709120 byte limit: "
        "/tmp/sparse"
    )
    assert actor.stream_command is None, "refused before reading anything"


def test_a_stream_that_outgrows_the_limit_is_refused(monkeypatch):
    monkeypatch.setattr(fs_mod, "MAX_READ_FILE_BYTES", 5)
    actor = ReadActor(b"0123456789", collect_returncode=_fs.EXIT_NOT_INLINE)
    with pytest.raises(SandboxFilesystemFileTooLargeError):
        run(filesystem(actor).read_bytes("/dev/zero"))
    assert actor.released


def test_copy_to_local_is_paced_and_uncapped(tmp_path):
    content = bytes(range(256)) * 3
    actor = ReadActor(content)
    destination = tmp_path / "out.bin"
    run(filesystem(actor).copy_to_local("/some/file", destination))
    assert destination.read_bytes() == content
    assert actor.stream_kwargs == {"paced": True, "client_released": True}
    assert "head -c" not in actor.stream_command[2]


def test_a_namespace_kept_past_detach_fails_as_on_modal():
    """Modal's calls fail through their exec, as a SandboxFilesystemError."""
    actor = ReadActor(b"hello")
    with pytest.raises(SandboxFilesystemError, match="detached"):
        run(filesystem(actor, attached=False).read_text("/some/file"))
    assert actor.collect_command is None


def test_a_finished_sandbox_is_reported_in_modals_words():
    """Not the remote traceback Ray's task error carries as its message."""
    actor = ReadActor(b"", collect_error=NotFoundError("ray::_SandboxActor ... junk"))
    with pytest.raises(NotFoundError) as excinfo:
        run(filesystem(actor).read_text("/some/file"))
    assert str(excinfo.value) == (
        "The Sandbox is unavailable. This Sandbox may have already shut down."
    )


def test_transfers_keep_their_sessions_and_report_a_lost_sandbox():
    """Both transfers leave releasing their session to themselves, so the
    actor never evicts it first, and ask for a command the sandbox's end
    killed to be reported as the sandbox being gone."""
    upload = UploadActor()
    run(filesystem(upload).write_bytes(b"x", "/dest/file"))
    assert upload.start_kwargs == {"stdout_devnull": True, "client_released": True}
    assert upload.wait_kwargs == {"require_live": True}

    download = ReadActor(b"abc", collect_returncode=_fs.EXIT_NOT_INLINE)
    run(filesystem(download).read_bytes("/some/file"))
    assert download.wait_kwargs == {"require_live": True}


def test_a_sandbox_ending_mid_download_is_reported_as_unavailable():
    """It used to surface as a filesystem error carrying exit code 137."""

    class EndedActor(ReadActor):
        async def _exec_wait(self, exec_id, timeout, **kwargs):
            if kwargs.get("require_live"):
                raise NotFoundError("The Sandbox is unavailable: it was terminated.")
            return 137

    actor = EndedActor(b"abc", collect_returncode=_fs.EXIT_NOT_INLINE)
    with pytest.raises(NotFoundError) as excinfo:
        run(filesystem(actor).read_bytes("/some/file"))
    assert str(excinfo.value) == (
        "The Sandbox is unavailable. This Sandbox may have already shut down."
    )
    assert actor.released


def test_copy_to_local_accepts_a_name_at_the_length_limit(tmp_path):
    """The temporary file embedded the destination's name, and a name past 231
    bytes pushed it over NAME_MAX: a valid destination could not be opened."""
    actor = ReadActor(b"payload")
    destination = tmp_path / ("n" * 250)
    run(filesystem(actor).copy_to_local("/some/file", destination))
    assert destination.read_bytes() == b"payload"
    assert os.listdir(tmp_path) == ["n" * 250]


class _CleanupRecordingActor(UploadActor):
    """Records the temporary-file removal an upload issues once it has failed."""

    def __init__(self, fail_in: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self._fail_in = fail_in
        self.collected: List[List[str]] = []

    async def _close_stdin(self, exec_id, offset=None):
        if self._fail_in == "close_stdin":
            raise asyncio.CancelledError()
        await super()._close_stdin(exec_id, offset)

    async def _exec_wait(self, exec_id, timeout, **kwargs):
        if self._fail_in == "exec_wait":
            raise asyncio.CancelledError()
        return await super()._exec_wait(exec_id, timeout, **kwargs)

    async def _exec_collect(self, command, stdin=None):
        self.collected.append(command)
        return {"returncode": 0, "stdout": b"", "stderr": b""}


@pytest.mark.parametrize("fail_in", ["close_stdin", "exec_wait"])
def test_an_upload_stopped_after_its_last_write_removes_its_temporary_file(fail_in):
    """Cleanup followed only a failure inside the chunk loop. A cancellation
    while closing stdin or waiting released -- killed -- the command before
    its `mv`, and left the hidden partial file beside the destination."""
    actor = _CleanupRecordingActor(fail_in=fail_in)
    with pytest.raises(asyncio.CancelledError):
        run(filesystem(actor).write_bytes(b"data", "/dest/file"))
    assert actor.collected == [_fs.make_remove_temp_command(actor.command[-1])]


@pytest.mark.parametrize(
    "returncode,removed",
    [(0, False), (_fs.EXIT_PERMISSION_DENIED, False), (137, True)],
)
def test_only_an_upload_that_could_not_clean_up_is_cleaned_up(returncode, removed):
    """The script removes its temporary file on every failure it reports
    itself; a command killed by a signal cannot."""
    actor = _CleanupRecordingActor(returncode=returncode)
    try:
        run(filesystem(actor).write_bytes(b"data", "/dest/file"))
    except SandboxFilesystemError:
        pass
    assert bool(actor.collected) is removed


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
