"""The ``Sandbox.filesystem`` namespace."""

import asyncio
import collections
import errno
import functools
import logging
import os
import stat as stat_module
from pathlib import Path
from typing import (
    AsyncIterator,
    Awaitable,
    BinaryIO,
    Callable,
    Iterator,
    List,
    Optional,
    Union,
)

import ray
from ray.experimental.sandbox.modal import _fs_commands as _fs
from ray.experimental.sandbox.modal._actor import STDERR_FD, STDOUT_FD
from ray.experimental.sandbox.modal._sync import synchronize_api
from ray.experimental.sandbox.modal.exception import (
    NotFoundError,
    NotSupportedError,
    SandboxFilesystemError,
    SandboxFilesystemFileTooLargeError,
)
from ray.experimental.sandbox.modal.io_streams import iter_stream
from ray.experimental.sandbox.modal.types import (
    FileInfo,
    FileWatchEvent,
    FileWatchEventType,
)

logger = logging.getLogger(__name__)

# Upload chunk size. Past Ray's inline limit (`max_direct_call_object_size`,
# 100 KiB) a chunk travels through the object store, a shared-memory copy per
# chunk -- and a transfer still runs about five times faster for it, measured:
# each call costs about a millisecond whatever it carries, so a large transfer
# pays for the number of calls, not the bytes.
_COPY_CHUNK_SIZE = 1024 * 1024

# How many chunk writes may be outstanding before the next one waits. Keeping
# several in flight hides the round trip. Ray runs an async actor's calls in no
# guaranteed order, so each write carries its offset and the actor puts them in
# order (see _SandboxActor.write_stdin), holding each until its turn -- which
# makes this window also the bound on what the actor holds: ~8 MiB at 1 MiB a
# chunk.
_COPY_WINDOW = 8

# The largest file read_bytes(), read_text() and copy_to_local() return:
# Modal's, which its helper enforces before reading a byte.
MAX_READ_FILE_BYTES = _fs.MAX_READ_FILE_BYTES

# Files up to this size come back in the reply to a single call. Larger ones
# are streamed, so no one value -- in the actor or in the object store -- has
# to hold a whole file of up to MAX_READ_FILE_BYTES.
_INLINE_READ_BYTES = 16 * 1024 * 1024

# Where every filesystem command runs. Its paths are all absolute, so the
# directory only has to exist -- which the image's WORKDIR, the default, need
# not: once a caller removed it, runsc failed every command before its script
# ran, and each error message was the whole script.
_COMMAND_CWD = "/"


class _SandboxFilesystem:
    """Filesystem operations on a running Sandbox.

    Every ``remote_path`` must be absolute. Reached through
    :attr:`Sandbox.filesystem` rather than constructed directly.
    """

    def __init__(self, sandbox):
        # A strong reference. A weak one leaves this namespace outliving the
        # Sandbox it belongs to, which is exactly what
        # `Sandbox.create(...).filesystem` does: the blocking wrapper holding
        # the only strong reference is dropped after the attribute access, and
        # every later call raises ReferenceError. The cycle a weakref would
        # avoid here is an ordinary one that CPython's collector handles.
        self._sandbox = sandbox

    # -- reads -------------------------------------------------------------

    async def read_bytes(self, remote_path: str) -> bytes:
        """Read a file and return its contents as bytes.

        Args:
            remote_path: Absolute path to the file in the Sandbox.

        Returns:
            The file's raw bytes.

        Raises:
            SandboxFilesystemNotFoundError: The path does not exist.
            SandboxFilesystemIsADirectoryError: The path is a directory.
            SandboxFilesystemPermissionError: Read permission is denied.
            SandboxFilesystemFileTooLargeError: The file exceeds
                ``MAX_READ_FILE_BYTES`` (5 GiB), Modal's limit.
            SandboxFilesystemError: The command failed for any other reason.
        """
        _fs.validate_absolute_remote_path(remote_path, "read_bytes")
        with _fs.translate_exec_errors("read_bytes", remote_path):
            result = await self._run(
                _fs.make_read_file_command(
                    remote_path, limit=MAX_READ_FILE_BYTES, inline=_INLINE_READ_BYTES
                )
            )
            returncode, data = result["returncode"], result["stdout"]
            # One byte past the inline size means stat under-reported it, as
            # it does for /proc files; stream those too.
            if returncode == 0 and len(data) <= _INLINE_READ_BYTES:
                return data
            if returncode not in (0, _fs.EXIT_NOT_INLINE):
                _fs.raise_read_file_error(returncode, result["stderr"], remote_path)

            chunks: List[bytes] = []
            received = 0

            async def keep(chunk: bytes) -> None:
                nonlocal received
                received += len(chunk)
                if received > MAX_READ_FILE_BYTES:
                    raise SandboxFilesystemFileTooLargeError(
                        f"file exceeds the {MAX_READ_FILE_BYTES} byte limit: "
                        f"{remote_path}"
                    )
                chunks.append(chunk)

            await self._stream_file(
                _fs.make_read_file_command(remote_path, limit=MAX_READ_FILE_BYTES),
                remote_path,
                keep,
            )
            return b"".join(chunks)

    async def read_text(self, remote_path: str) -> str:
        """Read a file and return its contents decoded as UTF-8.

        Args:
            remote_path: Absolute path to the file in the Sandbox.

        Returns:
            The file's contents as text.

        Raises:
            SandboxFilesystemNotFoundError: The path does not exist.
            SandboxFilesystemIsADirectoryError: The path is a directory.
            SandboxFilesystemPermissionError: Read permission is denied.
            SandboxFilesystemFileTooLargeError: The file exceeds
                ``MAX_READ_FILE_BYTES`` (5 GiB), Modal's limit.
            SandboxFilesystemError: The command failed for any other reason.
        """
        _fs.validate_absolute_remote_path(remote_path, "read_text")
        data = await self.read_bytes(remote_path)
        return data.decode("utf-8")

    async def stat(self, remote_path: str) -> FileInfo:
        """Return metadata for a single path.

        A symlink is described as itself, not as its target.

        Args:
            remote_path: Absolute path in the Sandbox.

        Returns:
            A :class:`FileInfo` for the path.

        Raises:
            SandboxFilesystemNotFoundError: The path does not exist.
            SandboxFilesystemNotADirectoryError: A non-leaf component is not a
                directory.
            SandboxFilesystemPermissionError: A path component is not searchable.
            SandboxFilesystemError: The command failed for any other reason.
        """
        _fs.validate_absolute_remote_path(remote_path, "stat")
        with _fs.translate_exec_errors("stat", remote_path):
            result = await self._run(_fs.make_stat_command(remote_path))
        if result["returncode"] != 0:
            _fs.raise_stat_error(result["returncode"], result["stderr"], remote_path)
        info = _fs.parse_stat_record_for(result["stdout"], remote_path)
        if info is None:
            raise SandboxFilesystemError(f"Could not read metadata for '{remote_path}'")
        return info

    async def list_files(self, remote_path: str) -> List[FileInfo]:
        """List the entries of a directory.

        Args:
            remote_path: Absolute path to the directory in the Sandbox.

        Returns:
            A :class:`FileInfo` for each entry, including hidden ones, sorted by
            name.

        Raises:
            SandboxFilesystemNotFoundError: The path does not exist.
            SandboxFilesystemNotADirectoryError: The path is not a directory.
            SandboxFilesystemPermissionError: Read permission is denied.
            SandboxFilesystemError: The command failed for any other reason.
        """
        _fs.validate_absolute_remote_path(remote_path, "list_files")
        with _fs.translate_exec_errors("list_files", remote_path):
            result = await self._run(_fs.make_list_files_command(remote_path))
        if result["returncode"] != 0:
            _fs.raise_list_files_error(
                result["returncode"], result["stderr"], remote_path
            )
        # By code point, as Modal sorts: the shell's glob order depends on the
        # locale and puts hidden entries last.
        return sorted(_fs.parse_stat_records(result["stdout"]), key=lambda e: e.name)

    # -- writes ------------------------------------------------------------

    async def write_bytes(
        self, data: Union[bytes, bytearray, memoryview], remote_path: str
    ) -> None:
        """Write binary content to a file, creating parent directories.

        Note the Modal argument order: the data comes first.

        Args:
            data: Bytes to write.
            remote_path: Absolute path to the file in the Sandbox.

        Raises:
            TypeError: ``data`` is not bytes-like.
            SandboxFilesystemNotADirectoryError: A parent component is not a
                directory.
            SandboxFilesystemIsADirectoryError: ``remote_path`` is a directory.
            SandboxFilesystemPermissionError: Write permission is denied.
            SandboxFilesystemError: The command failed for any other reason.
        """
        # Path before type, the order Modal checks in: a call that gets both
        # wrong reports the same one first on either backend.
        _fs.validate_absolute_remote_path(remote_path, "write_bytes")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            # Modal's wording.
            raise TypeError("data must be bytes-like")
        # No size limit, as on Modal: the upload streams in chunks, so nothing
        # ever holds more of it than the caller already does.
        view = memoryview(data)
        view = view.cast("B") if view.c_contiguous else memoryview(view.tobytes())
        with _fs.translate_exec_errors("write_bytes", remote_path):
            await self._upload(_chunked(view), remote_path, size=view.nbytes)

    async def write_text(self, data: str, remote_path: str) -> None:
        """Write UTF-8 text to a file, creating parent directories.

        Args:
            data: Text to write.
            remote_path: Absolute path to the file in the Sandbox.

        Raises:
            TypeError: ``data`` is not a string.
            SandboxFilesystemNotADirectoryError: A parent component is not a
                directory.
            SandboxFilesystemIsADirectoryError: ``remote_path`` is a directory.
            SandboxFilesystemPermissionError: Write permission is denied.
            SandboxFilesystemError: The command failed for any other reason.
        """
        _fs.validate_absolute_remote_path(remote_path, "write_text")
        if not isinstance(data, str):
            raise TypeError("data must be str")
        await self.write_bytes(data.encode("utf-8"), remote_path)

    async def make_directory(
        self, remote_path: str, *, create_parents: bool = True
    ) -> None:
        """Create a directory.

        With ``create_parents`` (the default) missing parents are created and
        the call succeeds silently if the directory already exists. Without it,
        the parent must exist and the path must not.

        Args:
            remote_path: Absolute path of the directory to create.
            create_parents: Create missing parents and tolerate an existing
                directory.

        Raises:
            SandboxFilesystemNotFoundError: The parent does not exist and
                ``create_parents`` is false.
            SandboxFilesystemPathAlreadyExistsError: The path already exists.
            SandboxFilesystemNotADirectoryError: A path component is not a
                directory.
            SandboxFilesystemPermissionError: Creation is not permitted.
            SandboxFilesystemError: The command failed for any other reason.
        """
        _fs.validate_absolute_remote_path(remote_path, "make_directory")
        with _fs.translate_exec_errors("make_directory", remote_path):
            result = await self._run(
                _fs.make_make_directory_command(remote_path, create_parents)
            )
        if result["returncode"] != 0:
            _fs.raise_make_directory_error(
                result["returncode"], result["stderr"], remote_path
            )

    async def remove(self, remote_path: str, *, recursive: bool = False) -> None:
        """Remove a file or directory.

        A directory is removed only if empty, unless ``recursive`` is set.

        Args:
            remote_path: Absolute path to remove.
            recursive: Remove a directory and everything under it.

        Raises:
            SandboxFilesystemNotFoundError: The path does not exist.
            SandboxFilesystemDirectoryNotEmptyError: The directory is not empty
                and ``recursive`` is false.
            SandboxFilesystemPermissionError: Removal is not permitted.
            SandboxFilesystemError: The command failed for any other reason.
        """
        _fs.validate_absolute_remote_path(remote_path, "remove")
        with _fs.translate_exec_errors("remove", remote_path):
            result = await self._run(_fs.make_remove_command(remote_path, recursive))
        if result["returncode"] != 0:
            _fs.raise_remove_error(result["returncode"], result["stderr"], remote_path)

    # -- transfers ---------------------------------------------------------

    async def copy_from_local(
        self, local_path: Union[str, os.PathLike], remote_path: str
    ) -> None:
        """Copy a local file into the Sandbox, streaming its contents.

        Parent directories are created and an existing remote file is
        overwritten.

        Args:
            local_path: Path to the file on the local machine.
            remote_path: Absolute path to the file in the Sandbox.

        Raises:
            SandboxFilesystemNotADirectoryError: A parent component of
                ``remote_path`` is not a directory.
            SandboxFilesystemIsADirectoryError: ``remote_path`` is a directory.
            SandboxFilesystemPermissionError: Write permission is denied in the
                Sandbox.
            SandboxFilesystemError: The command failed for any other reason.
            FileNotFoundError: ``local_path`` does not exist.
            IsADirectoryError: ``local_path`` is a directory.
            PermissionError: Reading ``local_path`` is not permitted.
        """
        _fs.validate_absolute_remote_path(remote_path, "copy_from_local")
        # Opened outside the translation block so a local OSError
        # (FileNotFoundError, IsADirectoryError, PermissionError) reaches the
        # caller unchanged rather than being reported as a Sandbox failure.
        with open(local_path, "rb") as source:
            # The size the command checks before installing the file. Only a
            # regular file has one up front; a pipe or a device is sent to its
            # end, unchecked.
            info = os.fstat(source.fileno())
            size = info.st_size if stat_module.S_ISREG(info.st_mode) else None
            with _fs.translate_exec_errors("copy_from_local", remote_path):
                await self._upload(_read_chunks(source, size), remote_path, size=size)

    async def copy_to_local(
        self, remote_path: str, local_path: Union[str, os.PathLike]
    ) -> None:
        """Copy a file out of the Sandbox, streaming its contents.

        The local file is written atomically: content lands in a sibling
        temporary file that replaces the destination on success. Subject to
        Modal's read limit, as on Modal, whose copy reads through the same
        helper command as ``read_bytes``.

        Args:
            remote_path: Absolute path to the file in the Sandbox.
            local_path: Destination path on the local machine.

        Raises:
            SandboxFilesystemNotFoundError: The remote path does not exist.
            SandboxFilesystemIsADirectoryError: The remote path is a directory.
            SandboxFilesystemPermissionError: Read permission is denied.
            SandboxFilesystemFileTooLargeError: The file exceeds
                ``MAX_READ_FILE_BYTES`` (5 GiB), Modal's limit.
            SandboxFilesystemError: The command failed for any other reason.
            IsADirectoryError: ``local_path`` is a directory.
            NotADirectoryError: A component of ``local_path``'s parent is not a
                directory.
            PermissionError: Writing ``local_path`` is not permitted.
        """
        _fs.validate_absolute_remote_path(remote_path, "copy_to_local")
        destination = Path(local_path)
        # Off the loop: one daemon loop serves every blocking call in the
        # process, so a slow disk here stalls unrelated sandboxes too.
        try:
            await asyncio.to_thread(
                destination.parent.mkdir, parents=True, exist_ok=True
            )
        except FileExistsError:
            # A file where the parent directory should be. mkdir says "exists";
            # opening the destination there would say ENOTDIR, which is what
            # this method documents.
            raise NotADirectoryError(
                errno.ENOTDIR, os.strerror(errno.ENOTDIR), str(destination.parent)
            ) from None
        temp_path = destination.with_name(_fs.temp_name())

        with _fs.translate_exec_errors("copy_to_local", remote_path):
            sink = await asyncio.to_thread(open, temp_path, "wb")
            try:
                try:
                    received = 0

                    async def write(chunk: bytes) -> None:
                        # Counted as it arrives, as read_bytes() counts it: the
                        # command refuses a file stat says is too large before
                        # reading it, but stat under-reports /proc files.
                        nonlocal received
                        received += len(chunk)
                        if received > MAX_READ_FILE_BYTES:
                            raise SandboxFilesystemFileTooLargeError(
                                f"file exceeds the {MAX_READ_FILE_BYTES} byte "
                                f"limit: {remote_path}"
                            )
                        await asyncio.to_thread(sink.write, chunk)

                    await self._stream_file(
                        _fs.make_read_file_command(
                            remote_path, limit=MAX_READ_FILE_BYTES
                        ),
                        remote_path,
                        write,
                    )
                finally:
                    await asyncio.to_thread(sink.close)
                await asyncio.to_thread(temp_path.replace, destination)
            except BaseException:
                await asyncio.to_thread(temp_path.unlink, missing_ok=True)
                raise

    def watch(
        self,
        remote_path: str,
        *,
        filter: Optional[List[FileWatchEventType]] = None,
        recursive: bool = False,
        timeout: Optional[int] = None,
    ) -> Iterator[FileWatchEvent]:
        """Not supported yet.

        The parameters are Modal's, declared rather than swallowed by
        ``**kwargs`` so that a caller passing them gets this refusal instead of
        a ``TypeError``. Note Modal defaults ``recursive`` to ``False`` here and
        to ``None`` on :meth:`Sandbox.watch`.

        Args:
            remote_path: Absolute path in the Sandbox to watch.
            filter: Event types to report. None reports all of them.
            recursive: Also report events in nested subdirectories.
            timeout: Seconds to watch for. None watches indefinitely.

        Returns:
            On Modal, an iterator of :class:`FileWatchEvent`. Nothing is
            returned here; the call always raises.

        Raises:
            NotImplementedError: Always.
        """
        # Not a coroutine, because Modal's is not: it returns an iterator and
        # only then does any work.
        # NotSupportedError, not the builtin: it subclasses NotImplementedError
        # while also joining the modal.Error hierarchy, so this stays catchable
        # the same way every other unsupported member is.
        raise NotSupportedError(
            "Sandbox.filesystem.watch() is not supported by the Ray sandbox "
            "backend yet."
        )

    # -- internals ---------------------------------------------------------

    @property
    def _actor(self):
        # This namespace can outlive detach() in a variable. On Modal its
        # calls then fail through their exec; here they would have kept
        # working. Only ever read inside translate_exec_errors, which turns
        # the ClientClosed into the SandboxFilesystemError Modal raises.
        self._sandbox._ensure_attached()
        return self._sandbox._actor

    async def _run(self, command: List[str], stdin: Optional[bytes] = None):
        """Run one filesystem command to completion inside the sandbox."""
        return await _await_or_cancel(
            self._actor.exec_collect.remote(command, stdin=stdin, cwd=_COMMAND_CWD)
        )

    async def _stream_file(self, command: List[str], remote_path: str, consume) -> None:
        """Run a read command and hand its stdout to ``consume``, chunk by chunk.

        Paced: the command waits for this side rather than the actor holding
        what has not been read yet. A transfer used to run the command flat
        out, and where the actor could not spill -- a nearly full disk, a
        spent budget -- a reader more than a few MiB behind lost bytes and the
        copy failed.
        """
        actor = self._actor
        # client_released: the `finally` below always releases this session,
        # so the actor must not evict it first. Its command can exit with a
        # buffer still unread, and evicting it then lost that tail.
        exec_id = await _start_exec(
            actor,
            command,
            functools.partial(self._release, actor),
            cwd=_COMMAND_CWD,
            paced=True,
            client_released=True,
        )
        try:
            received = 0
            async for offset, chunk in iter_stream(actor, exec_id, STDOUT_FD):
                if offset != received:
                    # Pacing never drops, so a hole means the session's output
                    # was released under the read -- terminate() releases every
                    # stream's. Say the sandbox is gone, as Modal does; only a
                    # hole in a live sandbox is reported as lost bytes.
                    await _raise_if_sandbox_ended(actor)
                    raise SandboxFilesystemError(
                        f"Reading '{remote_path}' lost {offset - received} bytes"
                    )
                received += len(chunk)
                await consume(chunk)
            # require_live: a command the sandbox ended under failed because
            # the sandbox went, which is a NotFoundError, as exec_collect
            # reports it -- not a filesystem error with exit code 137.
            returncode = await actor.exec_wait.remote(exec_id, None, require_live=True)
            if returncode != 0:
                # Drained only on failure: on the common path this is an extra
                # streaming session whose output is discarded.
                _fs.raise_read_file_error(
                    returncode, await self._drain(exec_id, STDERR_FD), remote_path
                )
        finally:
            await self._release(actor, exec_id)

    async def _upload(
        self, chunks, remote_path: str, size: Optional[int] = None
    ) -> None:
        """Stream ``chunks`` into ``remote_path`` as the file's new contents.

        Shared by :meth:`write_bytes` and :meth:`copy_from_local`, which differ
        only in where their bytes come from. ``size``, when known, is what the
        chunks add up to; the command refuses to install anything shorter.
        """
        actor = self._actor
        temp_path = _fs.make_temp_path(remote_path)

        async def abandon(exec_id: str) -> None:
            # Started after the caller gave up: stop it before its stdin can
            # close, then remove what it may have begun writing.
            await self._release(actor, exec_id)
            await self._remove_quietly(actor, temp_path)

        # client_released, as in _stream_file: the stderr a failure is reported
        # from must still be there when it is drained below.
        exec_id = await _start_exec(
            actor,
            _fs.make_write_file_command(remote_path, temp_path, size=size),
            abandon,
            cwd=_COMMAND_CWD,
            stdout_devnull=True,
            client_released=True,
        )
        returncode = None
        # Released in `finally`, as copy_to_local does: a read error or a broken
        # pipe part-way through would otherwise strand the session on the actor,
        # holding its output buffers and possibly a live process, for as long as
        # the sandbox exists.
        try:
            # Replies are not awaited one at a time -- awaiting each turned a
            # large file into one round trip per chunk -- but only _COPY_WINDOW
            # may be outstanding at once. Each names its offset: Ray may run
            # them in any order, and the actor writes them in this one.
            writes = collections.deque()
            offset = 0
            reading = True
            try:
                async for chunk in chunks:
                    writes.append(actor.write_stdin.remote(exec_id, chunk, offset))
                    offset += len(chunk)
                    while len(writes) >= _COPY_WINDOW:
                        reading = await writes.popleft() is not False and reading
                    if not reading:
                        # The command stopped reading: it failed before taking
                        # the payload -- a directory in the way, a read-only
                        # filesystem. Its exit code says which; the rest of
                        # the payload would only be thrown away.
                        break
                while writes:
                    await writes.popleft()
            except BaseException:
                # Settle the writes still in flight before the release below.
                # Left alone, they ran against the released session and each
                # logged an unhandled "Unknown exec session" error.
                await asyncio.gather(*writes, return_exceptions=True)
                raise
            await actor.close_stdin.remote(exec_id, offset)
            returncode = await actor.exec_wait.remote(exec_id, None, require_live=True)
            if returncode != 0:
                # Inside the `try`, so the drain happens before the `finally`
                # releases the session: exec_release drops the session and the
                # stderr this message is built from along with it, which used to
                # make every failing upload report a lookup error instead of
                # what actually went wrong. Drained only on failure, since on
                # the common path it is a streaming session whose output is
                # built and thrown away.
                _fs.raise_write_file_error(
                    returncode, await self._drain(exec_id, STDERR_FD), remote_path
                )
        finally:
            # Releasing a running upload kills it before its stdin can close,
            # so the destination is never replaced by a partial file.
            await self._release(actor, exec_id)
            # The script removes its temporary file on every failure it reports
            # itself. One that never reported -- aborted, or cancelled while
            # closing stdin or waiting -- or that was killed (128 + signal) can
            # have left it behind. Cleaning up only after a failure in the
            # chunk loop missed the cancellations after it.
            if returncode is None or returncode >= 128:
                await self._remove_quietly(actor, temp_path)

    @staticmethod
    async def _remove_quietly(actor, path: str) -> None:
        """Delete an aborted upload's temporary file, never masking the error.

        The killed command could not do it itself; left alone, the hidden
        partial file would sit beside its destination for good.
        """
        try:
            await actor.exec_collect.remote(
                _fs.make_remove_temp_command(path), cwd=_COMMAND_CWD
            )
        except Exception:
            logger.debug("Failed to remove %s", path, exc_info=True)

    @staticmethod
    async def _release(actor, exec_id: str) -> None:
        """Release an exec session without ever masking the real failure.

        This runs from a `finally`, so an exception here replaces whatever sent
        us there -- and the most likely cause, the actor having died mid
        transfer, is exactly when the original error matters most.
        """
        try:
            await actor.exec_release.remote(exec_id)
        except Exception:
            logger.debug("Failed to release exec %s", exec_id, exc_info=True)

    async def _drain(self, exec_id: str, fd: int) -> bytes:
        """Collect everything remaining on one stream of a finished command.

        The offset is ignored: this collects stderr to build an error message,
        where a gap costs some context but nothing depends on the bytes being
        contiguous.
        """
        collected = bytearray()
        async for _, chunk in iter_stream(self._actor, exec_id, fd):
            collected.extend(chunk)
        return bytes(collected)


async def _raise_if_sandbox_ended(actor) -> None:
    """Raise NotFoundError if the sandbox behind ``actor`` has ended.

    Called inside ``translate_exec_errors``, which turns this -- or the
    RayActorError of an actor already gone -- into Modal's "The Sandbox is
    unavailable" message.
    """
    state = await actor.get_state.remote()
    if state.get("exit_reason") is not None or state.get("deleted"):
        raise NotFoundError("The Sandbox has ended.")


async def _chunked(data: memoryview) -> AsyncIterator[bytes]:
    """Yield an in-memory payload in upload-sized pieces.

    Each piece is copied out of the view as it is sent, so a large payload is
    never duplicated whole. Empty input yields nothing, so the write command
    sees an immediate EOF and creates an empty file -- which is what
    ``write_bytes(b"")`` should do.
    """
    for start in range(0, len(data), _COPY_CHUNK_SIZE):
        yield bytes(data[start : start + _COPY_CHUNK_SIZE])


async def _read_chunks(
    source: BinaryIO, size: Optional[int] = None
) -> AsyncIterator[bytes]:
    """Yield a local file in upload-sized pieces.

    Reads go to a thread for the same reason the actor does its blocking work
    there: this coroutine shares one loop with every other call in the process.

    Args:
        source: The open file.
        size: The bytes to send, which the command checks before installing
            the file: no more are read, and a file that ends sooner raises.
            None reads to the end.

    Raises:
        OSError: The file ended before ``size`` bytes.
    """
    remaining = size
    while remaining is None or remaining > 0:
        want = (
            _COPY_CHUNK_SIZE if remaining is None else min(_COPY_CHUNK_SIZE, remaining)
        )
        chunk = await asyncio.to_thread(source.read, want)
        if not chunk:
            if remaining:
                raise OSError(
                    f"{source.name} shrank while it was being copied: "
                    f"{size - remaining} of {size} bytes were read"
                )
            return
        if remaining is not None:
            remaining -= len(chunk)
        yield chunk


async def _await_or_cancel(ref):
    """Await an actor call, cancelling it if the wait is cancelled.

    Cancelling the await of an ObjectRef stops only the waiting: the actor
    task runs on. exec_collect stops its command when it is cancelled itself,
    so a filesystem call given up on -- Ctrl-C, a timeout -- used to leave a
    blocked read (a FIFO, a stalled FUSE mount) running in the sandbox until
    teardown.
    """
    try:
        return await ref
    except asyncio.CancelledError:
        try:
            ray.cancel(ref)
        except Exception:
            # Never in place of the cancellation; an already finished task, or
            # one the actor's death took, leaves nothing to stop.
            logger.debug("Could not cancel a filesystem command", exc_info=True)
        raise


# Clean-ups for sessions started after their caller was cancelled, held so the
# tasks are not collected before they finish.
_ABANDONED: set = set()


async def _start_exec(
    actor, command: List[str], abandon: Callable[[str], Awaitable[None]], **kwargs
) -> str:
    """Start a transfer's command, handing it to ``abandon`` if the caller quits.

    A cancellation that lands while exec_start is in flight leaves a session
    nobody has the id of, so nobody releases it: a paced read blocked at its
    window, an upload's command waiting on stdin, for as long as the sandbox
    lives. The start runs on, shielded, and once it has the id ``abandon``
    gets it.
    """
    started = asyncio.ensure_future(
        _resolve(actor.exec_start.remote(command, **kwargs))
    )
    try:
        return await asyncio.shield(started)
    except asyncio.CancelledError:
        started.add_done_callback(functools.partial(_abandon_started, abandon))
        raise


async def _resolve(awaitable):
    return await awaitable


def _abandon_started(abandon: Callable[[str], Awaitable[None]], started) -> None:
    if started.cancelled() or started.exception() is not None:
        # Never started, so there is nothing to release.
        return
    task = asyncio.ensure_future(abandon(started.result()))
    _ABANDONED.add(task)
    task.add_done_callback(_ABANDONED.discard)


SandboxFilesystem = synchronize_api(_SandboxFilesystem)
