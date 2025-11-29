# Copyright 2017 The Ray Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import contextlib
import os
import select
import time
from typing import Optional

if os.name == "nt":
    import ctypes
    import msvcrt
    from ctypes import wintypes

    _ERROR_BROKEN_PIPE = 109  # Windows ERROR_BROKEN_PIPE
    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.PeekNamedPipe.restype = wintypes.BOOL
    _kernel32.PeekNamedPipe.argtypes = (
        wintypes.HANDLE,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
    )

    def _windows_peek_available(fd: int) -> int:
        """Return available bytes for an anonymous pipe, or -1 if the writer is closed."""
        handle = msvcrt.get_osfhandle(fd)
        avail = wintypes.DWORD()
        res = _kernel32.PeekNamedPipe(
            wintypes.HANDLE(handle), None, 0, None, ctypes.byref(avail), None
        )
        if res == 0:
            err = ctypes.get_last_error()
            if err == _ERROR_BROKEN_PIPE:
                return -1
            raise ctypes.WinError(err)
        return int(avail.value)


def _read_from_fd(fd: int, timeout_s: int = 30, chunk_size: int = 64) -> str:
    """Read data from a pipe file descriptor.

    Args:
        fd: The file descriptor to read from.
        timeout_s: How long to wait for data before timing out.
        chunk_size: Size of each read chunk.

    Returns:
        The data read from the pipe as a string.

    Raises:
        RuntimeError: If timeout occurs or EOF before data.
    """
    deadline = time.monotonic() + timeout_s
    chunks = []
    while time.monotonic() < deadline:
        remaining = max(0.0, deadline - time.monotonic())
        if os.name == "nt":
            available = _windows_peek_available(fd)
            if available < 0:
                chunk = b""
            elif available == 0:
                time.sleep(0.05)
                continue
            else:
                chunk = os.read(fd, min(chunk_size, available))
        else:
            rlist, _, _ = select.select([fd], [], [], min(remaining, 1.0))
            if not rlist:
                continue
            chunk = os.read(fd, chunk_size)
        if chunk:
            chunks.append(chunk)
        else:
            # Writer closed.
            if chunks:
                return b"".join(chunks).decode(errors="replace")
            raise RuntimeError("EOF before any data was read")

    raise RuntimeError("Timed out waiting for data from pipe.")


class PipePair:
    """Anonymous pipe for passing data between processes.

    Use cases:
    1. Parent creates pipe, two child processes communicate:
       - One child reads (e.g., ray_client_server)
       - Another child writes (e.g., Runtime Env Agent)

    2. Parent creates pipe, parent reads, child writes:
       - Parent reads (e.g., node.py reading GCS port)
       - Child writes (e.g., gcs_server)

    Example (two child processes):
        pipe = PipePair()
        reader_handle = pipe.make_reader_handle()
        writer_handle = pipe.make_writer_handle()
        start_reader(..., pass_handles=[reader_handle])
        pipe.close_reader_handle()  # Close parent's copy after subprocess starts
        start_writer(..., pass_handles=[writer_handle])
        pipe.close_writer_handle()  # Close parent's copy after subprocess starts

    Example (parent reads):
        pipe = PipePair()
        writer_handle = pipe.make_writer_handle()
        start_writer(..., pass_handles=[writer_handle])
        pipe.close_writer_handle()  # Close parent's copy after subprocess starts
        with pipe.make_reader() as reader:
            data = reader.read()
    """

    def __init__(self):
        self._read_fd, self._write_fd = os.pipe()
        # Note: We do NOT set inheritable here. Fds are non-inheritable by default.
        # Only make_reader_handle() and make_writer_handle() set inheritable=True
        # for the specific fd being passed to a subprocess. This prevents
        # other subprocesses from accidentally inheriting fds they shouldn't have.
        # Track fds that were returned via make_*_handle() for later closing
        self._read_fd_for_close: Optional[int] = None
        self._write_fd_for_close: Optional[int] = None

    def make_reader_handle(self) -> int:
        """Get read handle to pass to a subprocess.

        Use this when a child process will read from the pipe.
        The handle should be passed via pass_fds/pass_handles to subprocess.
        After subprocess starts, call close_reader_handle() to close the
        parent's copy of the fd.

        Returns:
            The read handle (fd on POSIX, HANDLE on Windows).
        """
        if self._read_fd is None:
            raise RuntimeError("read_fd already taken or closed")
        fd = self._read_fd
        # Set inheritable so subprocess can inherit this fd
        os.set_inheritable(fd, True)
        # Keep track of fd for close_reader_handle(), but mark as "taken"
        self._read_fd_for_close = fd
        self._read_fd = None
        if os.name == "nt":
            handle = int(msvcrt.get_osfhandle(fd))
            # On Windows, we got the handle but still need to track fd for closing
            self._read_fd_for_close = fd
            return handle
        return fd

    def make_writer_handle(self) -> int:
        """Get write handle to pass to a subprocess.

        Use this when a child process will write to the pipe.
        The handle should be passed via pass_fds/pass_handles to subprocess.
        After subprocess starts, call close_writer_handle() to close the
        parent's copy of the fd.

        Returns:
            The write handle (fd on POSIX, HANDLE on Windows).
        """
        if self._write_fd is None:
            raise RuntimeError("write_fd already taken or closed")
        fd = self._write_fd
        # Set inheritable so subprocess can inherit this fd
        os.set_inheritable(fd, True)
        # Keep track of fd for close_writer_handle(), but mark as "taken"
        self._write_fd_for_close = fd
        self._write_fd = None
        if os.name == "nt":
            handle = int(msvcrt.get_osfhandle(fd))
            # On Windows, we got the handle but still need to track fd for closing
            self._write_fd_for_close = fd
            return handle
        return fd

    def make_reader(self) -> "PipeReader":
        """Create a PipeReader for the parent process to read.

        Use this when the parent process itself wants to read from the pipe.
        The returned PipeReader owns the fd and will close it.

        Returns:
            A PipeReader instance.
        """
        if self._read_fd is None:
            raise RuntimeError("read_fd already taken or closed")
        fd = self._read_fd
        self._read_fd = None
        return PipeReader(fd)

    def close_reader_handle(self) -> None:
        """Close the parent's copy of the read fd after subprocess inherits it.

        Call this after the subprocess that will use the read handle has started.
        This is necessary on POSIX to avoid fd leaks and allow proper EOF detection.
        """
        if self._read_fd_for_close is not None:
            with contextlib.suppress(OSError):
                os.close(self._read_fd_for_close)
            self._read_fd_for_close = None

    def close_writer_handle(self) -> None:
        """Close the parent's copy of the write fd after subprocess inherits it.

        Call this after the subprocess that will use the write handle has started.
        This is necessary on POSIX to avoid fd leaks and allow proper EOF detection.
        """
        if self._write_fd_for_close is not None:
            with contextlib.suppress(OSError):
                os.close(self._write_fd_for_close)
            self._write_fd_for_close = None

    def close(self) -> None:
        """Close any remaining fds (cleanup if make_* wasn't called)."""
        if self._read_fd is not None:
            with contextlib.suppress(OSError):
                os.close(self._read_fd)
            self._read_fd = None
        if self._write_fd is not None:
            with contextlib.suppress(OSError):
                os.close(self._write_fd)
            self._write_fd = None
        # Also close any handles that were given out but not yet closed
        self.close_reader_handle()
        self.close_writer_handle()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class PipeReader:
    """Read helper that accepts an inheritable pipe handle from another process."""

    def __init__(self, pipe_handle: int):
        if os.name == "nt":
            # Convert OS handle to an fd we can read with os.read.
            self._fd = msvcrt.open_osfhandle(pipe_handle, os.O_RDONLY)
        else:
            self._fd = int(pipe_handle)
        self._closed = False

    def read(self, timeout_s: int = 30, chunk_size: int = 64) -> str:
        """Read data from the pipe.

        Args:
            timeout_s: How long to wait for data before timing out.
            chunk_size: Size of each read chunk.

        Returns:
            The data read from the pipe as a string.

        Raises:
            RuntimeError: If timeout occurs or EOF before data.
        """
        if self._closed:
            raise RuntimeError("Attempting to read from a closed pipe.")
        return _read_from_fd(self._fd, timeout_s, chunk_size)

    def close(self) -> None:
        if self._closed:
            return
        with contextlib.suppress(OSError):
            os.close(self._fd)
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class PipeWriter:
    """Write helper that accepts an inheritable pipe handle from another process."""

    def __init__(self, pipe_handle: int):
        if os.name == "nt":
            # Convert OS handle to an fd we can write with os.write.
            self._fd = msvcrt.open_osfhandle(pipe_handle, os.O_WRONLY)
        else:
            self._fd = int(pipe_handle)
        self._closed = False

    def write(self, data: str) -> None:
        if isinstance(data, str):
            payload = data.encode()
        else:
            payload = data
        view = memoryview(payload)
        while view:
            try:
                written = os.write(self._fd, view)
            except InterruptedError:
                continue
            if written == 0:
                raise RuntimeError("Failed to write to pipe.")
            view = view[written:]

    def close(self) -> None:
        if self._closed:
            return
        with contextlib.suppress(OSError):
            os.close(self._fd)
        self._closed = True

    # Alias for compatibility
    close_write = close

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class MultiPipeWriter:
    """Write to multiple pipe handles simultaneously.

    This is useful when multiple consumers need to receive the same data,
    e.g., reporting a port to both Raylet and ray_client_server.
    """

    def __init__(self, pipe_handles: list):
        """Initialize with a list of pipe handles.

        Args:
            pipe_handles: List of pipe handles (fd on POSIX, HANDLE on Windows).
                          None values in the list are ignored.
        """
        self._writers = [
            PipeWriter(h) for h in pipe_handles if h is not None and h >= 0
        ]

    def write(self, data: str) -> None:
        """Write data to all pipes."""
        for writer in self._writers:
            writer.write(data)

    def close(self) -> None:
        """Close all pipe write ends."""
        for writer in self._writers:
            writer.close()

    # Alias for compatibility
    close_write = close

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
