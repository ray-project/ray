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


class Pipe:
    """Anonymous pipe for passing data between processes.

    Example 1: Parent reads, child writes
        # Parent process
        pipe = Pipe()
        writer_handle = pipe.make_writer_handle()
        subprocess.Popen(
            ["python", "child.py", "--writer-handle", str(writer_handle)],
            ...,
        )
        pipe.close_writer_handle()  # Close parent's copy of writer
        data = pipe.read()
        pipe.close()

        # Child process (child.py)
        writer_handle = int(args.writer_handle)
        with Pipe.from_writer_handle(writer_handle) as pipe:
            pipe.write("hello from child")

    Example 2: Child B writes, child C reads (sibling communication)
        # Parent process A
        pipe = Pipe()
        writer_handle = pipe.make_writer_handle()
        reader_handle = pipe.make_reader_handle()
        subprocess.Popen(
            ["python", "child_b.py", "--writer-handle", str(writer_handle)],
            ...,
        )
        pipe.close_writer_handle()  # Close parent's copy of writer

        subprocess.Popen(
            ["python", "child_c.py", "--reader-handle", str(reader_handle)],
            ...,
        )
        pipe.close_reader_handle()  # Close parent's copy of reader

        # Child process B (child_b.py)
        writer_handle = int(args.writer_handle)
        with Pipe.from_writer_handle(writer_handle) as pipe:
            pipe.write("hello from child B")

        # Child process C (child_c.py)
        reader_handle = int(args.reader_handle)
        with Pipe.from_reader_handle(reader_handle) as pipe:
            data = pipe.read()
    """

    def __init__(self):
        self._read_fd, self._write_fd = os.pipe()
        self._read_fd_for_close: Optional[int] = None
        self._write_fd_for_close: Optional[int] = None

    @classmethod
    def from_reader_handle(cls, reader_handle: int) -> "Pipe":
        """Create a Pipe that wraps an external reader fd."""
        pipe = cls.__new__(cls)
        if os.name == "nt":
            pipe._read_fd = msvcrt.open_osfhandle(reader_handle, os.O_RDONLY)
        else:
            pipe._read_fd = int(reader_handle)
        os.set_inheritable(pipe._read_fd, False)
        pipe._write_fd = None
        pipe._read_fd_for_close = None
        pipe._write_fd_for_close = None
        return pipe

    @classmethod
    def from_writer_handle(cls, writer_handle: int) -> "Pipe":
        """Create a Pipe that wraps an external writer fd."""
        pipe = cls.__new__(cls)
        pipe._read_fd = None
        if os.name == "nt":
            pipe._write_fd = msvcrt.open_osfhandle(writer_handle, os.O_WRONLY)
        else:
            pipe._write_fd = int(writer_handle)
        os.set_inheritable(pipe._write_fd, False)
        pipe._read_fd_for_close = None
        pipe._write_fd_for_close = None
        return pipe

    def make_reader_handle(self) -> int:
        """Get read handle to pass to another process."""
        if self._read_fd is None:
            raise RuntimeError("read_fd already taken or closed")
        fd = self._read_fd
        os.set_inheritable(fd, True)
        self._read_fd_for_close = fd
        self._read_fd = None
        if os.name == "nt":
            handle = int(msvcrt.get_osfhandle(fd))
            self._read_fd_for_close = fd
            return handle
        return fd

    def make_writer_handle(self) -> int:
        """Get write handle to pass to another process."""
        if self._write_fd is None:
            raise RuntimeError("write_fd already taken or closed")
        fd = self._write_fd
        os.set_inheritable(fd, True)
        self._write_fd_for_close = fd
        self._write_fd = None
        if os.name == "nt":
            handle = int(msvcrt.get_osfhandle(fd))
            self._write_fd_for_close = fd
            return handle
        return fd

    def read(self, timeout_s: int = 30, chunk_size: int = 64) -> str:
        """Read data from the pipe with timeout.

        Returns data read so far if any data was received before timeout/EOF.
        Only raises an error if no data was read at all.
        """
        if self._read_fd is None:
            raise RuntimeError("read_fd already taken or closed")

        fd = self._read_fd
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
                rlist, _, _ = select.select([fd], [], [], remaining)
                if not rlist:
                    continue
                chunk = os.read(fd, chunk_size)
            if chunk:
                chunks.append(chunk)
            else:
                break

        if chunks:
            return b"".join(chunks).decode(errors="replace")
        raise RuntimeError("Timed out or EOF before any data was read")

    def write(self, data: str) -> None:
        """Write data to the pipe."""
        if self._write_fd is None:
            raise RuntimeError("write_fd already taken or closed")
        if isinstance(data, str):
            payload = data.encode()
        else:
            payload = data
        view = memoryview(payload)
        while view:
            try:
                written = os.write(self._write_fd, view)
            except InterruptedError:
                continue
            if written == 0:
                raise RuntimeError("Failed to write to pipe.")
            view = view[written:]

    def close_reader_handle(self) -> None:
        if self._read_fd_for_close is not None:
            with contextlib.suppress(OSError):
                os.close(self._read_fd_for_close)
            self._read_fd_for_close = None

    def close_writer_handle(self) -> None:
        if self._write_fd_for_close is not None:
            with contextlib.suppress(OSError):
                os.close(self._write_fd_for_close)
            self._write_fd_for_close = None

    def close(self) -> None:
        if self._read_fd is not None:
            with contextlib.suppress(OSError):
                os.close(self._read_fd)
            self._read_fd = None
        if self._write_fd is not None:
            with contextlib.suppress(OSError):
                os.close(self._write_fd)
            self._write_fd = None

        self.close_reader_handle()
        self.close_writer_handle()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    @staticmethod
    def format_handles(handles: list) -> str:
        """Format a list of handles into a comma-separated string."""
        return ",".join(str(h) for h in handles)
