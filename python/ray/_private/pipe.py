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
    """Anonymous pipe for a parent process to get a short payload from its children."""

    def __init__(self):
        self._read_fd, self._write_fd = os.pipe()
        os.set_inheritable(self._write_fd, True)

    @property
    def read_fd(self) -> Optional[int]:
        return self._read_fd

    @property
    def write_fd(self) -> Optional[int]:
        return self._write_fd

    @property
    def write_handle(self) -> Optional[int]:
        """Return the OS handle to pass across children (fd on POSIX, HANDLE on Windows)."""
        if self._write_fd is None:
            return None
        if os.name == "nt":
            return int(msvcrt.get_osfhandle(self._write_fd))
        return self._write_fd

    def close_write(self) -> None:
        if self._write_fd is not None:
            with contextlib.suppress(OSError):
                os.close(self._write_fd)
            self._write_fd = None

    def close_read(self) -> None:
        if self._read_fd is not None:
            with contextlib.suppress(OSError):
                os.close(self._read_fd)
            self._read_fd = None

    def read(self, timeout_s: int = 30, chunk_size: int = 64) -> str:
        assert (
            self._read_fd is not None
        ), "Attempting to read from a closed pipe read end."

        deadline = time.monotonic() + timeout_s
        chunks = []
        while time.monotonic() < deadline:
            # time may advance between loop check and computing remaining
            remaining = max(0.0, deadline - time.monotonic())
            if os.name == "nt":
                available = _windows_peek_available(self._read_fd)
                if available < 0:
                    chunk = b""
                elif available == 0:
                    time.sleep(0.05)
                    continue
                else:
                    chunk = os.read(self._read_fd, min(chunk_size, available))
            else:
                rlist, _, _ = select.select([self._read_fd], [], [], remaining)
                if not rlist:
                    continue
                chunk = os.read(self._read_fd, chunk_size)
            if chunk:
                chunks.append(chunk)
            else:
                # Writer closed.
                if chunks:
                    return b"".join(chunks).decode(errors="replace")
                raise RuntimeError("EOF before any data was read")

        raise RuntimeError("Timed out waiting for data from pipe.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close_read()
        self.close_write()
