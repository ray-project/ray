# Arrow Flight object store — included in _raylet.pyx.
#
# C++ layer: just process_vm_readv / process_vm_writev syscalls.
# Python layer: PyArrow Flight server for table storage + transfer.

from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
from libc.errno cimport errno
from libc.stdint cimport uintptr_t, int64_t
from posix.types cimport pid_t

import os as _flight_os
import sys as _flight_sys


def vm_read(int remote_pid, unsigned long remote_addr, long long size):
    """Read bytes from a remote process via process_vm_readv (Linux only).

    Returns a Python bytes object with the data.
    """
    cdef size_t c_size = <size_t>size
    cdef pid_t c_pid = <pid_t>remote_pid
    cdef uintptr_t c_addr = <uintptr_t>remote_addr
    cdef void *buf = malloc(c_size)
    if buf == NULL:
        raise MemoryError(f"Failed to allocate {size} bytes")
    cdef ssize_t nread
    try:
        with nogil:
            nread = ReadFromRemoteProcess(c_pid, buf, c_addr, c_size)
        if nread < 0:
            raise OSError(
                errno,
                f"process_vm_readv failed (pid={remote_pid}, "
                f"addr=0x{remote_addr:x}, size={size})")
        if nread != <ssize_t>c_size:
            raise OSError(
                0,
                f"process_vm_readv partial read: {nread} of {size} bytes")
        return (<char *>buf)[:c_size]
    finally:
        free(buf)


def vm_read_into_arrow_buffer(int remote_pid, unsigned long remote_addr,
                              long long size):
    """Read from a remote process directly into a PyArrow buffer (Linux only).

    Returns a pyarrow.Buffer. Single copy: process_vm_readv writes directly
    into the Arrow-allocated memory. The returned buffer can be passed to
    ipc.open_stream() without any additional copy.
    """
    import pyarrow as _pa

    # Allocate a mutable Arrow buffer.
    arrow_buf = _pa.allocate_buffer(size, resizable=False)
    cdef size_t c_size = <size_t>size
    cdef pid_t c_pid = <pid_t>remote_pid
    cdef uintptr_t c_addr = <uintptr_t>remote_addr
    # Get the raw pointer from the Arrow buffer.
    cdef uintptr_t buf_addr = arrow_buf.address
    cdef ssize_t nread
    with nogil:
        nread = ReadFromRemoteProcess(
            c_pid, <void *>buf_addr, c_addr, c_size)
    if nread < 0:
        raise OSError(
            errno,
            f"process_vm_readv failed (pid={remote_pid}, "
            f"addr=0x{remote_addr:x}, size={size})")
    if nread != <ssize_t>c_size:
        raise OSError(
            0,
            f"process_vm_readv partial read: {nread} of {size} bytes")
    return arrow_buf


def vm_write(int remote_pid, unsigned long remote_addr, bytes data):
    """Write bytes to a remote process via process_vm_writev (Linux only).

    Returns number of bytes written.
    """
    cdef const char *buf = data
    cdef size_t c_size = len(data)
    cdef pid_t c_pid = <pid_t>remote_pid
    cdef uintptr_t c_addr = <uintptr_t>remote_addr
    cdef ssize_t nwritten
    with nogil:
        nwritten = WriteToRemoteProcess(c_pid, buf, c_addr, c_size)
    if nwritten < 0:
        raise OSError(
            errno,
            f"process_vm_writev failed (pid={remote_pid}, "
            f"addr=0x{remote_addr:x}, size={c_size})")
    return nwritten


def vm_write_into_buffer(int remote_pid, unsigned long remote_addr,
                         long long size, bytes data):
    """Write IPC bytes into a pre-allocated buffer in a remote process.

    The remote process must have allocated `size` bytes at `remote_addr`
    and be waiting for the write to complete.
    """
    if len(data) > size:
        raise ValueError(
            f"Data size {len(data)} exceeds buffer size {size}")
    return vm_write(remote_pid, remote_addr, data)
