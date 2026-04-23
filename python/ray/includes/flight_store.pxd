# distutils: language = c++

from libc.stdint cimport uintptr_t
from libcpp cimport bool as c_bool

cdef extern from "sys/types.h" nogil:
    ctypedef int pid_t
    ctypedef long ssize_t

cdef extern from "ray/flight_store/vm_transfer.h" \
        namespace "ray::vm_transfer" nogil:
    ssize_t ReadFromRemoteProcess(pid_t remote_pid,
                                   void *local_buf,
                                   uintptr_t remote_addr,
                                   size_t size)
    ssize_t WriteToRemoteProcess(pid_t remote_pid,
                                  const void *local_buf,
                                  uintptr_t remote_addr,
                                  size_t size)
    ssize_t ScatterWriteToRemoteProcess(pid_t remote_pid,
                                         const uintptr_t *local_addrs,
                                         const size_t *local_sizes,
                                         size_t num_bufs,
                                         uintptr_t remote_addr,
                                         size_t remote_size)
