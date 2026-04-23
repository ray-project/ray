#pragma once

#include <cstddef>
#include <cstdint>
#include <sys/types.h>

namespace ray {
namespace vm_transfer {

// Read `size` bytes from remote process's address space into `local_buf`.
// Returns bytes read, or -1 on error (check errno).
ssize_t ReadFromRemoteProcess(pid_t remote_pid,
                               void *local_buf,
                               uintptr_t remote_addr,
                               size_t size);

// Write `size` bytes from `local_buf` into remote process's address space.
// Returns bytes written, or -1 on error (check errno).
ssize_t WriteToRemoteProcess(pid_t remote_pid,
                              const void *local_buf,
                              uintptr_t remote_addr,
                              size_t size);

// Scatter-gather write: write multiple local buffers into a contiguous
// remote buffer in a single process_vm_writev syscall.
// `local_addrs` and `local_sizes` are arrays of length `num_bufs`.
// Returns total bytes written, or -1 on error.
ssize_t ScatterWriteToRemoteProcess(pid_t remote_pid,
                                     const uintptr_t *local_addrs,
                                     const size_t *local_sizes,
                                     size_t num_bufs,
                                     uintptr_t remote_addr,
                                     size_t remote_size);

}  // namespace vm_transfer
}  // namespace ray
