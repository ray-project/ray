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

}  // namespace vm_transfer
}  // namespace ray
