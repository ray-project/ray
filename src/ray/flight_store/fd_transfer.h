#pragma once

#include <cstddef>

namespace ray {
namespace fd_transfer {

// Create an anonymous shared-memory buffer of `size` bytes and return an open
// file descriptor backing it.
//
// - Linux: memfd_create(2) — a truly anonymous, fd-backed region.
// - macOS: shm_open(2) with a uniquely generated name that is immediately
//   shm_unlink'd, so only the returned fd (and any fd passed to another process
//   via SCM_RIGHTS) keeps the memory alive. macOS has neither memfd_create nor
//   SHM_ANON, so this is the closest anonymous equivalent.
//
// The fd is truncated to `size` and can be mmap'd locally and/or handed to a
// consumer process over an AF_UNIX socket via SCM_RIGHTS. The caller owns the
// fd and must close it when done (which, together with any consumer mappings,
// determines when the backing memory is reclaimed).
//
// Returns the fd on success, or -1 on error (errno is set).
int CreateSharedBuffer(size_t size);

}  // namespace fd_transfer
}  // namespace ray
