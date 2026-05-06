#include "ray/flight_store/vm_transfer.h"

#include <cerrno>
#include <vector>
#ifdef __linux__
#include <sys/uio.h>
#endif

namespace ray {
namespace vm_transfer {

ssize_t ReadFromRemoteProcess(pid_t remote_pid,
                              void *local_buf,
                              uintptr_t remote_addr,
                              size_t size) {
#ifdef __linux__
  struct iovec local_iov = {local_buf, size};
  struct iovec remote_iov = {reinterpret_cast<void *>(remote_addr), size};
  return process_vm_readv(remote_pid, &local_iov, 1, &remote_iov, 1, 0);
#else
  (void)remote_pid;
  (void)local_buf;
  (void)remote_addr;
  (void)size;
  errno = ENOSYS;
  return -1;
#endif
}

ssize_t WriteToRemoteProcess(pid_t remote_pid,
                             const void *local_buf,
                             uintptr_t remote_addr,
                             size_t size) {
#ifdef __linux__
  struct iovec local_iov = {const_cast<void *>(local_buf), size};
  struct iovec remote_iov = {reinterpret_cast<void *>(remote_addr), size};
  return process_vm_writev(remote_pid, &local_iov, 1, &remote_iov, 1, 0);
#else
  (void)remote_pid;
  (void)local_buf;
  (void)remote_addr;
  (void)size;
  errno = ENOSYS;
  return -1;
#endif
}

ssize_t ScatterWriteToRemoteProcess(pid_t remote_pid,
                                    const uintptr_t *local_addrs,
                                    const size_t *local_sizes,
                                    size_t num_bufs,
                                    uintptr_t remote_addr,
                                    size_t remote_size) {
#ifdef __linux__
  // Build local iovec array from the scatter list.
  std::vector<struct iovec> local_iovs(num_bufs);
  for (size_t i = 0; i < num_bufs; ++i) {
    local_iovs[i].iov_base = reinterpret_cast<void *>(local_addrs[i]);
    local_iovs[i].iov_len = local_sizes[i];
  }
  // Single contiguous remote destination.
  struct iovec remote_iov = {reinterpret_cast<void *>(remote_addr), remote_size};
  return process_vm_writev(remote_pid, local_iovs.data(), num_bufs, &remote_iov, 1, 0);
#else
  (void)remote_pid;
  (void)local_addrs;
  (void)local_sizes;
  (void)num_bufs;
  (void)remote_addr;
  (void)remote_size;
  errno = ENOSYS;
  return -1;
#endif
}

}  // namespace vm_transfer
}  // namespace ray
