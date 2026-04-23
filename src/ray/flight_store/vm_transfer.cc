#include "ray/flight_store/vm_transfer.h"

#include <cerrno>
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

}  // namespace vm_transfer
}  // namespace ray
