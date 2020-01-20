#ifndef RAY_UTIL_PROCESS_H
#define RAY_UTIL_PROCESS_H

#ifdef __linux__
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <boost/asio/io_service.hpp>
#include <boost/process/args.hpp>
#include <boost/process/child.hpp>
#include <functional>
#include <memory>
#include <utility>

// We only define operators required by the standard library (==, hash).
// We declare but avoid defining the rest so that they're not used by accident.

namespace ray {

typedef boost::process::pid_t pid_t;

class Process : public boost::process::child {
 protected:
  class ProcessFD {
    // This class makes a best-effort attempt to keep a PID alive.
    // However, it cannot make any guarantees.
    // The kernel might not even support this mechanism.
    // See here: https://unix.stackexchange.com/a/181249
#ifdef __linux__
    int fd_;
#endif
   public:
#ifdef __linux__
    ~ProcessFD() {
      if (fd_ != -1) {
        ::close(fd_);
      }
    }
    ProcessFD(pid_t pid) : fd_(-1) {
      if (pid != -1) {
        char path[64];
        sprintf(path, "/proc/%d/ns/pid", static_cast<int>(pid));
        fd_ = ::open(path, O_RDONLY);
      }
    }
    ProcessFD(ProcessFD &&other) : fd_(std::move(other.fd_)) { other.fd_ = -1; }
    ProcessFD(const ProcessFD &other) : fd_(other.fd_ != -1 ? ::dup(other.fd_) : -1) {}
    ProcessFD &operator=(ProcessFD other) {
      using std::swap;
      swap(fd_, other.fd_);
      return *this;
    }
#else
    ProcessFD(pid_t) {}
#endif
  };
  ProcessFD fd_;

 public:
  template <typename... T>
  explicit Process(T &&... args)
      : boost::process::child(std::forward<T>(args)...),
        fd_(boost::process::child::id()) {}
};

/// A managed equivalent of a pid_t (to manage the lifetime of each process).
/// TODO(mehrdadn): This hasn't been a great design, but we play along to
/// minimize the changes needed for Windows compatibility.
/// (We used to represent a worker process by just its pid_t, which carries
/// no ownership/lifetime information.)
/// Once this code is running properly, refactor the data structures to create
/// a better ownership structure between the worker processes and the workers.
class ProcessHandle {
  std::shared_ptr<Process> proc_;

 public:
  ProcessHandle(const std::shared_ptr<Process> &proc = std::shared_ptr<Process>())
      : proc_(proc) {}
  Process *get() const { return proc_.get(); }
  explicit operator bool() const { return !!proc_; }
  static ProcessHandle FromPid(pid_t pid) {
    Process temp(pid);
    temp.detach();
    return std::make_shared<Process>(std::move(temp));
  }
};

}  // namespace ray

// Define comparators for process handles:
// -   Valid process objects must be distinguished by their IDs.
// - Invalid process objects must be distinguished by their addresses.
namespace std {

template <>
struct equal_to<ray::ProcessHandle> {
  bool operator()(const ray::ProcessHandle &x, const ray::ProcessHandle &y) const {
    const ray::Process *a = x.get(), *b = y.get();
    // See explanation above
    return a ? b ? a->valid()
                       ? b->valid() ? equal_to<ray::pid_t>()(a->id(), b->id()) : false
                       : b->valid() ? false : equal_to<void const *>()(a, b)
                 : false
             : !b;
  }
};

template <>
struct hash<ray::ProcessHandle> {
  size_t operator()(const ray::ProcessHandle &value) const {
    const ray::Process *p = value.get();
    // See explanation above
    return p ? p->valid() ? hash<ray::pid_t>()(p->id()) : hash<void const *>()(p)
             : size_t();
  }
};

}  // namespace std

#endif
