#include "ray/util/process.h"

#include <unistd.h>

#include <algorithm>
#include <boost/process/args.hpp>
#include <boost/process/async.hpp>
#include <boost/process/child.hpp>
#include <boost/process/search_path.hpp>
#include <string>
#include <vector>

#include "ray/util/logging.h"

namespace bp = boost::process;

namespace ray {

class Process::ProcessFD {
  // This class makes a best-effort attempt to keep a PID alive.
  // However, it cannot make any guarantees.
  // The kernel might not even support this mechanism.
  // See here: https://unix.stackexchange.com/a/181249
  int fd_;

 public:
  ~ProcessFD();
  ProcessFD(pid_t pid);
  ProcessFD(ProcessFD &&other);
  ProcessFD(const ProcessFD &other);
  ProcessFD &operator=(ProcessFD other);
};

Process::ProcessFD::~ProcessFD() {
  if (fd_ != -1) {
#ifdef __linux__
    ::close(fd_);
#endif
  }
}
Process::ProcessFD::ProcessFD(pid_t pid) : fd_(-1) {
  if (pid != -1) {
#ifdef __linux__
    char path[64];
    sprintf(path, "/proc/%d/ns/pid", static_cast<int>(pid));
    fd_ = ::open(path, O_RDONLY);
#endif
  }
}
Process::ProcessFD::ProcessFD(ProcessFD &&other) : fd_(std::move(other.fd_)) {
  other.fd_ = -1;
}
Process::ProcessFD::ProcessFD(const ProcessFD &other) : fd_(-1) {
  if (other.fd_ != -1) {
#ifdef __linux__
    fd_ = ::dup(other.fd_);
#endif
  }
}
Process::ProcessFD &Process::ProcessFD::operator=(ProcessFD other) {
  using std::swap;
  swap(fd_, other.fd_);
  return *this;
}

Process::~Process() {}

Process::Process() {}

Process::Process(const Process &) = default;

Process::Process(Process &&) = default;

Process &Process::operator=(Process other) {
  p_ = std::move(other.p_);
  return *this;
}

Process::Process(pid_t &pid) {
  bp::child proc(pid);
  proc.detach();
  p_ = std::make_shared<std::pair<ProcessFD, boost::process::child>>(ProcessFD(pid),
                                                                     std::move(proc));
}

Process::Process(const char *argv[], boost::asio::io_service *io_service,
                 const std::function<void(int, const std::error_code &)> &on_exit,
                 std::error_code &ec) {
  bp::child proc;
  std::vector<std::string> args;
  for (size_t i = 0; argv[i]; ++i) {
    args.push_back(argv[i]);
  }
  if (io_service) {
    if (on_exit) {
      proc = bp::child(args, ec, *io_service, bp::on_exit = on_exit);
    } else {
      proc = bp::child(args, ec, *io_service);
    }
  } else {
    if (on_exit) {
      proc = bp::child(args, ec, bp::on_exit = on_exit);
    } else {
      proc = bp::child(args, ec);
    }
  }
  if (!ec) {
    p_ = std::make_shared<std::pair<ProcessFD, boost::process::child>>(
        ProcessFD(proc.id()), std::move(proc));
  }
}

void Process::Detach() {
  if (p_) {
    p_->second.detach();
  }
}

Process Process::CreateNewDummy() {
  pid_t pid = -1;
  Process result(pid);
  return result;
}

Process Process::FromPid(pid_t pid) {
  RAY_CHECK(pid >= 0);
  Process result(pid);
  return result;
}

bp::child *Process::Get() const { return p_ ? &p_->second : NULL; }

pid_t Process::GetId() const { return p_ ? p_->second.id() : -1; }

bool Process::IsNull() const { return !p_; }

bool Process::IsRunning() const { return p_ && p_->second.running(); }

bool Process::IsValid() const { return p_ && p_->second.valid(); }

void Process::Kill() {
  if (p_) {
    p_->second.terminate();
  }
}

void Process::Join() {
  if (p_) {
    p_->second.join();
  }
}

}  // namespace ray

namespace std {

bool equal_to<ray::Process>::operator()(const ray::Process &x,
                                        const ray::Process &y) const {
  const bp::child *a = x.Get(), *b = y.Get();
  return a ? b ? a->valid() ? b->valid() ? equal_to<pid_t>()(a->id(), b->id()) : false
                            : b->valid() ? false : equal_to<void const *>()(a, b)
               : false
           : !b;
}

size_t hash<ray::Process>::operator()(const ray::Process &value) const {
  const bp::child *p = value.Get();
  return p ? p->valid() ? hash<pid_t>()(p->id()) : hash<void const *>()(p) : size_t();
}

}  // namespace std
