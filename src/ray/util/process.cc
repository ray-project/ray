// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/util/process.h"

#ifdef _WIN32
#include <process.h>
#else
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#endif
#include <unistd.h>

#include <algorithm>
#include <string>
#include <vector>

#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {

class ProcessFD {
  pid_t pid_;
  intptr_t fd_;

 public:
  ~ProcessFD();
  ProcessFD();
  ProcessFD(pid_t pid, intptr_t fd = -1);
  ProcessFD(const ProcessFD &other);
  ProcessFD(ProcessFD &&other);
  ProcessFD &operator=(const ProcessFD &other);
  ProcessFD &operator=(ProcessFD &&other);
  intptr_t CloneFD() const;
  void CloseFD();
  intptr_t GetFD() const;
  pid_t GetId() const;

  // Fork + exec combo. Returns -1 for the PID on failure.
  static ProcessFD spawnvp(const char *argv[], std::error_code &ec) {
    ec = std::error_code();
    intptr_t fd;
    pid_t pid;
#ifdef _WIN32
    std::vector<std::string> args;
    for (size_t i = 0; argv[i]; ++i) {
      args.push_back(argv[i]);
    }
    // Calling CreateCommandLine() here wouldn't make sense here if the
    // Microsoft C runtime properly quoted each command-argument argument.
    // However, it doesn't quote at all. It just joins arguments with a space.
    // So we have to do the quoting manually and pass everything as a single argument.
    fd = _spawnlp(P_NOWAIT, args[0].c_str(), CreateCommandLine(args).c_str(), NULL);
    if (fd != -1) {
      pid = static_cast<pid_t>(GetProcessId(reinterpret_cast<HANDLE>(fd)));
      if (pid == 0) {
        pid = -1;
      }
    } else {
      pid = -1;
    }
    if (pid == -1) {
      ec = std::error_code(GetLastError(), std::system_category());
    }
#else
    // TODO(mehrdadn): Use clone() on Linux or posix_spawnp() on Mac to avoid duplicating
    // file descriptors into the child process, as that can be problematic.
    pid = fork();
    if (pid == 0) {
      // Child process case. Reset the SIGCHLD handler for the worker.
      signal(SIGCHLD, SIG_DFL);
      if (execvp(argv[0], const_cast<char *const *>(argv)) == -1) {
        pid = -1;
        abort();  // fork() succeeded but exec() failed, so abort the child
      }
    }
    if (pid == -1) {
      ec = std::error_code(errno, std::system_category());
    }
    // TODO(mehrdadn): This would be a good place to open a descriptor later
    fd = -1;
#endif
    return ProcessFD(pid, fd);
  }
};

ProcessFD::~ProcessFD() {
  if (fd_ != -1) {
#ifdef _WIN32
    CloseHandle(reinterpret_cast<HANDLE>(fd_));
#else
    close(static_cast<int>(fd_));
#endif
  }
}

ProcessFD::ProcessFD() : pid_(-1), fd_(-1) {}

ProcessFD::ProcessFD(pid_t pid, intptr_t fd) : pid_(pid), fd_(fd) {
  if (pid != -1) {
    bool process_does_not_exist = false;
    std::error_code error;
#ifdef _WIN32
    if (fd == -1) {
      BOOL inheritable = FALSE;
      DWORD permissions = MAXIMUM_ALLOWED;
      HANDLE handle = OpenProcess(permissions, inheritable, static_cast<DWORD>(pid));
      if (handle) {
        fd_ = reinterpret_cast<intptr_t>(handle);
      } else {
        DWORD error_code = GetLastError();
        error = std::error_code(error_code, std::system_category());
        if (error_code == ERROR_INVALID_PARAMETER) {
          process_does_not_exist = true;
        }
      }
    } else {
      RAY_CHECK(pid == GetProcessId(reinterpret_cast<HANDLE>(fd)));
    }
#else
    if (kill(pid, 0) == -1 && errno == ESRCH) {
      process_does_not_exist = true;
    }
#endif
    // Don't verify anything if the PID is too high, since that's used for testing
    if (pid < PID_MAX_LIMIT) {
      if (process_does_not_exist) {
        // NOTE: This indicates a race condition where a process died and its process
        // table entry was removed before the ProcessFD could be instantiated. For
        // processes owned by this process, we should make this impossible by keeping
        // the SIGCHLD signal. For processes not owned by this process, we need to come up
        // with a strategy to create this class in a way that avoids race conditions.
        RAY_LOG(ERROR) << "Process " << pid << " does not exist.";
      }
      if (error) {
        // TODO(mehrdadn): Should this be fatal, or perhaps returned as an error code?
        // Failures might occur due to reasons such as permission issues.
        RAY_LOG(ERROR) << "error " << error << " opening process " << pid << ": "
                       << error.message();
      }
    }
  }
}

ProcessFD::ProcessFD(const ProcessFD &other) : ProcessFD(other.pid_, other.CloneFD()) {}

ProcessFD::ProcessFD(ProcessFD &&other) : ProcessFD() { *this = std::move(other); }

ProcessFD &ProcessFD::operator=(const ProcessFD &other) {
  if (this != &other) {
    // Construct a copy, then call the move constructor
    *this = static_cast<ProcessFD>(other);
  }
  return *this;
}

ProcessFD &ProcessFD::operator=(ProcessFD &&other) {
  if (this != &other) {
    // We use swap() to make sure the argument is actually moved from
    using std::swap;
    swap(pid_, other.pid_);
    swap(fd_, other.fd_);
  }
  return *this;
}

intptr_t ProcessFD::CloneFD() const {
  intptr_t fd;
  if (fd_ != -1) {
#ifdef _WIN32
    HANDLE handle;
    BOOL inheritable = FALSE;
    fd = DuplicateHandle(GetCurrentProcess(), reinterpret_cast<HANDLE>(fd_),
                         GetCurrentProcess(), &handle, 0, inheritable,
                         DUPLICATE_SAME_ACCESS)
             ? reinterpret_cast<intptr_t>(handle)
             : -1;
#else
    fd = dup(static_cast<int>(fd_));
#endif
    RAY_DCHECK(fd != -1);
  } else {
    fd = -1;
  }
  return fd;
}

void ProcessFD::CloseFD() { fd_ = -1; }

intptr_t ProcessFD::GetFD() const { return fd_; }

pid_t ProcessFD::GetId() const { return pid_; }

Process::~Process() {}

Process::Process() {}

Process::Process(const Process &) = default;

Process::Process(Process &&) = default;

Process &Process::operator=(Process other) {
  p_ = std::move(other.p_);
  return *this;
}

Process::Process(pid_t pid) { p_ = std::make_shared<ProcessFD>(pid); }

Process::Process(const char *argv[], void *io_service, std::error_code &ec) {
  (void)io_service;
  ProcessFD procfd = ProcessFD::spawnvp(argv, ec);
  if (!ec) {
    p_ = std::make_shared<ProcessFD>(std::move(procfd));
  }
}

Process Process::CreateNewDummy() {
  pid_t pid = -1;
  Process result(pid);
  return result;
}

Process Process::FromPid(pid_t pid) {
  RAY_DCHECK(pid >= 0);
  Process result(pid);
  return result;
}

const void *Process::Get() const { return p_ ? &*p_ : NULL; }

pid_t Process::GetId() const { return p_ ? p_->GetId() : -1; }

bool Process::IsNull() const { return !p_; }

bool Process::IsValid() const { return GetId() != -1; }

int Process::Wait() const {
  int status;
  if (p_) {
    pid_t pid = p_->GetId();
    if (pid >= 0) {
      std::error_code error;
      intptr_t fd = p_->GetFD();
#ifdef _WIN32
      HANDLE handle = fd != -1 ? reinterpret_cast<HANDLE>(fd) : NULL;
      DWORD exit_code = STILL_ACTIVE;
      if (WaitForSingleObject(handle, INFINITE) == WAIT_OBJECT_0 &&
          GetExitCodeProcess(handle, &exit_code)) {
        status = static_cast<int>(exit_code);
      } else {
        error = std::error_code(GetLastError(), std::system_category());
        status = -1;
      }
#else
      (void)fd;
      if (waitpid(pid, &status, 0) != 0) {
        error = std::error_code(errno, std::system_category());
      }
#endif
      if (error) {
        RAY_LOG(ERROR) << "Failed to wait for process " << pid << " with error " << error
                       << ": " << error.message();
      }
    } else {
      // (Dummy process case)
      status = 0;
    }
  } else {
    // (Null process case)
    status = -1;
  }
  return status;
}

void Process::Kill() {
  if (p_) {
    pid_t pid = p_->GetId();
    if (pid >= 0) {
      std::error_code error;
      intptr_t fd = p_->GetFD();
#ifdef _WIN32
      HANDLE handle = fd != -1 ? reinterpret_cast<HANDLE>(fd) : NULL;
      if (!::TerminateProcess(handle, ERROR_PROCESS_ABORTED)) {
        error = std::error_code(GetLastError(), std::system_category());
      }
#else
      (void)fd;
      if (kill(pid, SIGKILL) != 0) {
        error = std::error_code(errno, std::system_category());
      }
#endif
      if (error) {
        RAY_LOG(ERROR) << "Failed to kill process " << pid << " with error " << error
                       << ": " << error.message();
      }
    } else {
      // (Dummy process case)
      // Theoretically we could keep around an exit code here for Wait() to return,
      // but we might as well pretend this fake process had already finished running.
      // So don't bother doing anything.
    }
  } else {
    // (Null process case)
  }
}

}  // namespace ray

namespace std {

bool equal_to<ray::Process>::operator()(const ray::Process &x,
                                        const ray::Process &y) const {
  return !x.IsNull()
             ? !y.IsNull()
                   ? x.IsValid()
                         ? y.IsValid() ? equal_to<pid_t>()(x.GetId(), y.GetId()) : false
                         : y.IsValid() ? false
                                       : equal_to<void const *>()(x.Get(), y.Get())
                   : false
             : y.IsNull();
}

size_t hash<ray::Process>::operator()(const ray::Process &value) const {
  return !value.IsNull() ? value.IsValid() ? hash<pid_t>()(value.GetId())
                                           : hash<void const *>()(value.Get())
                         : size_t();
}

}  // namespace std
