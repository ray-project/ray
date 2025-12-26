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

#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/util/process_fd.h"
#include "ray/util/process_utils.h"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <Windows.h>
#include <Winternl.h>
#include <process.h>
#else
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/util/cmd_line_utils.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/macros.h"
#include "ray/util/subreaper.h"

namespace ray {

Process::Process() = default;

Process::Process(pid_t pid) { p_ = std::make_shared<ProcessFD>(pid); }

Process::Process(const char *argv[],
                 void *io_service,
                 std::error_code &ec,
                 bool decouple,
                 const ProcessEnvironment &env,
                 bool pipe_to_stdin,
                 std::function<void(const std::string &)> add_to_cgroup,
                 bool new_process_group) {
  /// TODO: use io_service with boost asio notify_fork.
  (void)io_service;
#ifdef __linux__
  KnownChildrenTracker::instance().AddKnownChild([&, this]() -> pid_t {
    ProcessFD procfd = ProcessFD::spawnvpe(argv,
                                           ec,
                                           decouple,
                                           env,
                                           pipe_to_stdin,
                                           std::move(add_to_cgroup),
                                           new_process_group);
    if (!ec) {
      this->p_ = std::make_shared<ProcessFD>(std::move(procfd));
    }
    return this->GetId();
  });
#else
  ProcessFD procfd = ProcessFD::spawnvpe(argv,
                                         ec,
                                         decouple,
                                         env,
                                         pipe_to_stdin,
                                         std::move(add_to_cgroup),
                                         new_process_group);
  if (!ec) {
    p_ = std::make_shared<ProcessFD>(std::move(procfd));
  }
#endif
}

std::string Process::Exec(const std::string command) {
  /// Based on answer in
  /// https://stackoverflow.com/questions/478898/how-do-i-execute-a-command-and-get-the-output-of-the-command-within-c-using-po
  std::array<char, 128> buffer;
  std::string result;
#ifdef _WIN32
  std::unique_ptr<FILE, decltype(&_pclose)> pipe(_popen(command.c_str(), "r"), _pclose);
#else
  std::unique_ptr<FILE, int (*)(FILE *)> pipe(popen(command.c_str(), "r"), pclose);
#endif
  RAY_CHECK(pipe != nullptr) << "popen() failed for command: " << command;
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

StatusOr<std::unique_ptr<ProcessInterface>> Process::Spawn(
    const std::vector<std::string> &args,
    bool decouple,
    const std::string &pid_file,
    const ProcessEnvironment &env,
    bool new_process_group) {
  std::vector<const char *> argv;
  argv.reserve(args.size() + 1);
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code error;
  std::unique_ptr<ProcessInterface> proc = std::make_unique<Process>(
      &*argv.begin(),
      nullptr,
      error,
      decouple,
      env,
      /*pipe_to_stdin=*/false,
      /*add_to_cgroup*/ [](const std::string &) {},
      new_process_group);
  if (!error && !pid_file.empty()) {
    std::ofstream file(pid_file, std::ios_base::out | std::ios_base::trunc);
    file << proc->GetId() << std::endl;
    RAY_CHECK(file.good());
  }

  if (error) {
    return Status::FromError(error);
  }
  return std::move(proc);
}

std::error_code Process::Call(const std::vector<std::string> &args,
                              const ProcessEnvironment &env) {
  std::vector<const char *> argv;
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code ec;
  Process proc(&*argv.begin(), nullptr, ec, true, env);
  if (!ec) {
    int return_code = proc.Wait();
    if (return_code != 0) {
      ec = std::error_code(return_code, std::system_category());
    }
  }
  return ec;
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
      // There are 3 possible cases:
      // - The process is a child whose death we await via waitpid().
      //   This is the usual case, when we have a child whose SIGCHLD we handle.
      // - The process shares a pipe with us whose closure we use to detect its death.
      //   This is used to track a non-owned process, like a grandchild.
      // - The process has no relationship with us, in which case we simply fail,
      //   since we have no need for this (and there's no good way to do it).
      // Why don't we just poll the PID? Because it's better not to:
      // - It would be prone to a race condition (we won't know when the PID is recycled).
      // - It would incur high latency and/or high CPU usage for the caller.
      if (fd != -1) {
        // We have a pipe, so wait for its other end to close, to detect process death.
        unsigned char buf[1 << 8];
        ptrdiff_t r;
        while ((r = read(fd, buf, sizeof(buf))) > 0) {
          // Keep reading until socket terminates
        }
        status = r == -1 ? -1 : 0;
      } else if (waitpid(pid, &status, 0) == -1) {
        // Just the normal waitpid() case.
        // (We can only do this once, only if we own the process. It fails otherwise.)
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

bool Process::IsAlive() const {
  if (p_) {
    return IsProcessAlive(p_->GetId());
  }
  return false;
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
        if (error.value() == ERROR_ACCESS_DENIED) {
          // This can occur in some situations if the process is already terminating.
          DWORD exit_code;
          if (GetExitCodeProcess(handle, &exit_code) && exit_code != STILL_ACTIVE) {
            // The process is already terminating, so consider the killing successful.
            error = std::error_code();
          }
        }
      }
#else
      pollfd pfd = {static_cast<int>(fd), POLLHUP};
      if (fd != -1 && poll(&pfd, 1, 0) == 1 && (pfd.revents & POLLHUP)) {
        // The process has already died; don't attempt to kill its PID again.
      } else if (kill(pid, SIGKILL) != 0) {
        error = std::error_code(errno, std::system_category());
      }
      if (error.value() == ESRCH) {
        // The process died before our kill().
        // This is probably due to using FromPid().Kill() on a non-owned process.
        // We got lucky here, because we could've killed a recycled PID.
        // To avoid this, do not kill a process that is not owned by us.
        // Instead, let its parent receive its SIGCHLD normally and call waitpid() on it.
        // (Exception: Tests might occasionally trigger this, but that should be benign.)
      }
#endif
      if (error) {
        RAY_LOG(DEBUG) << "Failed to kill process " << pid << " with error " << error
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
  if (x.IsNull() || y.IsNull()) {
    return x.IsNull() && y.IsNull();
  }
  if (x.IsValid() != y.IsValid()) {
    return false;
  }

  if (x.IsValid()) {
    return equal_to<>()(x.GetId(), y.GetId());
  }
  // Else, both are invalid, compare by address
  return equal_to<>()(x.Get(), y.Get());
}

size_t hash<ray::Process>::operator()(const ray::Process &value) const {
  if (value.IsNull()) {
    // Null process hash to 0
    return 0;
  } else if (value.IsValid()) {
    // Valid process hash by PID
    return std::hash<pid_t>()(value.GetId());
  }
  // Invalid process hash by address
  return std::hash<void const *>()(value.Get());
}

}  // namespace std
