// Copyright 2026 The Ray Authors.
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
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"
#include "ray/util/cmd_line_utils.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/macros.h"
#include "ray/util/subreaper.h"

#ifdef __APPLE__
extern char **environ;

// macOS doesn't come with execvpe.
// https://stackoverflow.com/questions/7789750/execve-with-path-search
int execvpe(const char *program, char *const argv[], char *const envp[]) {
  char **saved = environ;
  int rc;
  // Mutating environ is generally unsafe, but this logic only runs on the
  // start of a worker process. There should be no concurrent access to the
  // environment.
  environ = const_cast<char **>(envp);
  rc = execvp(program, argv);
  environ = saved;
  return rc;
}
#endif

namespace ray {

Process::~Process() {
  if (fd_ != -1) {
    bool success;
#ifdef _WIN32
    success = !!CloseHandle(reinterpret_cast<HANDLE>(fd_));
#else
    success = close(static_cast<int>(fd_)) == 0;
#endif
    RAY_CHECK(success) << absl::StrFormat(
        "error %s closing process %d FD", strerror(errno), pid_);
  }

  fd_ = -1;
}

Process::Process() : pid_(-1), fd_(-1) {}

Process::Process(pid_t pid) : pid_(pid), fd_(-1) {
  if (pid_ != -1) {
    bool process_does_not_exist = false;
    std::error_code error;
#ifdef _WIN32
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
#else
    if (kill(pid, 0) == -1 && errno == ESRCH) {
      process_does_not_exist = true;
    }
#endif
    // Don't verify anything if the PID is too high, since that's used for testing
    if (pid < PID_MAX_LIMIT) {
      if (process_does_not_exist) {
        // NOTE: This indicates a race condition where a process died and its process
        // table entry was removed before the Process could be instantiated. For
        // processes owned by this process, we should make this impossible by keeping
        // the SIGCHLD signal. For processes not owned by this process, we need to come up
        // with a strategy to create this class in a way that avoids race conditions.
        RAY_LOG(ERROR) << absl::StrFormat("Process %d does not exist.", pid);
      }
      if (error) {
        // TODO(mehrdadn): Should this be fatal, or perhaps returned as an error code?
        // Failures might occur due to reasons such as permission issues.
        RAY_LOG(ERROR) << absl::StrFormat(
            "error %d opening process %d: %s", error.value(), pid, error.message());
      }
    }
  }
}

Process::Process(const char *argv[],
                 std::error_code &ec,
                 bool decouple,
                 const ProcessEnvironment &env,
                 bool pipe_to_stdin,
                 std::function<void(const std::string &)> add_to_cgroup,
                 bool new_process_group) {
#ifdef __linux__
  KnownChildrenTracker::instance().AddKnownChild([&, this]() -> pid_t {
    std::pair<pid_t, intptr_t> result = Spawnvpe(
        argv, ec, decouple, env, pipe_to_stdin, add_to_cgroup, new_process_group);
    if (!ec) {
      pid_ = result.first;
      fd_ = result.second;
    }
    return this->GetId();
  });
#else
  auto result =
      Spawnvpe(argv, ec, decouple, env, pipe_to_stdin, add_to_cgroup, new_process_group);
  if (!ec) {
    pid_ = result.first;
    fd_ = result.second;
  }
#endif
}

Process::Process(Process &&other) : Process() { *this = std::move(other); }

Process &Process::operator=(Process &&other) {
  if (this != &other) {
    std::swap(pid_, other.pid_);
    std::swap(fd_, other.fd_);
  }
  return *this;
}

std::pair<pid_t, intptr_t> Process::Spawnvpe(const char *argv[],
                                             std::error_code &ec,
                                             bool decouple,
                                             const ProcessEnvironment &env,
                                             bool pipe_to_stdin,
                                             AddProcessToCgroupHook add_to_cgroup,
                                             bool new_process_group) const {
  intptr_t fd;
  pid_t pid;
  ec = std::error_code();
  ProcessEnvironment new_env;
  for (char *const *e = environ; *e; ++e) {
    RAY_CHECK(*e && **e != '\0') << "environment variable name is absent";
    const char *key_end = strchr(*e + 1 /* +1 is needed for Windows */, '=');
    RAY_CHECK(key_end) << "environment variable value is absent: " << e;
    new_env[std::string(*e, static_cast<size_t>(key_end - *e))] = key_end + 1;
  }
  for (const auto &item : env) {
    new_env[item.first] = item.second;
  }
  std::string new_env_block;
  for (const auto &item : new_env) {
    new_env_block += item.first + '=' + item.second + '\0';
  }
#ifdef _WIN32

  (void)decouple;  // Windows doesn't require anything particular for decoupling.
  std::vector<std::string> args;
  for (size_t i = 0; argv[i]; ++i) {
    args.push_back(argv[i]);
  }
  std::string cmds[] = {std::string(), CreateCommandLine(args)};
  if (GetFileName(args.at(0)).find('.') == std::string::npos) {
    // Some executables might be missing an extension.
    // Append a single "." to prevent automatic appending of extensions by the system.
    std::vector<std::string> args_direct_call = args;
    args_direct_call[0] += ".";
    cmds[0] = CreateCommandLine(args_direct_call);
  }
  bool succeeded = false;
  PROCESS_INFORMATION pi = {};
  for (int attempt = 0; attempt < sizeof(cmds) / sizeof(*cmds); ++attempt) {
    std::string &cmd = cmds[attempt];
    if (!cmd.empty()) {
      (void)cmd.c_str();  // We'll need this to be null-terminated (but mutable) below
      TCHAR *cmdline = &*cmd.begin();
      STARTUPINFO si = {sizeof(si)};
      RAY_UNUSED(new_env_block.c_str());  // Ensure there's a final terminator for Windows
      char *const envp = &new_env_block[0];
      if (CreateProcessA(NULL, cmdline, NULL, NULL, FALSE, 0, envp, NULL, &si, &pi)) {
        succeeded = true;
        break;
      }
    }
  }
  if (succeeded) {
    CloseHandle(pi.hThread);
    fd = reinterpret_cast<intptr_t>(pi.hProcess);
    pid = pi.dwProcessId;
  } else {
    ec = std::error_code(GetLastError(), std::system_category());
    return std::pair<pid_t, intptr_t>(-1, -1);
  }
#else
  std::vector<char *> new_env_ptrs;
  for (size_t i = 0; i < new_env_block.size(); i += strlen(&new_env_block[i]) + 1) {
    new_env_ptrs.push_back(&new_env_block[i]);
  }
  new_env_ptrs.push_back(static_cast<char *>(NULL));
  char **envp = &new_env_ptrs[0];

  // TODO(mehrdadn): Use clone() on Linux or posix_spawnp() on Mac to avoid duplicating
  // file descriptors into the child process, as that can be problematic.
  int pipefds[2];  // Create pipe to get PID & track lifetime
  int parent_lifetime_pipe[2];

  // Create pipes to health check parent <> child.
  // pipefds is used for parent to check child's health.
  if (pipe(pipefds) == -1) {
    pipefds[0] = pipefds[1] = -1;
  }
  // parent_lifetime_pipe is used for child to check parent's health.
  if (pipe_to_stdin) {
    if (pipe(parent_lifetime_pipe) == -1) {
      parent_lifetime_pipe[0] = parent_lifetime_pipe[1] = -1;
    }
  }

  pid = pipefds[1] != -1 ? fork() : -1;

  // The process was forked successfully and we're executing in the child
  // process.
  if (pid == 0) {
    add_to_cgroup(std::to_string(getpid()));
  }

  // If we don't pipe to stdin close pipes that are not needed.
  if (pid <= 0 && pipefds[0] != -1) {
    close(pipefds[0]);  // not the parent, so close the read end of the pipe
    pipefds[0] = -1;
  }
  if (pid != 0 && pipefds[1] != -1) {
    close(pipefds[1]);  // not the child, so close the write end of the pipe
    pipefds[1] = -1;
    // make sure the read end of the pipe is closed on exec
    SetFdCloseOnExec(pipefds[0]);
  }

  // Create a pipe and redirect the read pipe to a child's stdin.
  // Child can use it to detect the parent's lifetime.
  // See the below link for details.
  // https://stackoverflow.com/questions/12193581/detect-death-of-parent-process
  if (pipe_to_stdin) {
    if (pid <= 0 && parent_lifetime_pipe[1] != -1) {
      // Child. Close sthe write end of the pipe from child.
      close(parent_lifetime_pipe[1]);
      parent_lifetime_pipe[1] = -1;
      SetFdCloseOnExec(parent_lifetime_pipe[0]);
    }
    if (pid != 0 && parent_lifetime_pipe[0] != -1) {
      // Parent. Close the read end of the pipe.
      close(parent_lifetime_pipe[0]);
      parent_lifetime_pipe[0] = -1;
      // Make sure the write end of the pipe is closed on exec.
      SetFdCloseOnExec(parent_lifetime_pipe[1]);
    }
  } else {
    // parent_lifetime_pipe pipes are not used.
    parent_lifetime_pipe[0] = -1;
    parent_lifetime_pipe[1] = -1;
  }

  if (pid == 0) {
    // Child process case. Reset the SIGCHLD handler.
    signal(SIGCHLD, SIG_DFL);
    // If process needs to be decoupled, double-fork to avoid zombies.
    pid_t pid2 = decouple ? fork() : 0;
    if (pid2 != 0) {
      _exit(pid2 == -1 ? errno : 0);  // Parent of grandchild; must exit
    }

#if !defined(_WIN32)
    // Put this child into a new process group if requested, before exec.
    if (new_process_group) {
      // setpgrp() is equivalent to setpgid(0,0).
      if (setpgrp() == -1) {
        // If this fails, the process remains in the parent's process group.
        // Parent-side cleanup logic revalidates PGIDs to avoid mis-signaling.
        int err = errno;
#if defined(__GLIBC__)
        // GNU-specific strerror_r returns char*.
        char buf[128];
        char *msg = strerror_r(err, buf, sizeof(buf));
        dprintf(STDERR_FILENO,
                "ray: setpgrp() failed in child: errno=%d (%s)\n",
                err,
                msg ? msg : "unknown error");
#else
        // POSIX strerror_r returns int and fills buffer.
        char buf[128];
        if (strerror_r(err, buf, sizeof(buf)) == 0) {
          dprintf(
              STDERR_FILENO, "ray: setpgrp() failed in child: errno=%d (%s)\n", err, buf);
        } else {
          dprintf(STDERR_FILENO, "ray: setpgrp() failed in child: errno=%d\n", err);
        }
#endif
      }
    }
#endif

    // Redirect the read pipe to stdin so that child can track the
    // parent lifetime.
    if (parent_lifetime_pipe[0] != -1) {
      dup2(parent_lifetime_pipe[0], STDIN_FILENO);
    }

    // This is the spawned process. Any intermediate parent is now dead.
    pid_t my_pid = getpid();
    if (write(pipefds[1], &my_pid, sizeof(my_pid)) == sizeof(my_pid)) {
      execvpe(argv[0], const_cast<char *const *>(argv), const_cast<char *const *>(envp));
    }
    _exit(errno);  // fork() succeeded and exec() failed, so abort the child
  }
  if (pid > 0) {
    // Parent process case
    if (decouple) {
      int s;
      (void)waitpid(pid, &s, 0);  // can't do much if this fails, so ignore return value
      int r = read(pipefds[0], &pid, sizeof(pid));
      (void)r;  // can't do much if this fails, so ignore return value
    }
  }
  // Use pipe to track process lifetime. (The pipe closes when process terminates.)
  fd = pipefds[0];
  if (pid == -1) {
    ec = std::error_code(errno, std::system_category());
  }
#endif
  RAY_CHECK((pid == -1 && fd == -1) || (pid != -1 && fd != -1)) << absl::StrFormat(
      "Process (pid: %d) failed to be spawned to executed. "
      "Invariant violated: pid: %d and fd: %d are not both valid or both invalid.",
      pid,
      pid,
      fd);
  return std::pair<pid_t, intptr_t>(pid, fd);
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

std::pair<std::unique_ptr<ProcessInterface>, std::error_code> Process::Spawn(
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
  return std::pair<std::unique_ptr<ProcessInterface>, std::error_code>(std::move(proc),
                                                                       error);
}

std::error_code Process::Call(const std::vector<std::string> &args,
                              const ProcessEnvironment &env) {
  std::vector<const char *> argv;
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code ec;
  Process proc(&*argv.begin(), ec, true, env);
  if (!ec) {
    int return_code = proc.Wait();
    if (return_code != 0) {
      ec = std::error_code(return_code, std::system_category());
    }
  }
  return ec;
}

pid_t Process::GetId() const { return pid_; }

// TODO(Kunchd): We need to deprecate null process
bool Process::IsNull() const { return pid_ == -1; }

bool Process::IsValid() const { return GetId() != -1; }

int Process::Wait() const {
  int status = -1;
  if (IsValid()) {
    if (pid_ >= 0) {
      std::error_code error;
#ifdef _WIN32
      HANDLE handle = fd_ != -1 ? reinterpret_cast<HANDLE>(fd_) : NULL;
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
      if (fd_ != -1) {
        // We have a pipe, so wait for its other end to close, to detect process death.
        unsigned char buf[1 << 8];
        ptrdiff_t r;
        while ((r = read(fd_, buf, sizeof(buf))) > 0) {
          // Keep reading until socket terminates
        }
        status = r == -1 ? -1 : 0;
      } else if (waitpid(pid_, &status, 0) == -1) {
        // Just the normal waitpid() case.
        // (We can only do this once, only if we own the process. It fails otherwise.)
        error = std::error_code(errno, std::system_category());
      }
#endif
      if (error) {
        RAY_LOG(ERROR) << absl::StrFormat(
            "Failed to wait for process %d with error %d: %s",
            pid_,
            error.value(),
            error.message());
      }
    }
  }
  return status;
}

bool Process::IsAlive() const {
  if (IsValid()) {
    return IsProcessAlive(pid_);
  }
  return false;
}

void Process::Kill() {
  if (IsValid()) {
    if (pid_ >= 0) {
      std::error_code error;
#ifdef _WIN32
      HANDLE handle = fd_ != -1 ? reinterpret_cast<HANDLE>(fd_) : NULL;
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
      pollfd pfd = {static_cast<int>(fd_), POLLHUP};
      if (fd_ != -1 && poll(&pfd, 1, 0) == 1 && (pfd.revents & POLLHUP)) {
        // The process has already died; don't attempt to kill its PID again.
      } else if (kill(pid_, SIGKILL) != 0) {
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
        RAY_LOG(DEBUG) << absl::StrFormat("Failed to kill process %d with error %d: %s",
                                          pid_,
                                          error.value(),
                                          error.message());
      }
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
  return equal_to<>()(&x, &y);
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
  return std::hash<void const *>()(&value);
}

}  // namespace std
