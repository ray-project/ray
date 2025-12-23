// Copyright 2025 The Ray Authors.
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

#include "ray/util/process_fd.h"

#include <algorithm>
#include <cstring>

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

#include <string>
#include <utility>
#include <vector>

#include "ray/util/cmd_line_utils.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/macros.h"
#include "ray/util/process_utils.h"

#ifdef __APPLE__
extern char **environ;

// macOS doesn't come with execvpe.
// https://stackoverflow.com/questions/7789750/execve-with-path-search
static int execvpe(const char *program, char *const argv[], char *const envp[]) {
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

ProcessFD::~ProcessFD() { CloseFD(); }

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

ProcessFD::ProcessFD(ProcessFD &&other) : ProcessFD() { *this = std::move(other); }

ProcessFD &ProcessFD::operator=(ProcessFD &&other) {
  if (this != &other) {
    // We use swap() to make sure the argument is actually moved from
    using std::swap;
    swap(pid_, other.pid_);
    swap(fd_, other.fd_);
  }
  return *this;
}

void ProcessFD::CloseFD() {
  if (fd_ != -1) {
    bool success;
#ifdef _WIN32
    success = !!CloseHandle(reinterpret_cast<HANDLE>(fd_));
#else
    success = close(static_cast<int>(fd_)) == 0;
#endif
    RAY_CHECK(success) << "error " << errno << " closing process " << pid_ << " FD";
  }

  fd_ = -1;
}

intptr_t ProcessFD::GetFD() const { return fd_; }

pid_t ProcessFD::GetId() const { return pid_; }

ProcessFD ProcessFD::spawnvpe(const char *argv[],
                              std::error_code &ec,
                              bool decouple,
                              const ProcessEnvironment &env,
                              bool pipe_to_stdin,
                              std::function<void(const std::string &)> add_to_cgroup,
                              bool new_process_group) {
  ec = std::error_code();
  intptr_t fd;
  pid_t pid;
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
    fd = -1;
    pid = -1;
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
    if (pid_t pid2 = decouple ? fork() : 0) {
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
  return ProcessFD(pid, fd);
}

}  // namespace ray
