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

#if !defined(_WIN32)
void SetFdCloseOnExec(int fd) {
  if (fd < 0) {
    return;
  }
  int flags = fcntl(fd, F_GETFD, 0);
  RAY_CHECK_NE(flags, -1) << "fcntl error: errno = " << errno << ", fd = " << fd;
  const int ret = fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
  RAY_CHECK_NE(ret, -1) << "fcntl error: errno = " << errno << ", fd = " << fd;
  RAY_LOG(DEBUG) << "set FD_CLOEXEC to fd " << fd;
}

// A helper function to robustly read a specific number of bytes from a file descriptor.
// This handles partial reads and interruptions by signals.
static inline ssize_t ReadBytesFromFd(int fd, void *buffer, size_t count) {
  ssize_t total_bytes_read = 0;
  while (total_bytes_read < (ssize_t)count) {
    ssize_t bytes_read = read(fd,
                              reinterpret_cast<char *>(buffer) + total_bytes_read,
                              count - total_bytes_read);
    if (bytes_read == 0) {
      // EOF reached before all bytes were read.
      return total_bytes_read;
    }
    if (bytes_read == -1) {
      if (errno == EINTR) {
        continue;  // Interrupted by signal, retry.
      } else {
        return -1;  // A real read error occurred.
      }
    }
    total_bytes_read += bytes_read;
  }
  return total_bytes_read;
}
#endif

bool EnvironmentVariableLess::operator()(char a, char b) const {
  // TODO(mehrdadn): This is only used on Windows due to current lack of Unicode support.
  // It should be changed when Process adds Unicode support on Windows.
  return std::less<char>()(tolower(a), tolower(b));
}

bool EnvironmentVariableLess::operator()(const std::string &a,
                                         const std::string &b) const {
  bool result;
#ifdef _WIN32
  result = std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end(), *this);
#else
  result = a < b;
#endif
  return result;
}

class ProcessFD {
  pid_t pid_;
  intptr_t fd_;

 public:
  ~ProcessFD();
  ProcessFD();
  explicit ProcessFD(pid_t pid, intptr_t fd = -1);
  ProcessFD(ProcessFD &&other);
  ProcessFD &operator=(ProcessFD &&other);

  ProcessFD(const ProcessFD &other) = delete;
  ProcessFD &operator=(const ProcessFD &other) = delete;

  void CloseFD();
  intptr_t GetFD() const;
  pid_t GetId() const;

  // Fork + exec combo. Returns -1 for the PID on failure.
  static ProcessFD spawnvpe(const char *argv[],
                            std::error_code &ec,
                            bool decouple,
                            const ProcessEnvironment &env,
                            bool pipe_to_stdin) {
    ec = std::error_code();
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

    intptr_t fd;
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
        RAY_UNUSED(
            new_env_block.c_str());  // Ensure there's a final terminator for Windows
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

    intptr_t fd = -1;
    // Pipe for getting startup status (PID and potential errno) from the child.
    int status_pipe[2];
    if (pipe(status_pipe) == -1) {
      ec = std::error_code(errno, std::system_category());
      return ProcessFD(-1, -1);
    }

    // Pipe for parent lifetime tracking, connected to child's stdin.
    int parent_lifetime_pipe[2] = {-1, -1};
    if (pipe_to_stdin) {
      if (pipe(parent_lifetime_pipe) == -1) {
        close(status_pipe[0]);
        close(status_pipe[1]);
        ec = std::error_code(errno, std::system_category());
        return ProcessFD(-1, -1);
      }
    }

    pid = fork();

    if (pid == 0) {
      // --- Child Process (or Intermediate Process if decoupled) ---
      close(status_pipe[0]);  // Child only writes to the status pipe.
      if (pipe_to_stdin) {
        close(parent_lifetime_pipe[1]);  // Child only reads from the lifetime pipe.
      }

      signal(SIGCHLD, SIG_DFL);

      if (decouple) {
        if (fork() != 0) {
          // --- Intermediate Parent ---
          // This process must close ALL inherited pipe FDs before exiting
          // to prevent leaking them to the grandchild or holding pipes open.
          close(status_pipe[1]);
          if (pipe_to_stdin) {
            close(parent_lifetime_pipe[0]);
          }
          _exit(0);
        }
      }

      // --- Grandchild (if decoupled) or Direct Child (if not) ---
      if (pipe_to_stdin) {
        if (dup2(parent_lifetime_pipe[0], STDIN_FILENO) == -1) {
          _exit(errno);
        }
        // After dup2, this original FD is no longer needed.
        close(parent_lifetime_pipe[0]);
      }

      // If execve succeeds, this FD will be closed automatically.
      if (!decouple) {
        // Only set FD_CLOEXEC in the non-decouple case
        SetFdCloseOnExec(status_pipe[1]);
      }

      if (decouple) {
        pid_t my_pid = getpid();
        if (write(status_pipe[1], &my_pid, sizeof(my_pid)) != sizeof(my_pid)) {
          _exit(errno);
        }
      }

      execvpe(argv[0], const_cast<char *const *>(argv), const_cast<char *const *>(envp));

      // If execvpe returns, an error occurred. Write errno to the pipe.
      int err = errno;
      (void)!write(status_pipe[1], &err, sizeof(err));
      _exit(err);

    } else if (pid > 0) {
      // --- Parent Process ---
      close(status_pipe[1]);  // Parent only reads from the status pipe.
      if (pipe_to_stdin) {
        close(parent_lifetime_pipe[0]);  // Parent only writes to the lifetime pipe.
      }

      if (!decouple) {
        // Simple case for non-decoupled process
        int err_from_child;
        ssize_t bytes_read =
            ReadBytesFromFd(status_pipe[0], &err_from_child, sizeof(err_from_child));
        if (bytes_read == 0) {
          // Success: exec'd, pipe closed by CLOEXEC.
          ec = std::error_code();
        } else {
          // Failure: got an error from child or pipe broke.
          if (bytes_read == sizeof(err_from_child)) {
            // We received a full error code from the child.
            ec = std::error_code(err_from_child, std::system_category());
          } else {
            // The pipe was closed before we could read the full error.
            // This can happen if the child crashes.
            // If read() returned an error, use that errno. Otherwise, use EPIPE.
            ec = std::error_code(bytes_read < 0 ? errno : EPIPE, std::system_category());
          }
          while (waitpid(pid, NULL, 0) == -1 && errno == EINTR) {
            continue;
          }
          pid = -1;
        }
        close(status_pipe[0]);
      } else {
        while (waitpid(pid, NULL, 0) == -1 && errno == EINTR) {
          continue;
        }

        // Read the grandchild's PID from the pipe.
        ssize_t bytes_read_pid = ReadBytesFromFd(status_pipe[0], &pid, sizeof(pid));
        if (bytes_read_pid != sizeof(pid)) {
          // If we can't get the PID, it's a startup failure.
          ec = std::error_code(ECHILD, std::system_category());
          pid = -1;
          close(status_pipe[0]);
        } else {
          // We got the PID. Now, do a NON-BLOCKING read to check for an exec error.
          int flags = fcntl(status_pipe[0], F_GETFL, 0);
          fcntl(status_pipe[0], F_SETFL, flags | O_NONBLOCK);
          int exec_errno = 0;
          ssize_t bytes_read_errno =
              read(status_pipe[0], &exec_errno, sizeof(exec_errno));
          fcntl(status_pipe[0], F_SETFL, flags);  // Restore original flags.

          if (bytes_read_errno == sizeof(exec_errno)) {
            // We got an error code back. Launch failed.
            ec = std::error_code(exec_errno, std::system_category());
            pid = -1;
            close(status_pipe[0]);
          } else {
            // No error code was present. Launch was successful.
            // For backward compatibility with tests, we need to keep the pipe
            // open but NOT mark it with FD_CLOEXEC, so that child processes
            // inherit it and it stays open until all descendants exit.
            ec = std::error_code();
            fd = status_pipe[0];

            // Remove the FD_CLOEXEC flag that was set earlier
            flags = fcntl(fd, F_GETFD, 0);
            if (flags != -1) {
              fcntl(fd, F_SETFD, flags & ~FD_CLOEXEC);
            }
          }
        }
      }
    } else {
      // --- Fork Failed ---
      ec = std::error_code(errno, std::system_category());
      close(status_pipe[0]);
      close(status_pipe[1]);
      if (pipe_to_stdin) {
        close(parent_lifetime_pipe[0]);
        close(parent_lifetime_pipe[1]);
      }
    }
#endif
    return ProcessFD(pid, fd);
  }
};

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

Process::~Process() {}

Process::Process() {}

Process::Process(const Process &) = default;

Process::Process(Process &&) = default;

Process &Process::operator=(Process other) {
  p_ = std::move(other.p_);
  return *this;
}

Process::Process(pid_t pid) { p_ = std::make_shared<ProcessFD>(pid); }

Process::Process(const char *argv[],
                 void *io_service,
                 std::error_code &ec,
                 bool decouple,
                 const ProcessEnvironment &env,
                 bool pipe_to_stdin) {
  /// TODO: use io_service with boost asio notify_fork.
  (void)io_service;
#ifdef __linux__
  KnownChildrenTracker::instance().AddKnownChild([&, this]() -> pid_t {
    ProcessFD procfd = ProcessFD::spawnvpe(argv, ec, decouple, env, pipe_to_stdin);
    if (!ec) {
      this->p_ = std::make_shared<ProcessFD>(std::move(procfd));
    }
    return this->GetId();
  });
#else
  ProcessFD procfd = ProcessFD::spawnvpe(argv, ec, decouple, env, pipe_to_stdin);
  if (!ec) {
    p_ = std::make_shared<ProcessFD>(std::move(procfd));
  }
#endif
}

std::error_code Process::Call(const std::vector<std::string> &args,
                              const ProcessEnvironment &env) {
  std::vector<const char *> argv;
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code ec;
  Process proc(&*argv.begin(), NULL, ec, true, env);
  if (!ec) {
    int return_code = proc.Wait();
    if (return_code != 0) {
      ec = std::error_code(return_code, std::system_category());
    }
  }
  return ec;
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

std::pair<Process, std::error_code> Process::Spawn(const std::vector<std::string> &args,
                                                   bool decouple,
                                                   const std::string &pid_file,
                                                   const ProcessEnvironment &env) {
  std::vector<const char *> argv;
  argv.reserve(args.size() + 1);
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code error;
  Process proc(&*argv.begin(), NULL, error, decouple, env);
  if (!error && !pid_file.empty()) {
    std::ofstream file(pid_file, std::ios_base::out | std::ios_base::trunc);
    file << proc.GetId() << std::endl;
    RAY_CHECK(file.good());
  }
  return std::make_pair(std::move(proc), error);
}

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

#ifdef _WIN32
#ifndef STATUS_BUFFER_OVERFLOW
#define STATUS_BUFFER_OVERFLOW ((NTSTATUS)0x80000005L)
#endif
typedef LONG NTSTATUS;
typedef NTSTATUS WINAPI NtQueryInformationProcess_t(HANDLE ProcessHandle,
                                                    ULONG ProcessInformationClass,
                                                    PVOID ProcessInformation,
                                                    ULONG ProcessInformationLength,
                                                    ULONG *ReturnLength);

static std::atomic<NtQueryInformationProcess_t *> NtQueryInformationProcess_ = NULL;

pid_t GetParentPID() {
  NtQueryInformationProcess_t *NtQueryInformationProcess = NtQueryInformationProcess_;
  if (!NtQueryInformationProcess) {
    NtQueryInformationProcess = reinterpret_cast<NtQueryInformationProcess_t *>(
        GetProcAddress(GetModuleHandle(TEXT("ntdll.dll")),
                       _CRT_STRINGIZE(NtQueryInformationProcess)));
    NtQueryInformationProcess_ = NtQueryInformationProcess;
  }
  DWORD ppid = 0;
  PROCESS_BASIC_INFORMATION info;
  ULONG cb = sizeof(info);
  NTSTATUS status = NtQueryInformationProcess(GetCurrentProcess(), 0, &info, cb, &cb);
  if ((status >= 0 || status == STATUS_BUFFER_OVERFLOW) && cb >= sizeof(info)) {
    ppid = static_cast<DWORD>(reinterpret_cast<uintptr_t>(info.Reserved3));
  }
  pid_t result = 0;
  if (ppid > 0) {
    // For now, assume PPID = 1 (simulating the reassignment to "init" on Linux)
    result = 1;
    if (HANDLE parent = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, ppid)) {
      long long me_created, parent_created;
      FILETIME unused;
      if (GetProcessTimes(GetCurrentProcess(),
                          reinterpret_cast<FILETIME *>(&me_created),
                          &unused,
                          &unused,
                          &unused) &&
          GetProcessTimes(parent,
                          reinterpret_cast<FILETIME *>(&parent_created),
                          &unused,
                          &unused,
                          &unused)) {
        if (me_created >= parent_created) {
          // We verified the child is younger than the parent, so we know the parent
          // is still alive.
          // (Note that the parent can still die by the time this function returns,
          // but that race condition exists on POSIX too, which we're emulating here.)
          result = static_cast<pid_t>(ppid);
        }
      }
      CloseHandle(parent);
    }
  }
  return result;
}
#else
pid_t GetParentPID() { return getppid(); }
#endif  // #ifdef _WIN32

pid_t GetPID() {
#ifdef _WIN32
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

bool IsParentProcessAlive() { return GetParentPID() != 1; }

bool IsProcessAlive(pid_t pid) {
#if defined _WIN32
  if (HANDLE handle =
          OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, static_cast<DWORD>(pid))) {
    DWORD exit_code;
    if (GetExitCodeProcess(handle, &exit_code) && exit_code == STILL_ACTIVE) {
      return true;
    }
    CloseHandle(handle);
  }
  return false;
#else
  // Note if the process is a zombie (dead but not yet reaped), it will
  // still be alive by this check.
  if (kill(pid, 0) == -1 && errno == ESRCH) {
    return false;
  }
  return true;
#endif
}

#if defined(__linux__)
static inline std::error_code KillProcLinux(pid_t pid) {
  std::error_code error;
  if (kill(pid, SIGKILL) != 0) {
    error = std::error_code(errno, std::system_category());
  }
  return error;
}
#endif

std::optional<std::error_code> KillProc(pid_t pid) {
#if defined(__linux__)
  return {KillProcLinux(pid)};
#else
  return std::nullopt;
#endif
}

#if defined(__linux__)
static inline std::vector<pid_t> GetAllProcsWithPpidLinux(pid_t parent_pid) {
  std::vector<pid_t> child_pids;

  // Iterate over all files in the /proc directory, looking for directories.
  // See `man proc` for information on the directory structure.
  // Directories with only digits in their name correspond to processes in the process
  // table. We read in the status of each such process and parse the parent PID. If the
  // process parent PID is equal to parent_pid, then we add it to the vector to be
  // returned. Ideally, we use a library for this, but at the time of writing one is not
  // available in Ray C++.

  std::filesystem::directory_iterator dir(kProcDirectory);
  for (const auto &file : dir) {
    if (!file.is_directory()) {
      continue;
    }

    // Determine if the directory name consists of only digits (means it's a PID).
    const auto filename = file.path().filename().string();
    bool file_name_is_only_digit =
        std::all_of(filename.begin(), filename.end(), ::isdigit);
    if (!file_name_is_only_digit) {
      continue;
    }

    // If so, open the status file for reading.
    pid_t pid = std::stoi(filename);
    std::ifstream status_file(file.path() / "status");
    if (!status_file.is_open()) {
      continue;
    }

    // Scan for the line that starts with the ppid key.
    std::string line;
    const std::string key = "PPid:";
    while (std::getline(status_file, line)) {
      const auto substr = line.substr(0, key.size());
      if (substr != key) {
        continue;
      }

      // We found it, read and parse the PPID.
      pid_t ppid = std::stoi(line.substr(substr.size()));
      if (ppid == parent_pid) {
        child_pids.push_back(pid);
      }
      break;
    }
  }

  return child_pids;
}
#endif

std::optional<std::vector<pid_t>> GetAllProcsWithPpid(pid_t parent_pid) {
#if defined(__linux__)
  return {GetAllProcsWithPpidLinux(parent_pid)};
#else
  return std::nullopt;
#endif
}

}  // namespace ray

namespace std {

bool equal_to<ray::Process>::operator()(const ray::Process &x,
                                        const ray::Process &y) const {
  using namespace ray;  // NOLINT
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
  using namespace ray;  // NOLINT
  return !value.IsNull() ? value.IsValid() ? hash<pid_t>()(value.GetId())
                                           : hash<void const *>()(value.Get())
                         : size_t();
}

}  // namespace std
