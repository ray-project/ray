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

#include "ray/util/process_utils.h"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <Windows.h>
#include <Winternl.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "absl/strings/str_format.h"

#if defined(__linux__)
#include <filesystem>
#endif

#include "ray/util/logging.h"

namespace ray {

void SetFdCloseOnExec(int fd) {
#if !defined(_WIN32)
  if (fd < 0) {
    return;
  }
  int flags = fcntl(fd, F_GETFD, 0);
  RAY_CHECK_NE(flags, -1) << absl::StrFormat(
      "Failed to setup close on exec for %d: "
      "fctnl error: errno = %s. "
      "Was the fd open?",
      fd,
      strerror(errno));
  const int ret = fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
  RAY_CHECK_NE(ret, -1) << absl::StrFormat(
      "Failed to setup close on exec for %d: "
      "fcntl error: errno = %s. "
      "Was the fd open?",
      fd,
      strerror(errno));
#endif
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
      CloseHandle(handle);
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

std::optional<std::error_code> KillProcessGroup(pid_t pgid, int sig) {
#if !defined(_WIN32)
  std::error_code error;
  if (killpg(pgid, sig) != 0) {
    error = std::error_code(errno, std::system_category());
  }
  return {error};
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

void QuickExit() {
  ray::RayLog::ShutDownRayLog();
  _Exit(1);
}

}  // namespace ray
