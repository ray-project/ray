#ifndef RAY_UTIL_PROCESS_H
#define RAY_UTIL_PROCESS_H

#ifdef _WIN32
#include <Windows.h>
#include <process.h>
#endif

#include <vector>

#ifdef _WIN32
// Fork + exec combo for Windows. Returns -1 on failure.
// TODO(mehrdadn): This is dangerous on Windows.
// We need to keep the actual process handle alive for the PID to stay valid.
// Make this change as soon as possible, or the PID may refer to the wrong process.
static pid_t spawnvp_wrapper(std::vector<std::string> const &args) {
  pid_t pid;
  std::vector<const char *> str_args;
  for (const auto &arg : args) {
    str_args.push_back(arg.c_str());
  }
  str_args.push_back(NULL);
  HANDLE handle = (HANDLE)spawnvp(P_NOWAIT, str_args[0], str_args.data());
  if (handle != INVALID_HANDLE_VALUE) {
    pid = static_cast<pid_t>(GetProcessId(handle));
    if (pid == 0) {
      pid = -1;
    }
    CloseHandle(handle);
  } else {
    pid = -1;
    errno = EINVAL;
  }
  return pid;
}
#else
// Fork + exec combo for POSIX. Returns -1 on failure.
static pid_t spawnvp_wrapper(std::vector<std::string> const &args) {
  pid_t pid;
  std::vector<const char *> str_args;
  for (const auto &arg : args) {
    str_args.push_back(arg.c_str());
  }
  str_args.push_back(NULL);
  pid = fork();
  if (pid == 0) {
    // Child process case.
    // Reset the SIGCHLD handler for the worker.
    // TODO(mehrdadn): Move any work here to the child process itself
    //                 so that it can also be implemented on Windows.
    signal(SIGCHLD, SIG_DFL);
    if (execvp(str_args[0], const_cast<char *const *>(str_args.data())) == -1) {
      pid = -1;
      abort();  // fork() succeeded but exec() failed, so abort the child
    }
  }
  return pid;
}
#endif

static pid_t SpawnProcess(const std::vector<std::string> &worker_command_args) {
  if (RAY_LOG_ENABLED(DEBUG)) {
    std::stringstream stream;
    stream << "Starting worker process with command:";
    for (const auto &arg : worker_command_args) {
      stream << " " << arg;
    }
    RAY_LOG(DEBUG) << stream.str();
  }

  // Launch the process to create the worker.
  pid_t pid = spawnvp_wrapper(worker_command_args);
  if (pid == -1) {
    RAY_LOG(FATAL) << "Failed to start worker with error " << errno << ": "
                   << strerror(errno);
  }
  return pid;
}

#endif  // RAY_UTIL_PROCESS_H