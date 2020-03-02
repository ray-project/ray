#include <sys/wait.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <Windows.h>

pid_t waitpid(pid_t pid, int *status, int options) {
  int result;
  if (pid <= 0) {
    result = -1;
    errno = ECHILD;
  } else if (HANDLE process = OpenProcess(SYNCHRONIZE, FALSE, pid)) {
    DWORD timeout = status && *status == WNOHANG ? 0 : INFINITE;
    if (WaitForSingleObject(process, timeout) != WAIT_FAILED) {
      result = 0;
    } else {
      result = -1;
      errno = ECHILD;
    }
    CloseHandle(process);
  } else {
    result = -1;
    errno = ECHILD;
  }
  return result;
}
