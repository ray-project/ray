#include <unistd.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <Windows.h>

int usleep(useconds_t usec) {
  Sleep((usec + (1000 - 1)) / 1000);
  return 0;
}

unsigned sleep(unsigned seconds) {
  Sleep(seconds * 1000);
  return 0;
}

int kill(pid_t pid, int sig) {
  int result;
  if (HANDLE process = OpenProcess(PROCESS_TERMINATE, FALSE, pid)) {
    if (sig == SIGKILL) {
      if (TerminateProcess(process, ERROR_PROCESS_ABORTED)) {
        result = 0;
      } else {
        result = -1;
        errno = EPERM;
      }
    } else {
      result = -1;
      errno = EINVAL;
    }
    CloseHandle(process);
  } else {
    result = -1;
    errno = ESRCH;
  }
  return result;
}
