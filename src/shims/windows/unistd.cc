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
