#ifndef WAIT_H
#define WAIT_H

#include <unistd.h>  // pid_t

#define WNOHANG 1

__declspec(
    deprecated("Waiting on a process by ID has an inherent race condition"
               " on Windows and is discouraged. "
               "Please use a wrapper that keeps the process handle alive"
               " and waits on it directly as needed."
               "")) pid_t waitpid(pid_t pid, int *status, int options);

#endif /* WAIT_H */
