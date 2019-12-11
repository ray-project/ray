#ifndef UNISTD_H
#define UNISTD_H

typedef int pid_t /* technically unsigned on Windows, but no practical concern */;
enum { SIGKILL = 9 };

__declspec(
    deprecated("Killing a process by ID has an inherent race condition on Windows"
               " and is HIGHLY discouraged. "
               "Furthermore, signals other than SIGKILL are NOT portable. "
               "Please use a wrapper that keeps the process handle alive"
               " and terminates it directly as needed. "
               "For SIGTERM or other signals, a different IPC mechanism may be"
               " more appropriate (such as window messages on Windows)."
               "")) int kill(pid_t pid, int sig);

#endif /* UNISTD_H */
