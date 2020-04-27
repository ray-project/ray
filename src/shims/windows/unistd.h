#ifndef UNISTD_H
#define UNISTD_H

#include <getopt.h>
#include <io.h>       // open/read/write/close
#include <process.h>  // getpid

#ifndef EXTERN_C
#ifdef __cplusplus
#define EXTERN_C extern "C"
#else
#define EXTERN_C
#endif
#endif
#ifndef DECLSPEC_IMPORT
#define DECLSPEC_IMPORT __declspec(dllimport)
#endif
#ifndef WINBASEAPI
#define WINBASEAPI DECLSPEC_IMPORT
#endif
#ifndef WINAPI
#define WINAPI __stdcall
#endif
typedef int BOOL;
typedef void *HANDLE;
typedef unsigned long DWORD;
#ifdef _WIN64
typedef unsigned long long UINT_PTR;
typedef unsigned long long ULONG_PTR;
typedef long long ssize_t;
#else
typedef unsigned int UINT_PTR;
typedef unsigned long ULONG_PTR;
typedef int ssize_t;
#endif
typedef int pid_t /* technically unsigned on Windows, but no practical concern */;
enum { SIGKILL = 9 };
typedef ULONG_PTR SIZE_T;
EXTERN_C WINBASEAPI void WINAPI Sleep(DWORD dwMilliseconds);

typedef unsigned int useconds_t;
int usleep(useconds_t usec);
unsigned sleep(unsigned seconds);

pid_t getppid();

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
