#include <unistd.h>

#include <atomic>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <Windows.h>
#include <Winternl.h>

#ifndef STATUS_BUFFER_OVERFLOW
#define STATUS_BUFFER_OVERFLOW ((NTSTATUS)0x80000005L)
#endif

typedef LONG NTSTATUS;
typedef NTSTATUS WINAPI NtQueryInformationProcess_t(HANDLE ProcessHandle,
                                                    ULONG ProcessInformationClass,
                                                    PVOID ProcessInformation,
                                                    ULONG ProcessInformationLength,
                                                    ULONG *ReturnLength);

static std::atomic<NtQueryInformationProcess_t *> NtQueryInformationProcess_ =
    ATOMIC_VAR_INIT(NULL);

pid_t getppid() {
  NtQueryInformationProcess_t *NtQueryInformationProcess = ::NtQueryInformationProcess_;
  if (!NtQueryInformationProcess) {
    NtQueryInformationProcess = reinterpret_cast<NtQueryInformationProcess_t *>(
        GetProcAddress(GetModuleHandle(TEXT("ntdll.dll")),
                       _CRT_STRINGIZE(NtQueryInformationProcess)));
    ::NtQueryInformationProcess_ = NtQueryInformationProcess;
  }
  DWORD ppid = 0;
  PROCESS_BASIC_INFORMATION info;
  ULONG cb = sizeof(info);
  NTSTATUS status = NtQueryInformationProcess(GetCurrentProcess(), 0, &info, cb, &cb);
  if ((status >= 0 || status == STATUS_BUFFER_OVERFLOW) && cb >= sizeof(info)) {
    ppid = reinterpret_cast<DWORD>(info.Reserved3);
  }
  pid_t result = 0;
  if (ppid > 0) {
    // For now, assume PPID = 1 (simulating the reassignment to "init" on Linux)
    result = 1;
    if (HANDLE parent = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, ppid)) {
      long long me_created, parent_created;
      FILETIME unused;
      if (GetProcessTimes(GetCurrentProcess(), reinterpret_cast<FILETIME *>(&me_created),
                          &unused, &unused, &unused) &&
          GetProcessTimes(parent, reinterpret_cast<FILETIME *>(&parent_created), &unused,
                          &unused, &unused)) {
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

int usleep(useconds_t usec) {
  Sleep((usec + (1000 - 1)) / 1000);
  return 0;
}

unsigned sleep(unsigned seconds) {
  Sleep(seconds * 1000);
  return 0;
}
