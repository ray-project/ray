
#include "counter.h"

#ifdef _WIN32
#include "windows.h"
#else
#include "signal.h"
#include "unistd.h"
#endif

Counter::Counter(int init) { count = init; }

Counter *Counter::FactoryCreate() { return new Counter(0); }

Counter *Counter::FactoryCreate(int init) { return new Counter(init); }

Counter *Counter::FactoryCreate(int init1, int init2) {
  return new Counter(init1 + init2);
}

int Counter::Plus1() {
  count += 1;
  return count;
}

int Counter::Add(int x) {
  count += x;
  return count;
}

int Counter::Exit() {
  ray::api::Ray::ExitActor();
  return 1;
}

bool Counter::IsProcessAlive(uint64_t pid) {
#ifdef _WIN32
  auto process = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, pid);
  if (process == NULL) {
    return false;
  }

  CloseHandle(process);
  return true;
#else
  if (kill(pid, 0) == -1 && errno == ESRCH) {
    return false;
  }
  return true;
#endif
}

uint64_t Counter::GetPid() {
#ifdef _WIN32
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

RAY_REMOTE(RAY_FUNC(Counter::FactoryCreate), RAY_FUNC(Counter::FactoryCreate, int),
           RAY_FUNC(Counter::FactoryCreate, int, int), &Counter::Plus1, &Counter::Add,
           &Counter::Exit, &Counter::GetPid, &Counter::ExceptionFunc);

RAY_REMOTE(ActorConcurrentCall::FactoryCreate, &ActorConcurrentCall::CountDown);