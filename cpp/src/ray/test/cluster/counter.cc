// Copyright 2021 The Ray Authors.
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

#include "counter.h"

#ifdef _WIN32
#include "windows.h"
#else
#include "signal.h"
#include "unistd.h"
#endif

Counter::Counter(int init, bool with_exception) {
  if (with_exception) {
    throw std::invalid_argument("creation error");
  }
  count = init;
  is_restared = ray::WasCurrentActorRestarted();
}

Counter *Counter::FactoryCreate() { return new Counter(0); }

Counter *Counter::FactoryCreateException() { return new Counter(0, true); }

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
  ray::ExitActor();
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

bool Counter::CheckRestartInActorCreationTask() { return is_restared; }

bool Counter::CheckRestartInActorTask() { return ray::WasCurrentActorRestarted(); }

RAY_REMOTE(RAY_FUNC(Counter::FactoryCreate),
           Counter::FactoryCreateException,
           RAY_FUNC(Counter::FactoryCreate, int),
           RAY_FUNC(Counter::FactoryCreate, int, int),
           &Counter::Plus1,
           &Counter::Add,
           &Counter::Exit,
           &Counter::GetPid,
           &Counter::ExceptionFunc,
           &Counter::CheckRestartInActorCreationTask,
           &Counter::CheckRestartInActorTask,
           &Counter::GetVal,
           &Counter::GetIntVal);

RAY_REMOTE(ActorConcurrentCall::FactoryCreate, &ActorConcurrentCall::CountDown);
