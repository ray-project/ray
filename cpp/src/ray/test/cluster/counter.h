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

#pragma once

#include <ray/api.h>

#include <condition_variable>
#include <mutex>

/// a class of user code
class Counter {
 public:
  Counter(int init, bool with_exception = false);
  static Counter *FactoryCreate();
  static Counter *FactoryCreateException();
  static Counter *FactoryCreate(int init);
  static Counter *FactoryCreate(int init1, int init2);

  int Plus1();
  int Add(int x);
  int Exit();
  uint64_t GetPid();
  void ExceptionFunc() { throw std::invalid_argument("error"); }
  static bool IsProcessAlive(uint64_t pid);

  bool CheckRestartInActorCreationTask();
  bool CheckRestartInActorTask();

  std::string GetVal(ray::ObjectRef<std::string> obj) { return *obj.Get(); }

  int GetIntVal(ray::ObjectRef<ray::ObjectRef<int>> obj) {
    auto val = *obj.Get();
    return *val.Get();
  }

  bool Initialized() { return ray::IsInitialized(); }

 private:
  int count;
  bool is_restared = false;
};

inline Counter *CreateCounter() { return new Counter(0); }
RAY_REMOTE(CreateCounter);

class CountDownLatch {
 public:
  explicit CountDownLatch(size_t count) : m_count(count) {}

  void Wait() {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_count > 0) {
      m_cv.wait(lock, [this]() { return m_count == 0; });
    }
  }

  void CountDown() {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_count > 0) {
      m_count--;
      m_cv.notify_all();
    }
  }

 private:
  std::mutex m_mutex;
  std::condition_variable m_cv;
  size_t m_count = 0;
};

class ActorConcurrentCall {
 public:
  static ActorConcurrentCall *FactoryCreate() { return new ActorConcurrentCall(); }

  std::string CountDown() {
    contdown_.CountDown();
    contdown_.Wait();
    return "ok";
  }

 private:
  CountDownLatch contdown_{3};
};