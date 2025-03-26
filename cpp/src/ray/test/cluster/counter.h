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

#include <cassert>
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
  int GetCount() { return count; }
  uint64_t GetPid();
  void ExceptionFunc() { throw std::invalid_argument("error"); }
  static bool IsProcessAlive(uint64_t pid);

  bool CheckRestartInActorCreationTask();
  bool CheckRestartInActorTask();
  ray::ActorHandle<Counter> CreateChildActor(std::string actor_name);
  std::string CreateNestedChildActor(std::string actor_name);
  int Plus1ForActor(ray::ActorHandle<Counter> actor);

  std::string GetNamespaceInActor();

  std::string GetVal(ray::ObjectRef<std::string> obj) { return *obj.Get(); }

  std::vector<std::byte> GetBytes(std::string s) {
    std::vector<std::byte> bytes;
    bytes.reserve(s.size());

    std::transform(std::begin(s), std::end(s), std::back_inserter(bytes), [](char c) {
      return std::byte(c);
    });

    return bytes;
  }

  std::vector<std::byte> EchoBytes(const std::vector<std::byte> &bytes) { return bytes; }

  std::string EchoString(const std::string &str) { return str; }

  std::vector<std::string> EchoStrings(const std::vector<std::string> &strings) {
    return strings;
  }

  std::vector<std::any> EchoAnyArray(const std::vector<std::any> &anys) {
    assert(anys.size() == 11);
    // check bool
    assert(anys[0].type() == typeid(bool));
    assert(std::any_cast<bool>(anys[0]) == true);
    // equal to java Byte.MAX_VALUE
    assert(anys[1].type() == typeid(uint64_t));
    assert(std::any_cast<uint64_t>(anys[1]) == 127);
    // equal to java Short.MAX_VALUE
    assert(anys[2].type() == typeid(uint64_t));
    assert(std::any_cast<uint64_t>(anys[2]) == 32767);
    // equal to java Integer.MAX_VALUE
    assert(anys[3].type() == typeid(uint64_t));
    assert(std::any_cast<uint64_t>(anys[3]) == 2147483647);
    // equal to java Short.MAX_VALUE
    assert(anys[4].type() == typeid(uint64_t));
    assert(std::any_cast<uint64_t>(anys[4]) == 9223372036854775807);
    // equal to java Long.MIN_VALUE
    assert(anys[5].type() == typeid(int64_t));
    assert(std::any_cast<int64_t>(anys[5]) == -9223372036854775808);
    // equal to java BigInteger.valueOf(Long.MAX_VALUE)
    assert(anys[6].type() == typeid(uint64_t));
    assert(std::any_cast<uint64_t>(anys[6]) == 9223372036854775807);
    // equal to java string "Hello World!"
    assert(anys[7].type() == typeid(std::string));
    assert(std::any_cast<std::string>(anys[7]) == "Hello World!");
    // equal to java float 1.234f
    assert(anys[8].type() == typeid(double));
    assert(std::any_cast<double>(anys[8]) == 1.234);
    // equal to java double 1.234
    assert(anys[9].type() == typeid(double));
    assert(std::any_cast<double>(anys[9]) == 1.234);
    // equal to java byte[]
    std::vector<char> bytes = {'b', 'i', 'n', 'a', 'r', 'y'};
    assert(anys[10].type() == typeid(std::vector<std::byte>));
    assert(std::any_cast<std::vector<char>>(anys[10]) == bytes);
    return anys;
  }

  int GetIntVal(ray::ObjectRef<ray::ObjectRef<int>> obj) {
    auto val = *obj.Get();
    return *val.Get();
  }

  bool Initialized() { return ray::IsInitialized(); }

  std::string GetEnvVar(std::string key) {
    auto value = std::getenv(key.c_str());
    return value == NULL ? "" : std::string(value);
  }

  int GetIntByObjectRef(ray::ObjectRef<int> object_ref);

 private:
  int count;
  bool is_restared = false;
  ray::ActorHandle<Counter> child_actor;
};

std::string GetEnvVar(std::string key);

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
