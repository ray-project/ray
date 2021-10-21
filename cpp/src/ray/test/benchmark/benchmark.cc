// Copyright 2017 The Ray Authors.
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

#include <chrono>
#include <set>

#include "gtest/gtest.h"
#include "ray/api.h"

using namespace ::ray;

void NoReturn() {}
int ReturnVal() { return 1; }

void Input(std::string str) {}

std::string Echo(std::string str) { return str; }

RAY_REMOTE(NoReturn, ReturnVal, Input, Echo);

class SimpleCounter {
 public:
  int count;

  explicit SimpleCounter(int init) { count = init; }

  static SimpleCounter *FactoryCreate() { return new SimpleCounter(0); }

  int Add() {
    count += 1;
    return count;
  }

  int Get() const { return count; }
};

RAY_REMOTE(SimpleCounter::FactoryCreate, &SimpleCounter::Add, &SimpleCounter::Get);

/// Only ray.call, don't get ObjectRef result.
template <typename F>
void RunLatencyTestIgnoreGetResult(F f, std::string func_name, size_t num_repeats) {
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < num_repeats; i++) {
    f();
  }
  auto end = std::chrono::system_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

  std::string out = "Benchmark ";
  out.append(func_name)
      .append(" finished, repeated ")
      .append(std::to_string(num_repeats))
      .append(" times, total duration ")
      .append(std::to_string(duration / 1000000))
      .append(" ms,")
      .append(" average duration ")
      .append(std::to_string(duration / num_repeats))
      .append(" ns.");
  RAYLOG(INFO) << out;
}

template <typename R, typename F>
void RunLatencyTest(F f, std::string func_name, size_t num_repeats) {
  std::set<uint64_t> set;
  for (size_t i = 0; i < num_repeats; i++) {
    auto start = std::chrono::system_clock::now();
    ObjectRef<R> object_ref = f();
    object_ref.Get();
    auto end = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    set.emplace(duration);
  }

  std::string out = "Latency ";
  out.append(func_name).append(": \n");

  for (auto &time : set) {
    out.append(std::to_string(time)).append("ns\n");
  }
  RAYLOG(INFO) << out;
}

TEST(BenchmarkTest, SimpleLatancyTest) {
  ActorHandle<SimpleCounter> actor = ray::Actor(SimpleCounter::FactoryCreate).Remote();
  size_t num_repeats = 20;
  RunLatencyTest<int>([&actor] { return actor.Task(&SimpleCounter::Add).Remote(); },
                      "&SimpleCounter::Add", num_repeats);

  RunLatencyTest<void>([] { return ray::Task(NoReturn).Remote(); }, "NoReturn",
                       num_repeats);

  RunLatencyTest<int>([] { return ray::Task(ReturnVal).Remote(); }, "ReturnVal",
                      num_repeats);
}

TEST(BenchmarkTest, SimpleRemoteTest) {
  ActorHandle<SimpleCounter> actor = ray::Actor(SimpleCounter::FactoryCreate).Remote();
  size_t num_repeats = 100000;
  RunLatencyTestIgnoreGetResult([&actor] { actor.Task(&SimpleCounter::Add).Remote(); },
                                "&SimpleCounter::Add", num_repeats);

  RunLatencyTestIgnoreGetResult([] { ray::Task(NoReturn).Remote(); }, "NoReturn",
                                num_repeats);

  RunLatencyTestIgnoreGetResult([] { ray::Task(ReturnVal).Remote(); }, "ReturnVal",
                                num_repeats);
}

TEST(BenchmarkTest, RemoteWithArgsTest) {
  size_t num_repeats = 100000;
  std::string small_str(100, ' ');
  RunLatencyTestIgnoreGetResult([&small_str] { ray::Task(Input).Remote(small_str); },
                                "Input small_str", num_repeats);
  RunLatencyTestIgnoreGetResult([&small_str] { ray::Task(Echo).Remote(small_str); },
                                "Echo small_str", num_repeats);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  ray::RayConfig config;
  ray::Init(config, argc, argv);
  int ret = RUN_ALL_TESTS();
  ray::Shutdown();

  return ret;
}