// Copyright 2025 The Ray Authors.
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

#include <benchmark/benchmark.h>

#include <algorithm>
#include <string>

#include "absl/strings/cord.h"

static void UseCord(benchmark::State &state) {
  // Code inside this loop is measured repeatedly
  std::string created_string(5000, 'h');
  for (auto _ : state) {
    auto cord = absl::MakeCordFromExternal(absl::string_view(created_string), []() {});
    // absl::string_view cord(created_string);
    // Make sure the variable is not optimized away by compiler
    benchmark::DoNotOptimize(cord);
  }
}
// Register the function as a benchmark
BENCHMARK(UseCord);

static void UseMemcpy(benchmark::State &state) {
  // Code before the loop is not measured
  std::string created_string(5000, 'h');
  for (auto _ : state) {
    std::string copy(created_string);
    benchmark::DoNotOptimize(copy);
  }
}
BENCHMARK(UseMemcpy);

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
