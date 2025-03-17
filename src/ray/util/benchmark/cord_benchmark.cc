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
#include "src/ray/protobuf/object_manager.pb.h"

constexpr size_t str_size = 5000000;

static void UseCord(benchmark::State &state) {
  // Code inside this loop is measured repeatedly
  char *created_string = new char[str_size];
  for (char *iter = created_string; iter < created_string + str_size; iter++) {
    *iter = 'h';
  }
  for (auto _ : state) {
    ray::rpc::PushRequest request;
    absl::Cord result_cord;
    result_cord.Append(
        absl::MakeCordFromExternal(absl::string_view(created_string, str_size), []() {}));
    std::string metadata{"metadata"};
    result_cord.Append(metadata);
    request.set_data(result_cord);
    // Make sure the variable is not optimized away by compiler
    benchmark::DoNotOptimize(request);
  }
}
// Register the function as a benchmark
BENCHMARK(UseCord);

static void UseMemcpy(benchmark::State &state) {
  // Code before the loop is not measured
  char *created_string = new char[str_size];
  for (char *iter = created_string; iter < created_string + str_size; iter++) {
    *iter = 'h';
  }
  for (auto _ : state) {
    std::string result;
    result.resize(str_size);
    std::memcpy(result.data(), created_string, str_size);
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK(UseMemcpy);

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
