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

/// This is an example of Ray C++ application. Please visit
/// `https://docs.ray.io/en/master/index.html` for more details.
#include <ray/api.h>
#include <ray/api/metric.h>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

ABSL_FLAG(int32_t, metric_time, 10, "The total time in seconds for recording metrics");

void test_metric(const std::string &exec_type, int total_time) {
  ray::Gauge gauge("ray_test_gauge", "test gauge", "unit", {"tag1", "tag2"});
  ray::Counter counter("ray_test_counter", "test counter", "unit", {"tag1", "tag2"});
  ray::Histogram histogram(
      "ray_test_histogram", "test histogram", "unit", {1, 10}, {"tag1", "tag2"});
  ray::Sum sum("ray_test_sum", "test sum", "unit", {"tag1", "tag2"});

  std::unordered_map<std::string, std::string> tag_1 = {{"tag1", "increasing"},
                                                        {"tag2", exec_type}};
  std::unordered_map<std::string, std::string> tag_2 = {{"tag1", "steady"},
                                                        {"tag2", exec_type}};
  int num = total_time;
  for (int i = 0; i < num; i++) {
    gauge.Set(i, tag_1);
    counter.Inc(i, tag_1);
    histogram.Observe(i, tag_1);
    sum.Record(i, tag_1);

    gauge.Set(1, tag_2);
    counter.Inc(1, tag_2);
    histogram.Observe(1, tag_2);
    sum.Record(1, tag_2);
    sleep(1);
  }
}

class MetricActor {
 public:
  static MetricActor *FactoryCreate() { return new MetricActor(); }
  void record_metric(int total_time) { test_metric("actor", total_time); }
};

RAY_REMOTE(RAY_FUNC(MetricActor::FactoryCreate), &MetricActor::record_metric);

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  int total_time = absl::GetFlag(FLAGS_metric_time);
  // Start ray cluster and ray runtime.
  ray::RayConfig config;
  ray::Init(config, argc, argv);
  auto actor = ray::Actor(MetricActor::FactoryCreate).Remote();
  auto object_ref = actor.Task(&MetricActor::record_metric).Remote(total_time);
  test_metric("driver", total_time);
  object_ref.Get();
  // Stop ray cluster and ray runtime.
  ray::Shutdown();
  return 0;
}
