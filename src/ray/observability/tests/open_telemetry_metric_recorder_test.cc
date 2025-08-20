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

#include "ray/observability/open_telemetry_metric_recorder.h"

#include "gtest/gtest.h"

namespace ray {
namespace observability {

class OpenTelemetryMetricRecorderTest : public ::testing::Test {
 public:
  OpenTelemetryMetricRecorderTest()
      : recorder_(OpenTelemetryMetricRecorder::GetInstance()) {}

  static void SetUpTestSuite() {
    // Initialize the OpenTelemetryMetricRecorder with a mock endpoint and intervals
    OpenTelemetryMetricRecorder::GetInstance().RegisterGrpcExporter(
        "localhost:1234",
        std::chrono::milliseconds(10000),
        std::chrono::milliseconds(5000));
  }

  static void TearDownTestSuite() {
    // Cleanup if necessary
    OpenTelemetryMetricRecorder::GetInstance().Shutdown();
  }

  std::optional<double> GetObservableMetricValue(
      const std::string &name,
      const absl::flat_hash_map<std::string, std::string> &tags) {
    std::lock_guard<std::mutex> lock(recorder_.mutex_);
    auto it = recorder_.observations_by_name_.find(name);
    if (it == recorder_.observations_by_name_.end()) {
      return std::nullopt;  // Not registered
    }
    auto tag_it = it->second.find(tags);
    if (tag_it != it->second.end()) {
      return tag_it->second;  // Get the value
    }
    return std::nullopt;
  }

 protected:
  OpenTelemetryMetricRecorder &recorder_;
};

TEST_F(OpenTelemetryMetricRecorderTest, TestGaugeMetric) {
  recorder_.RegisterGaugeMetric("test_metric", "Test metric description");
  recorder_.SetMetricValue("test_metric", {{"tag1", "value1"}}, 42.0);
  // Get a non-empty value of a registered gauge metric and tags
  ASSERT_EQ(GetObservableMetricValue("test_metric", {{"tag1", "value1"}}), 42.0);
  // Get an empty value of a registered gauge metric with unregistered tags
  ASSERT_EQ(GetObservableMetricValue("test_metric", {{"tag1", "value2"}}), std::nullopt);
}

TEST_F(OpenTelemetryMetricRecorderTest, TestCounterMetric) {
  recorder_.RegisterCounterMetric("test_counter", "Test counter description");
  // Check that the counter metric is registered
  ASSERT_TRUE(recorder_.IsMetricRegistered("test_counter"));
}

TEST_F(OpenTelemetryMetricRecorderTest, TestSumMetric) {
  recorder_.RegisterSumMetric("test_sum", "Test sum description");
  // Check that the sum metric is registered
  ASSERT_TRUE(recorder_.IsMetricRegistered("test_sum"));
}

TEST_F(OpenTelemetryMetricRecorderTest, TestHistogramMetric) {
  recorder_.RegisterHistogramMetric(
      "test_histogram", "Test histogram description", {0.0, 10.0, 20.0, 30.0});
  // Check that the histogram metric is registered
  ASSERT_TRUE(recorder_.IsMetricRegistered("test_histogram"));
}

}  // namespace observability
}  // namespace ray
