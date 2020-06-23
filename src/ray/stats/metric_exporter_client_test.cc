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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "absl/memory/memory.h"
#include "ray/stats/metric_exporter.h"
#include "ray/stats/metric_exporter_client.h"
#include "ray/stats/stats.h"

namespace ray {
using namespace stats;

class MockExporterClient1 : public MetricExporterDecorator {
 public:
  MockExporterClient1(std::shared_ptr<MetricExporterClient> exporter)
      : MetricExporterDecorator(exporter) {
    client1_count = 0;
  }
  void ReportMetrics(const MetricPoints &points) override {
    MetricExporterDecorator::ReportMetrics(points);
    client1_count += points.size();
    client1_value = points.back().value;
    RAY_LOG(INFO) << "Client 1 " << client1_count << " " << points.back().value;
  }
  static int GetCount() { return client1_count; }
  static int GetValue() { return client1_value; }

 private:
  static int client1_count;
  static int client1_value;
};

class MockExporterClient2 : public MetricExporterDecorator {
 public:
  MockExporterClient2(std::shared_ptr<MetricExporterClient> exporter)
      : MetricExporterDecorator(exporter) {
    client2_count = 0;
  }
  void ReportMetrics(const MetricPoints &points) override {
    MetricExporterDecorator::ReportMetrics(points);
    client2_count += 2 * points.size();
    RAY_LOG(INFO) << "Client 2 " << client2_count << " " << points.back().value;
    client2_value = points.back().value;
  }
  static int GetCount() { return client2_count; }
  static int GetValue() { return client2_value; }

 private:
  static int client2_count;
  static int client2_value;
};

class MetricExporterClientTest : public ::testing::Test {
 public:
  void SetUp() {
    const stats::TagsType global_tags = {{stats::LanguageKey, "CPP"},
                                         {stats::WorkerPidKey, "1000"}};
    ray::stats::Init("127.0.0.1:8888", global_tags, false);
    std::shared_ptr<MetricExporterClient> exporter(new stats::StdoutExporterClient());
    std::shared_ptr<MetricExporterClient> mock1(new MockExporterClient1(exporter));
    std::shared_ptr<MetricExporterClient> mock2(new MockExporterClient2(mock1));
    MetricExporter::Register(mock2);
  }

  void Shutdown() {}
};

int MockExporterClient1::client1_count;
int MockExporterClient2::client2_count;
int MockExporterClient1::client1_value;
int MockExporterClient2::client2_value;

TEST_F(MetricExporterClientTest, decorator_test) {
  // Export client should emit at least once in 10 seconds.
  for (size_t i = 0; i < 100; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    stats::CurrentWorker().Record(i + 1);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  ASSERT_GE(100, MockExporterClient1::GetValue());
  ASSERT_GE(100, MockExporterClient2::GetValue());
  ASSERT_EQ(1, MockExporterClient1::GetCount());
  ASSERT_EQ(2, MockExporterClient2::GetCount());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
