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

const size_t kMockReportBatchSize = 10;

class MockExporterClient1 : public MetricExporterDecorator {
 public:
  MockExporterClient1(std::shared_ptr<MetricExporterClient> exporter)
      : MetricExporterDecorator(exporter) {
    client1_count = 0;
    lastest_hist_min = 0.0;
    lastest_hist_mean = 0.0;
    lastest_hist_max = 0.0;
  }

  void ReportMetrics(const std::vector<MetricPoint> &points) override {
    if (points.empty()) {
      return;
    }
    MetricExporterDecorator::ReportMetrics(points);
    client1_count += points.size();
    client1_value = points.back().value;
    RAY_LOG(DEBUG) << "Client 1 " << client1_count << " last metric "
                   << points.back().metric_name << ", value " << points.back().value;
    RecordLastHistData(points);
    // Point size must be less than or equal to report batch size.
    ASSERT_GE(kMockReportBatchSize, points.size());
  }

  static int GetCount() { return client1_count; }
  static void ResetCount() { client1_count = 0; }
  static int GetValue() { return client1_value; }
  static double GetLastestHistMin() { return lastest_hist_min; }
  static double GetLastestHistMean() { return lastest_hist_mean; }
  static double GetLastestHistMax() { return lastest_hist_max; }

 private:
  void RecordLastHistData(const std::vector<MetricPoint> &points) {
    for (auto &point : points) {
      if (point.metric_name.find(".min") != std::string::npos) {
        lastest_hist_min = point.value;
      }
      if (point.metric_name.find(".mean") != std::string::npos) {
        lastest_hist_mean = point.value;
      }
      if (point.metric_name.find(".max") != std::string::npos) {
        lastest_hist_max = point.value;
      }
    }
  }

 private:
  static int client1_count;
  static int client1_value;
  static double lastest_hist_min;
  static double lastest_hist_mean;
  static double lastest_hist_max;
};

class MockExporterClient2 : public MetricExporterDecorator {
 public:
  MockExporterClient2(std::shared_ptr<MetricExporterClient> exporter)
      : MetricExporterDecorator(exporter) {
    client2_count = 0;
  }
  void ReportMetrics(const std::vector<MetricPoint> &points) override {
    if (points.empty()) {
      return;
    }
    MetricExporterDecorator::ReportMetrics(points);
    client2_count += points.size();
    RAY_LOG(DEBUG) << "Client 2 " << client2_count << " last metric "
                   << points.back().metric_name << ", value " << points.back().value;
    client2_value = points.back().value;
  }
  static int GetCount() { return client2_count; }
  static void ResetCount() { client2_count = 0; }
  static int GetValue() { return client2_value; }

 private:
  static int client2_count;
  static int client2_value;
};

/// Default report flush interval is 500ms, so we may wait a while for data
/// exporting.
uint32_t kReportFlushInterval = 500;

class MetricExporterClientTest : public ::testing::Test {
 public:
  void SetUp() {
    const stats::TagsType global_tags = {{stats::LanguageKey, "CPP"},
                                         {stats::WorkerPidKey, "1000"}};
    absl::Duration report_interval = absl::Milliseconds(kReportFlushInterval);
    absl::Duration harvest_interval = absl::Milliseconds(kReportFlushInterval / 2);
    ray::stats::StatsConfig::instance().SetReportInterval(report_interval);
    ray::stats::StatsConfig::instance().SetHarvestInterval(harvest_interval);
    ray::stats::Init("127.0.0.1:8888", global_tags, false);
    std::shared_ptr<MetricExporterClient> exporter(new stats::StdoutExporterClient());
    std::shared_ptr<MetricExporterClient> mock1(new MockExporterClient1(exporter));
    std::shared_ptr<MetricExporterClient> mock2(new MockExporterClient2(mock1));
    MetricExporter::Register(mock2, kMockReportBatchSize);
  }

  void Shutdown() {
    MockExporterClient1::ResetCount();
    MockExporterClient2::ResetCount();
  }
};

int MockExporterClient1::client1_count;
double MockExporterClient1::lastest_hist_min;
double MockExporterClient1::lastest_hist_mean;
double MockExporterClient1::lastest_hist_max;
int MockExporterClient2::client2_count;
int MockExporterClient1::client1_value;
int MockExporterClient2::client2_value;

bool DoubleEqualTo(double value, double compared_value) {
  return value >= compared_value - 1e-5 && value <= compared_value + 1e-5;
}

TEST_F(MetricExporterClientTest, decorator_test) {
  // Export client should emit at least once in 10 seconds.
  for (size_t i = 0; i < 100; ++i) {
    stats::CurrentWorker().Record(i + 1);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(kReportFlushInterval + 20));
  ASSERT_GE(100, MockExporterClient1::GetValue());
  ASSERT_GE(100, MockExporterClient2::GetValue());
  ASSERT_EQ(1, MockExporterClient1::GetCount());
  ASSERT_EQ(1, MockExporterClient2::GetCount());
}

TEST_F(MetricExporterClientTest, exporter_client_caculation_test) {
  const stats::TagKeyType tag1 = stats::TagKeyType::Register("k1");
  const stats::TagKeyType tag2 = stats::TagKeyType::Register("k2");
  stats::Count random_counter("ray.random.counter", "", "", {tag1, tag2});
  stats::Gauge random_gauge("ray.random.gauge", "", "", {tag1, tag2});
  stats::Sum random_sum("ray.random.sum", "", "", {tag1, tag2});

  std::vector<double> hist_vector;
  for (int i = 0; i < 50; i++) {
    hist_vector.push_back((double)(i * 10.0));
  }
  stats::Histogram random_hist("ray.random.hist", "", "", hist_vector, {tag1, tag2});
  for (size_t i = 0; i < 500; ++i) {
    random_counter.Record(i, {{tag1, std::to_string(i)}, {tag2, std::to_string(i * 2)}});
    random_gauge.Record(i, {{tag1, std::to_string(i)}, {tag2, std::to_string(i * 2)}});
    random_sum.Record(i, {{tag1, std::to_string(i)}, {tag2, std::to_string(i * 2)}});
    random_hist.Record(i, {{tag1, std::to_string(i)}, {tag2, std::to_string(i * 2)}});
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(kReportFlushInterval + 20));
  RAY_LOG(INFO) << "Min " << MockExporterClient1::GetLastestHistMin() << ", mean "
                << MockExporterClient1::GetLastestHistMean() << ", max "
                << MockExporterClient1::GetLastestHistMax();
  ASSERT_TRUE(DoubleEqualTo(MockExporterClient1::GetLastestHistMin(), 0.0));
  ASSERT_TRUE(DoubleEqualTo(MockExporterClient1::GetLastestHistMean(), 249.5));
  ASSERT_TRUE(DoubleEqualTo(MockExporterClient1::GetLastestHistMax(), 499.0));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
