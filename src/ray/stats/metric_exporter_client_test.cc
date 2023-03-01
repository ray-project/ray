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

#include "ray/stats/metric_exporter_client.h"

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "absl/memory/memory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/internal/stats_exporter_impl.h"
#include "ray/stats/metric_defs.h"
#include "ray/stats/metric_exporter.h"
#include "ray/stats/stats.h"

namespace ray {
using namespace stats;

const size_t kMockReportBatchSize = 10;
const int MetricsAgentPort = 10054;

class MockExporterClient1 : public MetricExporterDecorator {
 public:
  MockExporterClient1(std::shared_ptr<MetricExporterClient> exporter)
      : MetricExporterDecorator(exporter),
        client1_count(0),
        client1_value(0),
        lastest_hist_min(0.0),
        lastest_hist_mean(0.0),
        lastest_hist_max(0.0) {}

  void ReportMetrics(const std::vector<MetricPoint> &points) override {
    if (points.empty()) {
      return;
    }
    MetricExporterDecorator::ReportMetrics(points);
    client1_count++;
    client1_value = points.back().value;
    RAY_LOG(DEBUG) << "Client 1 " << client1_count << " last metric "
                   << points.back().metric_name << ", value " << points.back().value;
    RecordLastHistData(points);
    // Point size must be less than or equal to report batch size.
    ASSERT_GE(kMockReportBatchSize, points.size());
  }
  int GetCount() { return client1_count; }
  int GetValue() { return client1_value; }
  double GetLastestHistMin() { return lastest_hist_min; }
  double GetLastestHistMean() { return lastest_hist_mean; }
  double GetLastestHistMax() { return lastest_hist_max; }

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
  int client1_count;
  int client1_value;
  double lastest_hist_min;
  double lastest_hist_mean;
  double lastest_hist_max;
};

class MockExporterClient2 : public MetricExporterDecorator {
 public:
  MockExporterClient2(std::shared_ptr<MetricExporterClient> exporter)
      : MetricExporterDecorator(exporter), client2_count(0), client2_value(0) {}
  void ReportMetrics(const std::vector<MetricPoint> &points) override {
    if (points.empty()) {
      return;
    }
    MetricExporterDecorator::ReportMetrics(points);
    client2_count++;
    RAY_LOG(DEBUG) << "Client 2 " << client2_count << " last metric "
                   << points.back().metric_name << ", value " << points.back().value;
    client2_value = points.back().value;
  }
  int GetCount() { return client2_count; }
  int GetValue() { return client2_value; }

 private:
  int client2_count;
  int client2_value;
};

/// Default report flush interval is 500ms, so we may wait a while for data
/// exporting.
uint32_t kReportFlushInterval = 500;

class MetricExporterClientTest : public ::testing::Test {
 public:
  virtual void SetUp() override {
    const stats::TagsType global_tags = {{stats::LanguageKey, "CPP"},
                                         {stats::WorkerPidKey, "1000"}};
    absl::Duration report_interval = absl::Milliseconds(kReportFlushInterval);
    absl::Duration harvest_interval = absl::Milliseconds(kReportFlushInterval / 2);
    ray::stats::StatsConfig::instance().SetReportInterval(report_interval);
    ray::stats::StatsConfig::instance().SetHarvestInterval(harvest_interval);

    exporter.reset(new stats::StdoutExporterClient());
    mock1.reset(new MockExporterClient1(exporter));
    mock2.reset(new MockExporterClient2(mock1));
    ray::stats::Init(
        global_tags, MetricsAgentPort, WorkerID::Nil(), mock2, kMockReportBatchSize);
  }

  virtual void TearDown() override { Shutdown(); }

  void Shutdown() { ray::stats::Shutdown(); }

 protected:
  std::shared_ptr<MetricExporterClient> exporter;
  std::shared_ptr<MockExporterClient1> mock1;
  std::shared_ptr<MockExporterClient2> mock2;
};

bool DoubleEqualTo(double value, double compared_value) {
  return value >= compared_value - 1e-5 && value <= compared_value + 1e-5;
}

TEST_F(MetricExporterClientTest, decorator_test) {
  // Export client should emit at least once in report flush interval.
  for (size_t i = 0; i < 100; ++i) {
    stats::LiveActors().Record(i + 1);
  }
  opencensus::stats::DeltaProducer::Get()->Flush();
  opencensus::stats::StatsExporterImpl::Get()->Export();
  ASSERT_GE(100, mock1->GetValue());
  ASSERT_EQ(1, mock1->GetCount());
  ASSERT_GE(100, mock2->GetValue());
  ASSERT_EQ(1, mock2->GetCount());
}

TEST_F(MetricExporterClientTest, exporter_client_caculation_test) {
  const stats::TagKeyType tag1 = stats::TagKeyType::Register("k1");
  const stats::TagKeyType tag2 = stats::TagKeyType::Register("k2");
  static stats::Count random_counter("ray.random.counter", "", "", {tag1, tag2});
  static stats::Gauge random_gauge("ray.random.gauge", "", "", {tag1, tag2});
  static stats::Sum random_sum("ray.random.sum", "", "", {tag1, tag2});

  std::vector<double> hist_vector;
  for (int i = 0; i < 50; i++) {
    hist_vector.push_back((double)(i * 10.0));
  }
  static stats::Histogram random_hist(
      "ray.random.hist", "", "", hist_vector, {tag1, tag2});
  for (size_t i = 0; i < 500; ++i) {
    random_counter.Record(i, {{tag1, std::to_string(i)}, {tag2, std::to_string(i * 2)}});
    random_gauge.Record(i, {{tag1, std::to_string(i)}, {tag2, std::to_string(i * 2)}});
    random_sum.Record(i, {{tag1, std::to_string(i)}, {tag2, std::to_string(i * 2)}});
    random_hist.Record(i, {{tag1, std::to_string(i)}, {tag2, std::to_string(i * 2)}});
  }
  opencensus::stats::DeltaProducer::Get()->Flush();
  opencensus::stats::StatsExporterImpl::Get()->Export();
  RAY_LOG(INFO) << "Min " << mock1->GetLastestHistMin() << ", mean "
                << mock1->GetLastestHistMean() << ", max " << mock1->GetLastestHistMax();
  ASSERT_TRUE(DoubleEqualTo(mock1->GetLastestHistMin(), 0.0));
  ASSERT_TRUE(DoubleEqualTo(mock1->GetLastestHistMean(), 249.5));
  ASSERT_TRUE(DoubleEqualTo(mock1->GetLastestHistMax(), 499.0));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
