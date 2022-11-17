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

#include "ray/stats/stats.h"

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "absl/memory/memory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/stats/metric_defs.h"

DEFINE_stats(test_hist,
             "TestStats",
             ("method", "method2"),
             (1.0, 2.0, 3.0, 4.0),
             ray::stats::HISTOGRAM);
DEFINE_stats(test_2,
             "TestStats",
             ("method", "method2"),
             (1.0),
             ray::stats::COUNT,
             ray::stats::SUM);
DEFINE_stats(test, "TestStats", ("method"), (1.0), ray::stats::COUNT, ray::stats::SUM);
DEFINE_stats(
    test_declare, "TestStats2", ("tag1"), (1.0), ray::stats::COUNT, ray::stats::SUM);
DECLARE_stats(test_declare);

namespace ray {

const int MetricsAgentPort = 10054;

class MockExporter : public opencensus::stats::StatsExporter::Handler {
 public:
  static void Register() {
    opencensus::stats::StatsExporter::RegisterPushHandler(
        absl::make_unique<MockExporter>());
  }

  void ExportViewData(
      const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                  opencensus::stats::ViewData>> &data) override {
    for (const auto &datum : data) {
      auto &descriptor = datum.first;
      auto &view_data = datum.second;

      ASSERT_EQ("local_available_resource", descriptor.name());
      ASSERT_EQ(opencensus::stats::ViewData::Type::kDouble, view_data.type());
      for (const auto &row : view_data.double_data()) {
        for (size_t i = 0; i < descriptor.columns().size(); ++i) {
          if (descriptor.columns()[i].name() == "ResourceName") {
            ASSERT_EQ("CPU", row.first[i]);
          }
        }
        // row.second store the data of this metric.
        ASSERT_EQ(2345, row.second);
      }
    }
  }
};

/// Default report flush interval is 500ms, so we may wait a while for data
/// exporting.
uint32_t kReportFlushInterval = 500;

class StatsTest : public ::testing::Test {
 public:
  void SetUp() override {
    absl::Duration report_interval = absl::Milliseconds(kReportFlushInterval);
    absl::Duration harvest_interval = absl::Milliseconds(kReportFlushInterval / 2);
    ray::stats::StatsConfig::instance().SetReportInterval(report_interval);
    ray::stats::StatsConfig::instance().SetHarvestInterval(harvest_interval);
    const stats::TagsType global_tags = {{stats::ResourceNameKey, "CPU"}};
    std::shared_ptr<stats::MetricExporterClient> exporter(
        new stats::StdoutExporterClient());
    ray::stats::Init(global_tags, MetricsAgentPort, WorkerID::Nil(), exporter);
    MockExporter::Register();
  }

  virtual void TearDown() override { Shutdown(); }

  void Shutdown() { ray::stats::Shutdown(); }
};

TEST_F(StatsTest, F) {
  for (size_t i = 0; i < 20; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    stats::TestMetrics().Record(2345);
  }
}

TEST_F(StatsTest, InitializationTest) {
  // Do initialization multiple times and make sure only the first initialization
  // was applied.
  ASSERT_TRUE(ray::stats::StatsConfig::instance().IsInitialized());
  auto test_tag_value_that_shouldnt_be_applied = "TEST";
  for (size_t i = 0; i < 20; ++i) {
    std::shared_ptr<stats::MetricExporterClient> exporter(
        new stats::StdoutExporterClient());
    ray::stats::Init({{stats::LanguageKey, test_tag_value_that_shouldnt_be_applied}},
                     MetricsAgentPort,
                     WorkerID::Nil(),
                     exporter);
  }

  auto &first_tag = ray::stats::StatsConfig::instance().GetGlobalTags()[0];
  ASSERT_TRUE(first_tag.second != test_tag_value_that_shouldnt_be_applied);

  ray::stats::Shutdown();
  ASSERT_FALSE(ray::stats::StatsConfig::instance().IsInitialized());

  // Reinitialize. It should be initialized now.
  const stats::TagsType global_tags = {
      {stats::LanguageKey, test_tag_value_that_shouldnt_be_applied}};
  std::shared_ptr<stats::MetricExporterClient> exporter(
      new stats::StdoutExporterClient());

  ray::stats::Init(global_tags, MetricsAgentPort, WorkerID::Nil(), exporter);
  ASSERT_TRUE(ray::stats::StatsConfig::instance().IsInitialized());
  auto &new_first_tag = ray::stats::StatsConfig::instance().GetGlobalTags()[0];
  ASSERT_TRUE(new_first_tag.second == test_tag_value_that_shouldnt_be_applied);
}

TEST(Metric, MultiThreadMetricRegisterViewTest) {
  ray::stats::Shutdown();
  std::shared_ptr<stats::MetricExporterClient> exporter(
      new stats::StdoutExporterClient());
  ray::stats::Init({}, MetricsAgentPort, WorkerID::Nil(), exporter);
  std::vector<std::thread> threads;
  const stats::TagKeyType tag1 = stats::TagKeyType::Register("k1");
  const stats::TagKeyType tag2 = stats::TagKeyType::Register("k2");
  for (int index = 0; index < 10; ++index) {
    threads.emplace_back([tag1, tag2, index]() {
      for (int i = 0; i < 100; i++) {
        stats::Count random_counter(
            "ray.random.counter" + std::to_string(index) + std::to_string(i),
            "",
            "",
            {tag1, tag2});
        random_counter.Record(i);
        stats::Gauge random_gauge(
            "ray.random.gauge" + std::to_string(index) + std::to_string(i),
            "",
            "",
            {tag1, tag2});
        random_gauge.Record(i);
        stats::Sum random_sum(
            "ray.random.sum" + std::to_string(index) + std::to_string(i),
            "",
            "",
            {tag1, tag2});
        random_sum.Record(i);
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  ray::stats::Shutdown();
}

TEST_F(StatsTest, MultiThreadedInitializationTest) {
  // Make sure stats module is thread-safe.
  // Shutdown the stats module first.
  ray::stats::Shutdown();
  // Spawn 10 threads that init and shutdown again and again.
  // The test will have memory corruption if it doesn't work as expected.
  const stats::TagsType global_tags = {{stats::LanguageKey, "CPP"},
                                       {stats::WorkerPidKey, "1000"}};
  std::vector<std::thread> threads;
  for (int i = 0; i < 5; i++) {
    threads.emplace_back([global_tags]() {
      for (int i = 0; i < 5; i++) {
        std::shared_ptr<stats::MetricExporterClient> exporter(
            new stats::StdoutExporterClient());
        unsigned int upper_bound = 100;
        unsigned int init_or_shutdown = (rand() % upper_bound);
        if (init_or_shutdown >= (upper_bound / 2)) {
          ray::stats::Init(global_tags, MetricsAgentPort, WorkerID::Nil(), exporter);
        } else {
          ray::stats::Shutdown();
        }
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  ray::stats::Shutdown();
  ASSERT_FALSE(ray::stats::StatsConfig::instance().IsInitialized());
  std::shared_ptr<stats::MetricExporterClient> exporter(
      new stats::StdoutExporterClient());
  ray::stats::Init(global_tags, MetricsAgentPort, WorkerID::Nil(), exporter);
  ASSERT_TRUE(ray::stats::StatsConfig::instance().IsInitialized());
}

TEST_F(StatsTest, TestShutdownTakesLongTime) {
  // Make sure it doesn't take long time to shutdown when harvestor / export interval is
  // large.
  ray::stats::Shutdown();
  // Spawn 10 threads that init and shutdown again and again.
  // The test will have memory corruption if it doesn't work as expected.
  const stats::TagsType global_tags = {{stats::LanguageKey, "CPP"},
                                       {stats::WorkerPidKey, "1000"}};
  std::shared_ptr<stats::MetricExporterClient> exporter(
      new stats::StdoutExporterClient());

  // Flush interval is 30 seconds. Shutdown should not take 30 seconds in this case.
  uint32_t kReportFlushInterval = 30000;
  absl::Duration report_interval = absl::Milliseconds(kReportFlushInterval);
  absl::Duration harvest_interval = absl::Milliseconds(kReportFlushInterval);
  ray::stats::StatsConfig::instance().SetReportInterval(report_interval);
  ray::stats::StatsConfig::instance().SetHarvestInterval(harvest_interval);
  ray::stats::Init(global_tags, MetricsAgentPort, WorkerID::Nil(), exporter);
  ray::stats::Shutdown();
}

TEST_F(StatsTest, STAT_DEF) {
  ray::stats::Shutdown();
  std::shared_ptr<stats::MetricExporterClient> exporter(
      new stats::StdoutExporterClient());
  ray::stats::Init({}, MetricsAgentPort, WorkerID::Nil(), exporter);
  STATS_test.Record(1.0);
  STATS_test_declare.Record(1.0, "Test");
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
