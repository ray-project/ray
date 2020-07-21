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
#include "ray/stats/stats.h"

namespace ray {

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

      ASSERT_EQ("current_worker", descriptor.name());
      ASSERT_EQ(opencensus::stats::ViewData::Type::kDouble, view_data.type());
      for (const auto row : view_data.double_data()) {
        for (size_t i = 0; i < descriptor.columns().size(); ++i) {
          if (descriptor.columns()[i].name() == "WorkerPidKey") {
            ASSERT_EQ("1000", row.first[i]);
          } else if (descriptor.columns()[i].name() == "LanguageKey") {
            ASSERT_EQ("CPP", row.first[i]);
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
  void SetUp() {
    absl::Duration report_interval = absl::Milliseconds(kReportFlushInterval);
    absl::Duration harvest_interval = absl::Milliseconds(kReportFlushInterval / 2);
    ray::stats::StatsConfig::instance().SetReportInterval(report_interval);
    ray::stats::StatsConfig::instance().SetHarvestInterval(harvest_interval);
    const stats::TagsType global_tags = {{stats::LanguageKey, "CPP"},
                                         {stats::WorkerPidKey, "1000"}};
    std::shared_ptr<stats::MetricExporterClient> exporter(
        new stats::StdoutExporterClient());
    ray::stats::Init(global_tags, 10054, io_service_, exporter);
    MockExporter::Register();
  }

  void Shutdown() {}

 private:
  boost::asio::io_service io_service_;
};

TEST_F(StatsTest, F) {
  for (size_t i = 0; i < 20; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    stats::CurrentWorker().Record(2345);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
