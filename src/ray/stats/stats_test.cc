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
          if (descriptor.columns()[i].name() == "NodeAddress") {
            ASSERT_EQ("Localhost", row.first[i]);
          }
        }
        // row.second store the data of this metric.
        ASSERT_EQ(2345, row.second);
      }
    }
  }
};

class StatsTest : public ::testing::Test {
 public:
  void SetUp() {
    ray::stats::Init("127.0.0.1:8888", {{stats::NodeAddressKey, "Localhost"}}, false);
    MockExporter::Register();
  }

  void Shutdown() {}
};

TEST_F(StatsTest, F) {
  for (size_t i = 0; i < 500; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    stats::CurrentWorker().Record(2345);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
