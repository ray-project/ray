#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/internal/stats_exporter_impl.h"
#include "ray/stats/stats.h"

#include "config/streaming_config.h"
#include "gtest/gtest.h"
#include "metrics/streaming_perf_metric.h"

using namespace ray::streaming;
using namespace ray;

class StreamingReporterCounterTest : public ::testing::Test {
 public:
  using UpdateFunc = std::function<void(size_t)>;

  void SetUp() {
    uint32_t kReportFlushInterval = 100;
    absl::Duration report_interval = absl::Milliseconds(kReportFlushInterval);
    absl::Duration harvest_interval = absl::Milliseconds(kReportFlushInterval / 2);
    ray::stats::StatsConfig::instance().SetReportInterval(report_interval);
    ray::stats::StatsConfig::instance().SetHarvestInterval(harvest_interval);
    const stats::TagsType global_tags = {{stats::ResourceNameKey, "CPU"}};
    std::shared_ptr<stats::MetricExporterClient> exporter(
        new stats::StdoutExporterClient());
    ray::stats::Init(global_tags, 10054, exporter);

    setenv("STREAMING_METRICS_MODE", "DEV", 1);
    setenv("ENABLE_RAY_STATS", "ON", 1);
    setenv("STREAMING_ENABLE_METRICS", "ON", 1);
    perf_counter_.reset(new StreamingReporter());

    const std::unordered_map<std::string, std::string> default_tags = {
        {"app", "s_test"}, {"cluster", "kmon-dev"}};
    metrics_conf_.SetMetricsGlobalTags(default_tags);
    perf_counter_->Start(metrics_conf_);
  }

  void TearDown() {
    opencensus::stats::DeltaProducer::Get()->Flush();
    opencensus::stats::StatsExporterImpl::Get()->Export();
    perf_counter_->Shutdown();
    ray::stats::Shutdown();
  }

  void RegisterAndRun(UpdateFunc update_handler) {
    auto stat_time_handler = [this](size_t thread_index, UpdateFunc update_handler) {
      auto start = std::chrono::high_resolution_clock::now();

      for (size_t loop_index = 0; loop_index < loop_update_times_; ++loop_index) {
        update_handler(loop_index);
      }

      auto end = std::chrono::high_resolution_clock::now();
      std::chrono::duration<double, std::micro> elapsed = end - start;

      std::string info = "Thread=" + std::to_string(thread_index) +
                         ", times=" + std::to_string(loop_update_times_) +
                         ", cost=" + std::to_string(elapsed.count()) + "us.";
      std::cout << info << std::endl;
    };

    for (size_t thread_index = 0; thread_index < op_thread_count_; ++thread_index) {
      thread_pool_.emplace_back(
          std::bind(stat_time_handler, thread_index, update_handler));
    }

    for (auto &thread : thread_pool_) {
      thread.join();
    }
  }

 protected:
  size_t op_thread_count_{4};
  size_t loop_update_times_{10};
  std::vector<std::thread> thread_pool_;

  StreamingMetricsConfig metrics_conf_;
  std::unique_ptr<StreamingReporter> perf_counter_;
};

TEST_F(StreamingReporterCounterTest, UpdateCounterWithOneKeyTest) {
  RegisterAndRun([this](size_t loop_index) {
    perf_counter_->UpdateCounter("domaina", "groupa", "a", loop_index);
  });
}

TEST_F(StreamingReporterCounterTest, UpdateCounterTest) {
  RegisterAndRun([this](size_t loop_index) {
    auto loop_index_str = std::to_string(loop_index % 10);
    perf_counter_->UpdateCounter("domaina" + loop_index_str, "groupa" + loop_index_str,
                                 "a" + loop_index_str, loop_index);
  });
}

TEST_F(StreamingReporterCounterTest, UpdateGaugeWithOneKeyTest) {
  RegisterAndRun([this](size_t loop_index) {
    std::unordered_map<std::string, std::string> tags;
    tags["tag1"] = "tag1";
    tags["tag2"] = std::to_string(loop_index);
    perf_counter_->UpdateGauge("streaming.test.gauge", tags, loop_index);
  });
}

TEST_F(StreamingReporterCounterTest, UpdateGaugeTest) {
  RegisterAndRun([this](size_t loop_index) {
    auto loop_index_str = std::to_string(loop_index % 10);
    perf_counter_->UpdateGauge("domaina" + loop_index_str, "groupa" + loop_index_str,
                               "a" + loop_index_str, loop_index);
  });
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
