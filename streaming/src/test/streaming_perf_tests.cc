#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "ray/metrics/metrics_util.h"
#include "streaming.h"
#include "streaming_config.h"
#include "streaming_perf_metric.h"

using namespace ray::metrics;
using namespace ray::streaming;

class MockMetricsConf : public MetricsConf {
 public:
  MockMetricsConf() : MetricsConf() {}

  void SetRegistryName(const std::string &registry_name) {
    this->registry_name_ = registry_name;
  }

  void SetReporterName(const std::string &reporter_name) {
    this->reporter_name_ = reporter_name;
  }

  void SetRegistryOption(const RegistryOption &registry_option) {
    this->registry_options_ = registry_option;
  }

  void SetReporterOption(const ReporterOption &reporter_option) {
    this->reporter_options_ = reporter_option;
  }
};

class StreamingPerfCounterTest : public ::testing::Test {
 public:
  using UpdateFunc = std::function<void(size_t)>;

  void SetUp() {
    setenv("STREAMING_METRICS_MODE", "DEV", 1);
    setenv("STREAMING_ENABLE_METRICS", "ON", 1);
    metrics_conf_.SetRegistryName(kMetricsOptionEmptyName);
    metrics_conf_.SetReporterName(kMetricsOptionEmptyName);
    perf_counter_.reset(new StreamingPerf());
    StreamingMetricsConfig metrics_config;

    registry_options_.default_tag_map_ = {{"app", metrics_config.GetMetrics_app_name()},
                                          {"cluster", "kmon-dev"}};
    // metric job name is streaming that's not true user-defined
    reporter_options_.job_name_ = metrics_config.GetMetrics_job_name();
    reporter_options_.service_addr_ = metrics_config.GetMetrics_url();
    reporter_options_.report_interval_ = std::chrono::seconds(1);

    metrics_conf_.SetRegistryOption(registry_options_);
    metrics_conf_.SetReporterOption(reporter_options_);
    perf_counter_->Start(metrics_conf_);
  }

  void TearDown() { perf_counter_->Shutdown(); }

  void RegisterAndRun(MetricType type, UpdateFunc update_handler) {
    auto stat_time_handler = [type, this](size_t thread_index,
                                          UpdateFunc update_handler) {
      auto start = std::chrono::high_resolution_clock::now();

      for (size_t loop_index = 0; loop_index < loop_update_times_; ++loop_index) {
        update_handler(loop_index);
      }

      auto end = std::chrono::high_resolution_clock::now();
      std::chrono::duration<double, std::micro> elapsed = end - start;

      std::string info = "Thread=" + std::to_string(thread_index) +
                         ", type=" + std::to_string(static_cast<int>(type)) +
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

  MockMetricsConf metrics_conf_;
  std::unique_ptr<StreamingPerf> perf_counter_;
  RegistryOption registry_options_;
  ReporterOption reporter_options_;
};

TEST_F(StreamingPerfCounterTest, UpdateCounterWithOneKeyTest) {
  RegisterAndRun(MetricType::kCounter, [this](size_t loop_index) {
    perf_counter_->UpdateCounter("domaina", "groupa", "a", loop_index);
  });
}

TEST_F(StreamingPerfCounterTest, UpdateCounterTest) {
  RegisterAndRun(MetricType::kCounter, [this](size_t loop_index) {
    auto loop_index_str = std::to_string(loop_index % 10);
    perf_counter_->UpdateCounter("domaina" + loop_index_str, "groupa" + loop_index_str,
                                 "a" + loop_index_str, loop_index);
  });
}

TEST_F(StreamingPerfCounterTest, UpdateGaugeWithOneKeyTest) {
  RegisterAndRun(MetricType::kGauge, [this](size_t loop_index) {
    std::map<std::string, std::string> tags;
    tags["tag1"] = "tag1";
    tags["tag2"] = std::to_string(loop_index);
    perf_counter_->UpdateGauge("streaming.test.gauge", tags, loop_index);
  });
}

TEST_F(StreamingPerfCounterTest, UpdateGaugeTest) {
  RegisterAndRun(MetricType::kGauge, [this](size_t loop_index) {
    auto loop_index_str = std::to_string(loop_index % 10);
    perf_counter_->UpdateGauge("domaina" + loop_index_str, "groupa" + loop_index_str,
                               "a" + loop_index_str, loop_index);
  });
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
