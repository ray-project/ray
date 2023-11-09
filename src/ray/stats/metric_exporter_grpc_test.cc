// Copyright 2023 The Ray Authors.
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

#include <chrono>
#include <iostream>
#include <vector>

#include "absl/memory/memory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/internal/stats_exporter_impl.h"
#include "opencensus/stats/stats.h"
#include "ray/stats/metric_defs.h"
#include "ray/stats/metric_exporter.h"
#include "ray/stats/metric_exporter_client.h"
#include "ray/stats/stats.h"
#include "ray/util/logging.h"

namespace ray {

using namespace stats;
using opencensus::stats::Aggregation;
using opencensus::stats::BucketBoundaries;
using opencensus::stats::MeasureInt64;
using opencensus::stats::ViewData;
using opencensus::stats::ViewDescriptor;
using opencensus::tags::TagKey;
using ::testing::UnorderedPointwise;

struct ExpectedPoint {
  std::string metric_name;
  // Not recording timestamp because we don't have a way to expect it.
  double value;
  std::unordered_map<std::string, std::string> tags;

  constexpr static double epsilion = 0.00001;
  bool operator==(const ExpectedPoint &rhs) const {
    return metric_name == rhs.metric_name && fabs(value - rhs.value) < epsilion &&
           tags == rhs.tags;
  }
};

std::ostream &operator<<(std::ostream &os, const ExpectedPoint &point) {
  os << "{.metric_name=\"" << point.metric_name << "\",.value=" << point.value
     << ",.tags={";
  for (const auto &[k, v] : point.tags) {
    os << "{\"" << k << "\",\"" << v << "\"},";
  }
  os << "}";
  return os;
}

class MockExporterClientKeepsAll : public MetricExporterClient {
 public:
  MockExporterClientKeepsAll() {}

  void ReportMetrics(const std::vector<MetricPoint> &points) override {
    for (const auto &point : points) {
      points_.push_back({
          point.metric_name,
          point.value,
          point.tags,
      });
      RAY_LOG(INFO) << "received metrics " << point.metric_name << ", " << point.timestamp
                    << ", " << point.value << ", ";
      for (const auto &[k, v] : point.tags) {
        RAY_LOG(INFO) << "tag" << k << " = " << v;
      }
    }
  }

  const std::vector<ExpectedPoint> &ReportedMetrics() const { return points_; }

 private:
  std::vector<ExpectedPoint> points_;
};

const size_t kMockReportBatchSize = 10;
const int MetricsAgentPort = 10054;
const auto method_tag_key = TagKey::Register("grpc_client_method");
const auto status_tag_key = TagKey::Register("grpc_client_status");

MeasureInt64 RpcClientReceivedMessagesPerRpc() {
  static const auto measure = MeasureInt64::Register(
      "grpc.io/client/sent_messages_per_rpc", "Number of messages received per RPC", "1");
  return measure;
}

const static opencensus::stats::ViewDescriptor descriptor =
    ViewDescriptor()
        .set_name("grpc.io/client/received_messages_per_rpc/cumulative")
        .set_measure(RpcClientReceivedMessagesPerRpc().GetDescriptor().name())
        .set_aggregation(Aggregation::Distribution(
            opencensus::stats::BucketBoundaries::Exponential(17, 1.0, 2.0)))
        .add_column(method_tag_key);

TEST(MetricPointExporterTest, adds_global_tags_to_grpc) {
  RpcClientReceivedMessagesPerRpc();
  descriptor.RegisterForExport();

  auto exporter = std::make_shared<MockExporterClientKeepsAll>();

  const stats::TagsType global_tags = {{stats::LanguageKey, "CPP"},
                                       {stats::WorkerPidKey, "1000"}};
  ray::stats::Init(
      global_tags, MetricsAgentPort, WorkerID::Nil(), exporter, kMockReportBatchSize);

  opencensus::stats::Record(
      {{RpcClientReceivedMessagesPerRpc(), 1}},
      {{method_tag_key, "MyService.myMethod"}, {status_tag_key, "OK"}});

  opencensus::stats::DeltaProducer::Get()->Flush();
  opencensus::stats::StatsExporterImpl::Get()->Export();

  const auto &out_data = exporter->ReportedMetrics();

  // Expected tags:
  // - "grpc_client_method" as registered in the ViewDescriptor
  // - NO "grpc_client_status" although it's recorded
  // - global tags "tagWorkerPid" and "tagLanguage".
  std::unordered_map<std::string, std::string> expected_tags{
      {"grpc_client_method", "MyService.myMethod"},
      {"WorkerPid", "1000"},
      {"Language", "CPP"}};

  ASSERT_THAT(
      out_data,
      testing::UnorderedElementsAre(
          ExpectedPoint{/*.metric_name=*/"grpc.io/client/sent_messages_per_rpc.mean",
                        /*.value=*/1.0,
                        /*.tags=*/expected_tags},
          ExpectedPoint{/*.metric_name=*/"grpc.io/client/sent_messages_per_rpc.min",
                        /*.value=*/1.0,
                        /*.tags=*/expected_tags},
          ExpectedPoint{/*.metric_name=*/"grpc.io/client/sent_messages_per_rpc.max",
                        /*.value=*/1.0,
                        /*.tags=*/expected_tags}));
  ray::stats::Shutdown();
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
