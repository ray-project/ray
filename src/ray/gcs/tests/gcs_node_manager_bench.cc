// Copyright 2024 The Ray Authors.
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

#include <benchmark/benchmark.h>

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/str_format.h"
#include "fakes/ray/rpc/raylet/raylet_client.h"
#include "mock/ray/pubsub/publisher.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_node_manager.h"

namespace ray {
namespace gcs {

class GcsNodeManagerBenchmarkFixture : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State &state) override {
    auto raylet_client = std::make_shared<FakeRayletClient>();
    client_pool_ = std::make_unique<rpc::RayletClientPool>(
        [raylet_client = std::move(raylet_client)](const rpc::Address &) {
          return raylet_client;
        });
    gcs_publisher_ = std::make_unique<pubsub::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    io_context_ =
        std::make_unique<InstrumentedIOContextWithThread>("GcsNodeManagerBench");

    node_manager_ = std::make_unique<GcsNodeManager>(gcs_publisher_.get(),
                                                     gcs_table_storage_.get(),
                                                     io_context_->GetIoService(),
                                                     client_pool_.get(),
                                                     ClusterID::Nil());
    int node_count = state.range(0);
    int label_size = state.range(1);
    SetupNodes(node_count, label_size);
  }

  void TearDown(benchmark::State &state) override {
    // Cleanup
    node_manager_.reset();
    io_context_.reset();
    gcs_publisher_.reset();
    client_pool_.reset();
  }

 protected:
  void SetupNodes(int node_count, int label_size) {
    for (int i = 0; i < node_count; ++i) {
      auto node = std::make_shared<rpc::GcsNodeInfo>();
      // Fields included in Light version
      node->set_node_id(NodeID::FromRandom().Binary());
      node->set_node_manager_address(absl::StrFormat("192.168.1.%d", i % 255));
      node->set_node_manager_port(8000 + i);
      node->set_object_manager_port(9000 + i);
      node->set_state(rpc::GcsNodeInfo::ALIVE);
      node->set_instance_id(absl::StrFormat("instance_%d", i));
      node->set_node_type_name(absl::StrFormat("node_type_%d", i % 3));
      node->set_instance_type_name(absl::StrFormat("m5.large_%d", i % 5));

      // Add resources
      auto resources = node->mutable_resources_total();
      (*resources)["CPU"] = 8.0;
      (*resources)["memory"] = 32000000000.0;
      (*resources)[absl::StrFormat("node:__internal_head__")] = 1.0;

      // Fields EXCLUDED from the light version (these add to the data size)
      node->set_node_name(absl::StrFormat("node_%d", i));
      node->set_raylet_socket_name(
          absl::StrFormat("/tmp/ray/session_latest/sockets/raylet.%d", i));
      node->set_object_store_socket_name(
          absl::StrFormat("/tmp/ray/session_latest/sockets/plasma_store.%d", i));
      node->set_node_manager_hostname(
          absl::StrFormat("ip-172-31-47-%d.ec2.internal", i % 255));
      node->set_metrics_export_port(8080 + i);
      node->set_runtime_env_agent_port(10001 + i);
      node->set_start_time_ms(1700000000000 + i * 1000);
      node->set_end_time_ms(0);  // Still running
      node->set_is_head_node(i == 0);

      // Add death_info for some nodes
      if (i % 10 == 0 && i > 0) {
        auto death_info = node->mutable_death_info();
        death_info->set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
        death_info->set_reason_message("Simulated node failure for testing");
      }

      // Add varying number of labels based on label_size parameter
      auto labels = node->mutable_labels();
      for (int j = 0; j < label_size; ++j) {
        std::string key = absl::StrFormat("label_key_%d", j);
        std::string value = absl::StrFormat(
            "label_value_%d_%s", j, std::string(100, 'x').c_str());  // 100 char values
        (*labels)[key] = value;
      }

      node_manager_->AddNode(node);
    }
  }

  std::unique_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::unique_ptr<rpc::RayletClientPool> client_pool_;
  std::unique_ptr<pubsub::GcsPublisher> gcs_publisher_;
  std::unique_ptr<InstrumentedIOContextWithThread> io_context_;
  std::unique_ptr<GcsNodeManager> node_manager_;
};

BENCHMARK_DEFINE_F(GcsNodeManagerBenchmarkFixture, HandleGetAllNodeInfo)
(benchmark::State &state) {
  size_t total_bytes = 0;

  for (auto _ : state) {
    rpc::GetAllNodeInfoRequest request;
    rpc::GetAllNodeInfoReply reply;

    bool callback_called = false;
    auto send_reply_callback =
        [&callback_called](Status status, std::function<void()>, std::function<void()>) {
          callback_called = true;
        };

    node_manager_->HandleGetAllNodeInfo(request, &reply, send_reply_callback);

    if (!callback_called) {
      state.SkipWithError("Callback not called");
      return;
    }

    // Calculate response size for reporting
    benchmark::DoNotOptimize(reply);
    for (const auto &node : reply.node_info_list()) {
      total_bytes += node.ByteSizeLong();
    }
  }

  // Report metrics
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(total_bytes);
  state.SetLabel(absl::StrFormat("nodes=%d,labels=%d",
                                 static_cast<int>(state.range(0)),
                                 static_cast<int>(state.range(1))));
}

BENCHMARK_DEFINE_F(GcsNodeManagerBenchmarkFixture, HandleGetAllNodeAddressAndLiveness)
(benchmark::State &state) {
  size_t total_bytes = 0;

  for (auto _ : state) {
    rpc::GetAllNodeAddressAndLivenessRequest request;
    rpc::GetAllNodeAddressAndLivenessReply reply;

    bool callback_called = false;
    auto send_reply_callback =
        [&callback_called](Status status, std::function<void()>, std::function<void()>) {
          callback_called = true;
        };

    node_manager_->HandleGetAllNodeAddressAndLiveness(
        request, &reply, send_reply_callback);

    if (!callback_called) {
      state.SkipWithError("Callback not called");
      return;
    }

    // Calculate response size for reporting
    benchmark::DoNotOptimize(reply);
    for (const auto &node : reply.node_info_list()) {
      total_bytes += node.ByteSizeLong();
    }
  }

  // Report metrics
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(total_bytes);
  state.SetLabel(absl::StrFormat("nodes=%d,labels=%d",
                                 static_cast<int>(state.range(0)),
                                 static_cast<int>(state.range(1))));
}

// Register benchmarks with various configurations
// Args: {node_count, label_count}
BENCHMARK_REGISTER_F(GcsNodeManagerBenchmarkFixture, HandleGetAllNodeInfo)
    ->Args({10, 0})     // Small cluster, no labels
    ->Args({10, 5})     // Small cluster, few labels
    ->Args({10, 20})    // Small cluster, moderate labels
    ->Args({10, 50})    // Small cluster, many labels
    ->Args({100, 0})    // Medium cluster, no labels
    ->Args({100, 5})    // Medium cluster, few labels
    ->Args({100, 20})   // Medium cluster, moderate labels
    ->Args({100, 50})   // Medium cluster, many labels
    ->Args({1000, 5})   // Large cluster, few labels
    ->Args({1000, 20})  // Large cluster, moderate labels
    ->Args({5000, 5})   // Very large cluster, few labels
    ->Args({10000, 5})  // Huge cluster, few labels
    ->Args({30000, 5})  // Massive cluster, few labels
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(GcsNodeManagerBenchmarkFixture, HandleGetAllNodeAddressAndLiveness)
    ->Args({10, 0})     // Small cluster, no labels
    ->Args({10, 5})     // Small cluster, few labels
    ->Args({10, 20})    // Small cluster, moderate labels
    ->Args({10, 50})    // Small cluster, many labels
    ->Args({100, 0})    // Medium cluster, no labels
    ->Args({100, 5})    // Medium cluster, few labels
    ->Args({100, 20})   // Medium cluster, moderate labels
    ->Args({100, 50})   // Medium cluster, many labels
    ->Args({1000, 5})   // Large cluster, few labels
    ->Args({1000, 20})  // Large cluster, moderate labels
    ->Args({5000, 5})   // Very large cluster, few labels
    ->Args({10000, 5})  // Huge cluster, few labels
    ->Args({30000, 5})  // Massive cluster, few labels
    ->Unit(benchmark::kMicrosecond);

}  // namespace gcs
}  // namespace ray

BENCHMARK_MAIN();
