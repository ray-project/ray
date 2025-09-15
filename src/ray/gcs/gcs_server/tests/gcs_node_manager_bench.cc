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

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "absl/strings/str_format.h"
#include "fakes/ray/rpc/raylet/raylet_client.h"
#include "mock/ray/pubsub/publisher.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"

namespace ray {
namespace gcs {

class GcsNodeManagerBenchmark {
 public:
  GcsNodeManagerBenchmark() {
    // Set up dependencies similar to the test
    auto raylet_client = std::make_shared<FakeRayletClient>();
    client_pool_ = std::make_unique<rpc::RayletClientPool>(
        [raylet_client = std::move(raylet_client)](const rpc::Address &) {
          return raylet_client;
        });
    gcs_publisher_ = std::make_unique<pubsub::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    io_context_ = std::make_unique<InstrumentedIOContextWithThread>("GcsNodeManagerBench");

    node_manager_ = std::make_unique<GcsNodeManager>(
        gcs_publisher_.get(),
        gcs_table_storage_.get(),  // nullptr is fine
        io_context_->GetIoService(),
        client_pool_.get(),
        ClusterID::Nil());
  }

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

      // Add resources (included in Light version)
      auto resources = node->mutable_resources_total();
      (*resources)["CPU"] = 8.0;
      (*resources)["memory"] = 32000000000.0;
      (*resources)[absl::StrFormat("node:__internal_head__")] = 1.0;

      // Fields EXCLUDED from Light version (these add to the data size)
      node->set_node_name(absl::StrFormat("node_%d", i));
      node->set_raylet_socket_name(absl::StrFormat("/tmp/ray/session_latest/sockets/raylet.%d", i));
      node->set_object_store_socket_name(absl::StrFormat("/tmp/ray/session_latest/sockets/plasma_store.%d", i));
      node->set_node_manager_hostname(absl::StrFormat("ip-172-31-47-%d.ec2.internal", i % 255));
      node->set_metrics_export_port(8080 + i);
      node->set_runtime_env_agent_port(10001 + i);
      node->set_start_time_ms(1700000000000 + i * 1000);
      node->set_end_time_ms(0);  // Still running
      node->set_is_head_node(i == 0);

      // Add death_info for some nodes (excluded from Light)
      if (i % 10 == 0 && i > 0) {
        auto death_info = node->mutable_death_info();
        death_info->set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
        death_info->set_reason_message("Simulated node failure for testing");
      }

      // Add varying number of labels based on label_size parameter (excluded from Light)
      auto labels = node->mutable_labels();
      for (int j = 0; j < label_size; ++j) {
        std::string key = absl::StrFormat("label_key_%d", j);
        std::string value = absl::StrFormat("label_value_%d_%s", j,
            std::string(100, 'x').c_str()); // 100 char values
        (*labels)[key] = value;
      }

      node_manager_->AddNode(node);
    }
  }

  void BenchmarkGetAllNodeInfo(int iterations) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t total_bytes = 0;
    for (int i = 0; i < iterations; ++i) {
      rpc::GetAllNodeInfoRequest request;
      rpc::GetAllNodeInfoReply reply;

      bool callback_called = false;
      auto send_reply_callback = [&callback_called](Status status,
                                                    std::function<void()>,
                                                    std::function<void()>) {
        callback_called = true;
      };

      node_manager_->HandleGetAllNodeInfo(request, &reply, send_reply_callback);

      if (!callback_called) {
        std::cerr << "Error: Callback not called" << std::endl;
        return;
      }

      // Calculate response size
      for (const auto& node : reply.node_info_list()) {
        total_bytes += node.ByteSizeLong();
      }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    std::cout << "  HandleGetAllNodeInfo: "
              << duration.count() / iterations << " us/op, "
              << "total bytes: " << total_bytes / iterations << std::endl;
  }

  void BenchmarkGetAllNodeInfoLight(int iterations) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t total_bytes = 0;
    for (int i = 0; i < iterations; ++i) {
      rpc::GetAllNodeInfoLightRequest request;
      rpc::GetAllNodeInfoLightReply reply;

      bool callback_called = false;
      auto send_reply_callback = [&callback_called](Status status,
                                                    std::function<void()>,
                                                    std::function<void()>) {
        callback_called = true;
      };

      node_manager_->HandleGetAllNodeInfoLight(request, &reply, send_reply_callback);

      if (!callback_called) {
        std::cerr << "Error: Callback not called" << std::endl;
        return;
      }

      // Calculate response size
      for (const auto& node : reply.node_info_list()) {
        total_bytes += node.ByteSizeLong();
      }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    std::cout << "  HandleGetAllNodeInfoLight: "
              << duration.count() / iterations << " us/op, "
              << "total bytes: " << total_bytes / iterations << std::endl;
  }

 private:
  std::unique_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::unique_ptr<rpc::RayletClientPool> client_pool_;
  std::unique_ptr<pubsub::GcsPublisher> gcs_publisher_;
  std::unique_ptr<InstrumentedIOContextWithThread> io_context_;
  std::unique_ptr<GcsNodeManager> node_manager_;
};

void RunBenchmarks() {
  std::vector<std::pair<int, int>> test_configs = {
    // node_count, label_count
    {10, 0},    // Small cluster, no labels
    {10, 5},    // Small cluster, few labels
    {10, 20},   // Small cluster, moderate labels
    {10, 50},   // Small cluster, many labels
    {100, 0},   // Medium cluster, no labels
    {100, 5},   // Medium cluster, few labels
    {100, 20},  // Medium cluster, moderate labels
    {100, 50},  // Medium cluster, many labels
    {1000, 5},  // Large cluster, few labels
    {1000, 20}, // Large cluster, moderate labels
    {5000, 5},  // Very large cluster, few labels
  };

  const int iterations = 100;

  for (const auto& [node_count, label_size] : test_configs) {
    std::cout << "\n=== Benchmark: " << node_count << " nodes, "
              << label_size << " labels per node ===" << std::endl;

    GcsNodeManagerBenchmark benchmark;
    benchmark.SetupNodes(node_count, label_size);

    benchmark.BenchmarkGetAllNodeInfo(iterations);
    benchmark.BenchmarkGetAllNodeInfoLight(iterations);

    // Calculate speedup
    std::cout << "  Speedup from using Light version: "
              << "(run both to see comparison)" << std::endl;
  }
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char** argv) {
  std::cout << "GcsNodeManager Benchmark\n";
  std::cout << "========================\n";
  ray::gcs::RunBenchmarks();
  return 0;
}
