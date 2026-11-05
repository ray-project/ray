// Copyright 2026 The Ray Authors.
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

// Measures the GCS-side CPU cost of one resource load pull round on an idle
// cluster, per thread:
//   - wire: stands in for the gRPC client polling thread, which deserializes
//     each reply (ClientCallManager::PollEventsFromCompletionQueue).
//   - pull: the GcsResourceLoadPuller io_context, which sends the requests and
//     runs the reply callbacks.
//   - main: the GCS main io_context, which applies the reports to
//     GcsResourceManager and GcsAutoscalerStateManager.
// The network and the raylet side are not measured: each fake raylet answers
// with pre-serialized bytes.
//
// Build with -DLOAD_PULL_VERSIONED for the versioned pull; without it, the
// benchmark compiles against the unversioned puller on master.

#include <time.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "gtest/gtest.h"
#include "ray/asio/asio_util.h"
#include "ray/asio/periodical_runner.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/gcs/actor/fake_gcs_actor_manager.h"
#include "ray/gcs/fake_gcs_node_manager.h"
#include "ray/gcs/fake_gcs_placement_group_manager.h"
#include "ray/gcs/gcs_autoscaler_state_manager.h"
#include "ray/gcs/gcs_resource_load_puller.h"
#include "ray/gcs/gcs_resource_manager.h"
#include "ray/gcs/store_client/fake_store_client.h"
#include "ray/gcs/store_client_kv.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/util/clock.h"

namespace ray {
namespace gcs {

namespace {

constexpr int kNumResources = 6;
constexpr int kNumDemandShapes = 10;
constexpr int kWarmupRounds = 3;
constexpr int kMeasuredRounds = 20;

int64_t ThreadCpuNanos() {
  struct timespec ts;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

/// Runs `fn` on `io` and waits for it.
template <typename T>
T RunOn(instrumented_io_context &io, std::function<T()> fn) {
  std::promise<T> promise;
  auto future = promise.get_future();
  io.post([&]() { promise.set_value(fn()); }, "Benchmark.run");
  return future.get();
}

void RunOn(instrumented_io_context &io, std::function<void()> fn) {
  RunOn<int>(io, [&]() {
    fn();
    return 0;
  });
}

rpc::ResourcesData MakeReport(const NodeID &node_id, int index) {
  const std::string ip = absl::StrFormat("10.0.%d.%d", index / 256, index % 256);
  rpc::ResourcesData data;
  data.set_node_id(node_id.Binary());
  data.set_node_manager_address(ip);
  const std::vector<std::pair<std::string, double>> resources = {
      {"CPU", 64},
      {"GPU", 8},
      {"memory", 512.0 * 1024 * 1024 * 1024},
      {"object_store_memory", 128.0 * 1024 * 1024 * 1024},
      {"node:" + ip, 1},
      {"accelerator_type:A100", 1},
  };
  static_assert(kNumResources == 6);
  for (const auto &[name, quantity] : resources) {
    (*data.mutable_resources_total())[name] = quantity;
    // Fully used, as the demand below is pending.
    (*data.mutable_resources_available())[name] = name.rfind("node:", 0) == 0 ? 1 : 0;
  }
  for (int i = 0; i < kNumDemandShapes; ++i) {
    auto *demand = data.mutable_resource_load_by_shape()->add_resource_demands();
    (*demand->mutable_shape())["CPU"] = i + 1;
    (*demand->mutable_shape())["memory"] = (i + 1) * 1024.0 * 1024 * 1024;
    demand->set_num_ready_requests_queued(i + 1);
    demand->set_backlog_size(i);
    (*data.mutable_resource_load())["CPU"] += (i + 1) * (i + 1);
    (*data.mutable_resource_load())["memory"] += (i + 1) * (i + 1) * 1024.0 * 1024 * 1024;
  }
  return data;
}

/// A raylet that answers from pre-serialized bytes: the reply is deserialized
/// on `wire_io`, as the gRPC polling thread would, and the callback then runs
/// on `pull_io`, as ClientCallManager posts it.
class WireRayletClient : public rpc::FakeRayletClient {
 public:
  WireRayletClient(const rpc::ResourcesData &report,
                   instrumented_io_context &wire_io,
                   instrumented_io_context &pull_io,
                   std::atomic<int64_t> &num_callbacks)
      : wire_io_(wire_io), pull_io_(pull_io), num_callbacks_(num_callbacks) {
    rpc::GetResourceLoadReply full;
    *full.mutable_resources() = report;
#ifdef LOAD_PULL_VERSIONED
    full.set_version(kVersion);
    rpc::GetResourceLoadReply unchanged;
    unchanged.set_version(kVersion);
    unchanged.set_unchanged(true);
    unchanged_reply_bytes_ = unchanged.SerializeAsString();
#endif
    full_reply_bytes_ = full.SerializeAsString();
  }

#ifdef LOAD_PULL_VERSIONED
  void GetResourceLoad(
      int64_t last_seen_version,
      const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback) override {
    rpc::GetResourceLoadRequest request;
    request.set_last_seen_version(last_seen_version);
    const std::string *reply_bytes =
        last_seen_version == kVersion ? &unchanged_reply_bytes_ : &full_reply_bytes_;
#else
  void GetResourceLoad(
      const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback) override {
    rpc::GetResourceLoadRequest request;
    const std::string *reply_bytes = &full_reply_bytes_;
#endif
    // gRPC serializes the request on the caller's thread.
    request_bytes_ = request.SerializeAsString();
    wire_io_.post(
        [this, reply_bytes, callback]() {
          rpc::GetResourceLoadReply reply;
          RAY_CHECK(reply.ParseFromString(*reply_bytes));
          pull_io_.post(
              [this, callback, reply = std::move(reply)]() mutable {
                callback(Status::OK(), std::move(reply));
                num_callbacks_++;
              },
              "WireRayletClient.callback");
        },
        "WireRayletClient.deserialize");
  }

 private:
#ifdef LOAD_PULL_VERSIONED
  static constexpr int64_t kVersion = 1;
#endif
  instrumented_io_context &wire_io_;
  instrumented_io_context &pull_io_;
  std::atomic<int64_t> &num_callbacks_;
  std::string full_reply_bytes_;
  std::string unchanged_reply_bytes_;
  std::string request_bytes_;
};

struct ThreadCost {
  double wire_ms;
  double pull_ms;
  double main_ms;
};

class LoadPullBenchmark {
 public:
  explicit LoadPullBenchmark(int num_nodes)
      : wire_thread_("bench_wire", false, false),
        pull_thread_("bench_pull", false, false),
        main_thread_("bench_main", false, false) {
    // Build and register the nodes on the main thread, where the managers'
    // thread checkers expect every call.
    RunOn(main_io(), [&]() { SetUp(num_nodes); });
  }

  ~LoadPullBenchmark() {
    // Tear down on the main thread, while the io threads still run.
    RunOn(main_io(), [&]() {
      puller_.reset();
      raylet_client_pool_.reset();
      clients_.clear();
      autoscaler_state_manager_.reset();
      placement_group_manager_.reset();
      resource_manager_.reset();
      observability_publisher_.reset();
      actor_manager_.reset();
      worker_client_pool_.reset();
      unused_raylet_client_pool_.reset();
      runtime_env_manager_.reset();
      function_manager_.reset();
      kv_manager_.reset();
      node_manager_.reset();
      cluster_resource_manager_.reset();
    });
  }

  /// Runs warmup rounds, then returns the mean per-round CPU time per thread.
  ThreadCost Run() {
    for (int i = 0; i < kWarmupRounds; ++i) {
      PullRound();
    }
    const auto before = Sample();
    for (int i = 0; i < kMeasuredRounds; ++i) {
      PullRound();
    }
    const auto after = Sample();
    auto per_round = [](int64_t a, int64_t b) {
      return static_cast<double>(b - a) / kMeasuredRounds / 1e6;
    };
    return ThreadCost{per_round(before[0], after[0]),
                      per_round(before[1], after[1]),
                      per_round(before[2], after[2])};
  }

 private:
  instrumented_io_context &wire_io() { return wire_thread_.GetIoService(); }
  instrumented_io_context &pull_io() { return pull_thread_.GetIoService(); }
  instrumented_io_context &main_io() { return main_thread_.GetIoService(); }

  void SetUp(int num_nodes) {
    cluster_resource_manager_ =
        std::make_unique<ClusterResourceManager>(PeriodicalRunner::Create(main_io()));
    node_manager_ = std::make_unique<FakeGcsNodeManager>();
    kv_manager_ = std::make_unique<GcsInternalKVManager>(
        std::make_unique<StoreClientInternalKV>(std::make_unique<FakeStoreClient>()),
        "{}",
        main_io());
    function_manager_ =
        std::make_unique<GCSFunctionManager>(kv_manager_->GetInstance(), main_io());
    runtime_env_manager_ = std::make_unique<RuntimeEnvManager>(
        [](const std::string &, std::function<void(bool)>) {});
    unused_raylet_client_pool_ = std::make_unique<rpc::RayletClientPool>(
        [](const rpc::Address &) { return std::make_shared<rpc::FakeRayletClient>(); });
    worker_client_pool_ =
        std::make_unique<rpc::CoreWorkerClientPool>([](const rpc::Address &) {
          return std::make_shared<rpc::FakeCoreWorkerClient>();
        });
    actor_manager_ = std::make_unique<FakeGcsActorManager>(*runtime_env_manager_,
                                                           *function_manager_,
                                                           *unused_raylet_client_pool_,
                                                           *worker_client_pool_);
    observability_publisher_ = std::make_unique<pubsub::ObservabilityPublisher>(
        std::make_unique<pubsub::FakePublisher>());
    resource_manager_ = std::make_unique<GcsResourceManager>(
        main_io(), *cluster_resource_manager_, *node_manager_, NodeID::FromRandom());
    placement_group_manager_ = std::make_unique<FakeGcsPlacementGroupManager>(
        *resource_manager_,
        placement_group_gauge_,
        placement_group_creation_latency_histogram_,
        placement_group_scheduling_latency_histogram_,
        placement_group_count_gauge_);
    autoscaler_state_manager_ =
        std::make_unique<GcsAutoscalerStateManager>("bench",
                                                    *node_manager_,
                                                    *actor_manager_,
                                                    *placement_group_manager_,
                                                    *unused_raylet_client_pool_,
                                                    kv_manager_->GetInstance(),
                                                    main_io(),
                                                    /*gcs_publisher=*/nullptr,
                                                    observability_publisher_.get(),
                                                    clock_);

    for (int i = 0; i < num_nodes; ++i) {
      const NodeID node_id = NodeID::FromRandom();
      const auto report = MakeReport(node_id, i);
      rpc::GcsNodeInfo node;
      node.set_node_id(node_id.Binary());
      node.set_state(rpc::GcsNodeInfo::ALIVE);
      node.set_node_manager_address(report.node_manager_address());
      node.set_node_manager_port(10000 + i % 50000);
      node.mutable_resources_total()->insert(report.resources_total().begin(),
                                             report.resources_total().end());
      resource_manager_->OnNodeAdd(node);
      autoscaler_state_manager_->OnNodeAdd(node);

      rpc::Address address;
      address.set_node_id(node_id.Binary());
      address.set_ip_address(node.node_manager_address());
      address.set_port(node.node_manager_port());
      addresses_.push_back(address);
      clients_[node_id] = std::make_shared<WireRayletClient>(
          report, wire_io(), pull_io(), num_callbacks_);
    }

    raylet_client_pool_ =
        std::make_unique<rpc::RayletClientPool>([this](const rpc::Address &address) {
          return clients_.at(NodeID::FromBinary(address.node_id()));
        });
    puller_ = std::make_unique<GcsResourceLoadPuller>(
        pull_io(),
        main_io(),
        *raylet_client_pool_,
#ifdef LOAD_PULL_VERSIONED
        [this](rpc::ResourcesData resources) -> bool {
          const bool loads_applied = resource_manager_->UpdateResourceLoads(resources);
          const bool usage_applied =
              autoscaler_state_manager_->UpdateResourceLoadAndUsage(std::move(resources));
          return loads_applied && usage_applied;
        });
#else
        [this](rpc::ResourcesData resources) {
          resource_manager_->UpdateResourceLoads(resources);
          autoscaler_state_manager_->UpdateResourceLoadAndUsage(std::move(resources));
        });
#endif
  }

  /// One pull of every node, waiting until every thread has drained the work
  /// the pull caused.
  void PullRound() {
    const int64_t expected = num_callbacks_ + static_cast<int64_t>(addresses_.size());
    auto addresses = addresses_;
    RunOn(pull_io(), [&]() { puller_->Pull(std::move(addresses)); });
    while (num_callbacks_.load() < expected) {
      std::this_thread::yield();
    }
    // Every apply was posted to main before its callback finished counting.
    // The versioned puller then posts its bookkeeping back to pull.
    RunOn(main_io(), []() {});
    RunOn(pull_io(), []() {});
  }

  std::vector<int64_t> Sample() {
    return {RunOn<int64_t>(wire_io(), ThreadCpuNanos),
            RunOn<int64_t>(pull_io(), ThreadCpuNanos),
            RunOn<int64_t>(main_io(), ThreadCpuNanos)};
  }

  InstrumentedIOContextWithThread wire_thread_;
  InstrumentedIOContextWithThread pull_thread_;
  InstrumentedIOContextWithThread main_thread_;
  std::atomic<int64_t> num_callbacks_{0};

  Clock clock_;
  std::unique_ptr<ClusterResourceManager> cluster_resource_manager_;
  std::unique_ptr<FakeGcsNodeManager> node_manager_;
  std::unique_ptr<GcsInternalKVManager> kv_manager_;
  std::unique_ptr<GCSFunctionManager> function_manager_;
  std::unique_ptr<RuntimeEnvManager> runtime_env_manager_;
  std::unique_ptr<rpc::RayletClientPool> unused_raylet_client_pool_;
  std::unique_ptr<rpc::CoreWorkerClientPool> worker_client_pool_;
  std::unique_ptr<FakeGcsActorManager> actor_manager_;
  std::unique_ptr<pubsub::ObservabilityPublisher> observability_publisher_;
  std::unique_ptr<GcsResourceManager> resource_manager_;
  ray::observability::FakeGauge placement_group_gauge_;
  ray::observability::FakeHistogram placement_group_creation_latency_histogram_;
  ray::observability::FakeHistogram placement_group_scheduling_latency_histogram_;
  ray::observability::FakeGauge placement_group_count_gauge_;
  std::unique_ptr<FakeGcsPlacementGroupManager> placement_group_manager_;
  std::unique_ptr<GcsAutoscalerStateManager> autoscaler_state_manager_;

  std::vector<rpc::Address> addresses_;
  absl::flat_hash_map<NodeID, std::shared_ptr<WireRayletClient>> clients_;
  std::unique_ptr<rpc::RayletClientPool> raylet_client_pool_;
  std::unique_ptr<GcsResourceLoadPuller> puller_;
};

}  // namespace

TEST(GcsResourceLoadPullBenchmark, IdleCluster) {
#ifdef LOAD_PULL_VERSIONED
  const char *variant = "versioned";
#else
  const char *variant = "master";
#endif
  std::printf("variant=%s resources/node=%d demand shapes/node=%d rounds=%d\n",
              variant,
              kNumResources,
              kNumDemandShapes,
              kMeasuredRounds);
  std::printf("%8s %14s %14s %14s   (CPU ms per pull round = ms/s at 1 pull/s)\n",
              "nodes",
              "wire(grpc)",
              "pull",
              "main");
  for (int num_nodes : {100, 1000, 5000, 10000}) {
    LoadPullBenchmark benchmark(num_nodes);
    const auto cost = benchmark.Run();
    std::printf("%8d %14.3f %14.3f %14.3f\n",
                num_nodes,
                cost.wire_ms,
                cost.pull_ms,
                cost.main_ms);
  }
}

}  // namespace gcs
}  // namespace ray
