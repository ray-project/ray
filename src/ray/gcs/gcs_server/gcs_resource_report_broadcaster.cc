#include "ray/gcs/gcs_server/gcs_resource_report_broadcaster.h"
#include "ray/stats/stats.h"

namespace ray {
namespace gcs {

GcsResourceReportBroadcaster::GcsResourceReportBroadcaster(
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
    std::function<void(rpc::ResourceUsageBatchData &)>
        get_resource_usage_batch_for_broadcast,
    std::function<void(const rpc::Address &,
                       std::shared_ptr<rpc::NodeManagerClientPool> &, std::string &,
                       const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &)>
        send_batch

    )
    : ticker_(broadcast_service_),
      raylet_client_pool_(raylet_client_pool),
      get_resource_usage_batch_for_broadcast_(get_resource_usage_batch_for_broadcast),
      send_batch_(send_batch),
      num_skipped_nodes_(0),
      broadcast_period_ms_(
          RayConfig::instance().raylet_report_resources_period_milliseconds()) {}

GcsResourceReportBroadcaster::~GcsResourceReportBroadcaster() {}

void GcsResourceReportBroadcaster::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &pair : gcs_init_data.Nodes()) {
    HandleNodeAdded(pair.second);
  }
}

void GcsResourceReportBroadcaster::Start() {
  broadcast_thread_.reset(new std::thread{[this]() {
    SetThreadName("resource_report_broadcaster");
    boost::asio::io_service::work work(broadcast_service_);

    broadcast_service_.run();
    RAY_LOG(DEBUG)
        << "GCSResourceReportBroadcaster has stopped. This should only happen if "
           "the cluster has stopped";
  }});
  ticker_.RunFnPeriodically(
      [this] { SendBroadcast(); }, broadcast_period_ms_,
      "GcsResourceReportBroadcaster.deadline_timer.pull_resource_report");
}

void GcsResourceReportBroadcaster::Stop() {
  if (broadcast_thread_ != nullptr) {
    // TODO (Alex): There's technically a race condition here if we start and stop the
    // thread in rapid succession.
    broadcast_service_.stop();
    if (broadcast_thread_->joinable()) {
      broadcast_thread_->join();
    }
  }
}

void GcsResourceReportBroadcaster::HandleNodeAdded(const rpc::GcsNodeInfo &node_info) {
  rpc::Address address;
  address.set_raylet_id(node_info.node_id());
  address.set_ip_address(node_info.node_manager_address());
  address.set_port(node_info.node_manager_port());

  NodeID node_id = NodeID::FromBinary(node_info.node_id());

  absl::MutexLock guard(&mutex_);
  nodes_[node_id] = std::move(address);
}

void GcsResourceReportBroadcaster::HandleNodeRemoved(const rpc::GcsNodeInfo &node_info) {
  NodeID node_id = NodeID::FromBinary(node_info.node_id());
  {
    absl::MutexLock guard(&mutex_);
    nodes_.erase(node_id);
    RAY_CHECK(!nodes_.count(node_id));
    RAY_LOG(DEBUG) << "Node removed (node_id: " << node_id
                   << ")# of remaining nodes: " << nodes_.size();
  }
}

std::string GcsResourceReportBroadcaster::DebugString() {
  absl::MutexLock guard(&mutex_);
  std::ostringstream stream;
  stream << "GcsResourceReportBroadcaster: {Tracked nodes: " << nodes_.size()
         << ", Nodes skipped in last broadcast: " << num_skipped_nodes_;

  return stream.str();
}

void GcsResourceReportBroadcaster::SendBroadcast() {
  rpc::ResourceUsageBatchData batch;
  get_resource_usage_batch_for_broadcast_(batch);

  // Serializing is relatively expensive on large batches, so we should only do it once.
  std::string serialized_batch = batch.SerializeAsString();
  stats::OutboundHeartbeatSizeKB.Record((double)(batch.ByteSizeLong() / 1024.0));

  absl::MutexLock guard(&mutex_);
  num_skipped_nodes_ = 0;
  for (const auto &pair : nodes_) {
    const auto &node_id = pair.first;
    const auto &address = pair.second;

    auto &num_inflight = inflight_updates_[node_id];

    if (num_inflight >= 1) {
      num_skipped_nodes_++;
      continue;
    }

    double start_time = absl::GetCurrentTimeNanos();
    auto callback = [this, &num_inflight, start_time](
                        const Status &status,
                        const rpc::UpdateResourceUsageReply &reply) {
      double end_time = absl::GetCurrentTimeNanos();
      double lapsed_time_ms = (end_time - start_time) * 1e6;
      ray::stats::GcsUpdateResourceUsageTime.Record(lapsed_time_ms);

      absl::MutexLock guard(&mutex_);
      num_inflight--;
    };
    num_inflight++;
    send_batch_(address, raylet_client_pool_, serialized_batch, callback);
  }
}

}  // namespace gcs
}  // namespace ray
