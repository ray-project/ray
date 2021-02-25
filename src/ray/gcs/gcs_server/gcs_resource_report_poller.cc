#include "ray/gcs/gcs_server/gcs_resource_report_poller.h"


namespace ray {
namespace gcs {

GcsResourceReportPoller::GcsResourceReportPoller(
                                                  std::shared_ptr<GcsResourceManager> gcs_resource_manager,
                                                  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool
                                                 ) :
  gcs_resource_manager_(gcs_resource_manager),
  raylet_client_pool_(raylet_client_pool),
  poll_timer_(polling_service_)
{}

void GcsResourceReportPoller::Start() {
  polling_thread_ = std::unique_ptr<std::thread>(new std::thread{
      [&](){
        RAY_LOG(ERROR) << "Polling thread created";
        polling_service_.post(
                              [&]() {
                                Tick();
                              }
                              );
        polling_service_.run();
          }
        });
}

void GcsResourceReportPoller::GetAllResourceUsage() {
  RAY_LOG(ERROR) << "Getting all resource usage";

  std::vector<rpc::Address> to_poll;
  {
    absl::MutexLock guard(&mutex_);
    for (const auto &pair : nodes_to_poll_) {
      to_poll.push_back(pair.second);
    }
  }

  for (const auto &address : to_poll) {
    auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(address);

    auto node_id = NodeID::FromBinary(address.raylet_id());
    raylet_client->RequestResourceReport([&, node_id] (const Status &status, const rpc::RequestResourceReportReply &reply) {
                                           // TODO (Alex): this callback is running on the wrong thread?
                                                    RAY_LOG(ERROR) << "Got report";
                                                  });
  }


}

void GcsResourceReportPoller::Tick() {
  GetAllResourceUsage();

  // TODO (Alex): Should this be an open system or closed?
  auto delay = boost::posix_time::milliseconds(100);
  poll_timer_.expires_from_now(delay);
  poll_timer_.async_wait([&](const boost::system::error_code &error) {
                           RAY_CHECK(!error) << "Timer failed for no apparent reason.";
                           Tick();
                         });
}

void GcsResourceReportPoller::HandleNodeAdded(std::shared_ptr<rpc::GcsNodeInfo> node_info) {
  NodeID node_id = NodeID::FromBinary(node_info->node_id());
  rpc::Address address;
  address.set_raylet_id(node_info->node_id());
  address.set_ip_address(node_info->node_manager_address());
  address.set_port(node_info->node_manager_port());

  {
    absl::MutexLock guard(&mutex_);
    nodes_to_poll_[node_id] = address;
  }
}



void GcsResourceReportPoller::HandleNodeRemoved(std::shared_ptr<rpc::GcsNodeInfo> node_info) {
  NodeID node_id = NodeID::FromBinary(node_info->node_id());

  {
    absl::MutexLock guard(&mutex_);
    nodes_to_poll_.erase(node_id);
  }
}

}  // namespace rpc
}  // namespace ray
