
#include "ray/gcs/gcs_server/gcs_resource_report_broadcaster.h"

namespace ray {
namespace gcs {


GcsResourceReportBroadcaster::GcsResourceReportBroadcaster(
                                                           std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
                                                           std::function<void(rpc::ResourceUsageBatchData &)> get_resource_usage_batch_for_broadcast,
                                                           std::function<void(const rpc::Address &, std::shared_ptr<rpc::NodeManagerClientPool> &, std::string &)> send_batch
                                                           )
{}


}
}
