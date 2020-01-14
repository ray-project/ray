#include "stats_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultStatsHandler::HandleAddProfileData(const AddProfileDataRequest &request,
                                               AddProfileDataReply *reply,
                                               SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.profile_data().component_id());
  RAY_LOG(DEBUG) << "Adding profile data, component type = "
                 << request.profile_data().component_type() << ", node id = " << node_id;
  auto profile_table_data = std::make_shared<ProfileTableData>();
  profile_table_data->CopyFrom(request.profile_data());
  auto on_done = [node_id, request, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add profile data, component type = "
                     << request.profile_data().component_type()
                     << ", node id = " << node_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Stats().AsyncAddProfileData(profile_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished adding profile data, component type = "
                 << request.profile_data().component_type() << ", node id = " << node_id;
}

}  // namespace rpc
}  // namespace ray
