#include "node_info_handler_impl.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultNodeInfoHandler::HandleRegisterNodeInfo(
    const rpc::RegisterNodeInfoRequest &request, rpc::RegisterNodeInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  //  RAY_LOG(DEBUG) << "Begin register node info, node id is:" << request.actor_id();
  //  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  //  auto on_done = [request, reply, send_reply_callback](
  //                     Status status, const boost::optional<ActorTableData> &result) {
  //    if (status.ok()) {
  //      if (result) {
  //        reply->mutable_actor_table_data()->CopyFrom(*result);
  //      }
  //    } else {
  //      RAY_LOG(ERROR) << "Failed to get actor info:" << status.ToString()
  //                     << ",actor id is:" << request.actor_id();
  //    }
  //    send_reply_callback(status, nullptr, nullptr);
  //  };
  //
  //  Status status = gcs_client_.Actors().AsyncGet(actor_id, on_done);
  //  if (!status.ok()) {
  //    on_done(status, boost::none);
  //  }
  //  RAY_LOG(DEBUG) << "Finish get actor info, actor id is:" << request.actor_id();
}

void DefaultNodeInfoHandler::HandleUnregisterNodeInfo(
    const rpc::UnregisterNodeInfoRequest &request, rpc::UnregisterNodeInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  //  RAY_LOG(DEBUG) << "Begin register actor info, actor id is:"
  //                 << request.actor_table_data().actor_id();
  //  auto actor_table_data = std::make_shared<ActorTableData>();
  //  actor_table_data->CopyFrom(request.actor_table_data());
  //  auto on_done = [request, send_reply_callback](Status status) {
  //    if (!status.ok()) {
  //      RAY_LOG(ERROR) << "Failed to register actor info:" << status.ToString()
  //                     << ",actor id is:" << request.actor_table_data().actor_id();
  //    }
  //    send_reply_callback(status, nullptr, nullptr);
  //  };
  //
  //  Status status = gcs_client_.Actors().AsyncRegister(
  //      actor_table_data, [send_reply_callback](Status status) {
  //        send_reply_callback(status, nullptr, nullptr);
  //      });
  //  if (!status.ok()) {
  //    on_done(status);
  //  }
  //  RAY_LOG(DEBUG) << "Finish register actor info, actor id is:"
  //                 << request.actor_table_data().actor_id();
}

void DefaultNodeInfoHandler::HandleGetAllNodesInfo(
    const rpc::GetAllNodesInfoRequest &request, rpc::GetAllNodesInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  //  RAY_LOG(DEBUG) << "Begin update actor info, actor id is:" << request.actor_id();
  //  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  //  auto actor_table_data = std::make_shared<ActorTableData>();
  //  actor_table_data->CopyFrom(request.actor_table_data());
  //  auto on_done = [request, send_reply_callback](Status status) {
  //    if (!status.ok()) {
  //      RAY_LOG(ERROR) << "Failed to update actor info:" << status.ToString()
  //                     << ",actor id is:" << request.actor_id();
  //    }
  //    send_reply_callback(status, nullptr, nullptr);
  //  };
  //
  //  Status status = gcs_client_.Actors().AsyncUpdate(
  //      actor_id, actor_table_data, [send_reply_callback](Status status) {
  //        send_reply_callback(status, nullptr, nullptr);
  //      });
  //  if (!status.ok()) {
  //    on_done(status);
  //  }
  //  RAY_LOG(DEBUG) << "Finish update actor info, actor id is:" << request.actor_id();
}

}  // namespace rpc
}  // namespace ray
