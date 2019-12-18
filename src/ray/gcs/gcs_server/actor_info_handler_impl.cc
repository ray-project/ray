#include "actor_info_handler_impl.h"
#include <assert.h>
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultActorInfoHandler::HandleGetActorInfo(
    const rpc::GetActorInfoRequest &request, rpc::GetActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Getting actor info, actor id = " << actor_id;

  auto on_done = [actor_id, reply, send_reply_callback](
                     Status status, const boost::optional<ActorTableData> &result) {
    if (status.ok()) {
      assert(result);
      reply->mutable_actor_table_data()->CopyFrom(*result);
    } else {
      RAY_LOG(ERROR) << "Failed to get actor info: " << status.ToString()
                     << ", actor id = " << actor_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Actors().AsyncGet(actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
  RAY_LOG(DEBUG) << "Finished getting actor info, actor id = " << actor_id;
}

void DefaultActorInfoHandler::HandleRegisterActorInfo(
    const rpc::RegisterActorInfoRequest &request, rpc::RegisterActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_table_data().actor_id());
  RAY_LOG(DEBUG) << "Registering actor info, actor id = " << actor_id;
  auto actor_table_data = std::make_shared<ActorTableData>();
  actor_table_data->CopyFrom(request.actor_table_data());
  auto on_done = [actor_id, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to register actor info: " << status.ToString()
                     << ", actor id = " << actor_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Actors().AsyncRegister(
      actor_table_data, [send_reply_callback](Status status) {
        send_reply_callback(status, nullptr, nullptr);
      });
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished registering actor info, actor id = " << actor_id;
}

void DefaultActorInfoHandler::HandleUpdateActorInfo(
    const rpc::UpdateActorInfoRequest &request, rpc::UpdateActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Updating actor info, actor id = " << actor_id;
  auto actor_table_data = std::make_shared<ActorTableData>();
  actor_table_data->CopyFrom(request.actor_table_data());
  auto on_done = [actor_id, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to update actor info: " << status.ToString()
                     << ", actor id = " << actor_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Actors().AsyncUpdate(
      actor_id, actor_table_data, [send_reply_callback](Status status) {
        send_reply_callback(status, nullptr, nullptr);
      });
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished updating actor info, actor id = " << actor_id;
}

}  // namespace rpc
}  // namespace ray
