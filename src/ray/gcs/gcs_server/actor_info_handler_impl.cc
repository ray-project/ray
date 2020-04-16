// Copyright 2017 The Ray Authors.
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

#include "actor_info_handler_impl.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultActorInfoHandler::HandleCreateActor(
    const ray::rpc::CreateActorRequest &request, ray::rpc::CreateActorReply *reply,
    ray::rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO) << "Registering actor, actor id = " << actor_id;
  gcs_actor_manager_.RegisterActor(request, [reply, send_reply_callback, actor_id](
                                                std::shared_ptr<gcs::GcsActor> actor) {
    RAY_LOG(INFO) << "Registered actor, actor id = " << actor_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  });
}

void DefaultActorInfoHandler::HandleGetActorInfo(
    const rpc::GetActorInfoRequest &request, rpc::GetActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Getting actor info, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;

  auto on_done = [actor_id, reply, send_reply_callback](
                     Status status, const boost::optional<ActorTableData> &result) {
    if (status.ok()) {
      if (result) {
        reply->mutable_actor_table_data()->CopyFrom(*result);
      }
    } else {
      RAY_LOG(ERROR) << "Failed to get actor info: " << status.ToString()
                     << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Actors().AsyncGet(actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
  RAY_LOG(DEBUG) << "Finished getting actor info, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
}

void DefaultActorInfoHandler::HandleRegisterActorInfo(
    const rpc::RegisterActorInfoRequest &request, rpc::RegisterActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_table_data().actor_id());
  RAY_LOG(DEBUG) << "Registering actor info, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
  auto actor_table_data = std::make_shared<ActorTableData>();
  actor_table_data->CopyFrom(request.actor_table_data());
  auto on_done = [actor_id, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to register actor info: " << status.ToString()
                     << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Actors().AsyncRegister(actor_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished registering actor info, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
}

void DefaultActorInfoHandler::HandleUpdateActorInfo(
    const rpc::UpdateActorInfoRequest &request, rpc::UpdateActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Updating actor info, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
  auto actor_table_data = std::make_shared<ActorTableData>();
  actor_table_data->CopyFrom(request.actor_table_data());
  auto on_done = [actor_id, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to update actor info: " << status.ToString()
                     << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Actors().AsyncUpdate(actor_id, actor_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished updating actor info, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
}

void DefaultActorInfoHandler::HandleAddActorCheckpoint(
    const AddActorCheckpointRequest &request, AddActorCheckpointReply *reply,
    SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.checkpoint_data().actor_id());
  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(request.checkpoint_data().checkpoint_id());
  RAY_LOG(DEBUG) << "Adding actor checkpoint, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id << ", checkpoint id = " << checkpoint_id;
  auto actor_checkpoint_data = std::make_shared<ActorCheckpointData>();
  actor_checkpoint_data->CopyFrom(request.checkpoint_data());
  auto on_done = [actor_id, checkpoint_id, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add actor checkpoint: " << status.ToString()
                     << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id
                     << ", checkpoint id = " << checkpoint_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Actors().AsyncAddCheckpoint(actor_checkpoint_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished adding actor checkpoint, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id << ", checkpoint id = " << checkpoint_id;
}

void DefaultActorInfoHandler::HandleGetActorCheckpoint(
    const GetActorCheckpointRequest &request, GetActorCheckpointReply *reply,
    SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(request.checkpoint_id());
  RAY_LOG(DEBUG) << "Getting actor checkpoint, job id = " << actor_id.JobId()
                 << ", checkpoint id = " << checkpoint_id;
  auto on_done = [actor_id, checkpoint_id, reply, send_reply_callback](
                     Status status, const boost::optional<ActorCheckpointData> &result) {
    if (status.ok()) {
      RAY_DCHECK(result);
      reply->mutable_checkpoint_data()->CopyFrom(*result);
    } else {
      RAY_LOG(ERROR) << "Failed to get actor checkpoint: " << status.ToString()
                     << ", job id = " << actor_id.JobId()
                     << ", checkpoint id = " << checkpoint_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status =
      gcs_client_.Actors().AsyncGetCheckpoint(checkpoint_id, actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
  RAY_LOG(DEBUG) << "Finished getting actor checkpoint, job id = " << actor_id.JobId()
                 << ", checkpoint id = " << checkpoint_id;
}

void DefaultActorInfoHandler::HandleGetActorCheckpointID(
    const GetActorCheckpointIDRequest &request, GetActorCheckpointIDReply *reply,
    SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Getting actor checkpoint id, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
  auto on_done = [actor_id, reply, send_reply_callback](
                     Status status,
                     const boost::optional<ActorCheckpointIdData> &result) {
    if (status.ok()) {
      RAY_DCHECK(result);
      reply->mutable_checkpoint_id_data()->CopyFrom(*result);
    } else {
      RAY_LOG(ERROR) << "Failed to get actor checkpoint id: " << status.ToString()
                     << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Actors().AsyncGetCheckpointID(actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
  RAY_LOG(DEBUG) << "Finished getting actor checkpoint id, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
}

}  // namespace rpc
}  // namespace ray
