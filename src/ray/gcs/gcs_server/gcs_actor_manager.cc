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

#include "ray/gcs/gcs_server/gcs_actor_manager.h"
namespace ray {
namespace gcs {

void GcsActorManager::HandleRegisterActor(rpc::RegisterActorRequest request,
                                          rpc::RegisterActorReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO) << "Registering actor, job id = " << actor_id.JobId()
                << ", actor id = " << actor_id;

  auto cb = [reply, send_reply_callback, actor_id](
      const std::shared_ptr<gcs::GcsActor> &actor, Status status) {
    if(status.ok()) {
      RAY_LOG(INFO) << "Registered actor, job id = " << actor_id.JobId()
                    << ", actor id = " << actor_id;
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    } else {
      RAY_LOG(WARNING) << "Failed to register actor: " << status.ToString()
                       << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    }
  };

  GetShard(actor_id).RegisterActor(request, std::move(cb));

  ++counts_[CountType::REGISTER_ACTOR_REQUEST];
}

void GcsActorManager::HandleCreateActor(rpc::CreateActorRequest request,
                                        rpc::CreateActorReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO) << "Creating actor, job id = " << actor_id.JobId()
                << ", actor id = " << actor_id;
  auto cb = [reply, send_reply_callback, actor_id](const std::shared_ptr<gcs::GcsActor> &actor,
                                                   const rpc::PushTaskReply &task_reply,
                                                   Status status) {
    if (status.IsSchedulingCancelled()) {
      // Actor creation is cancelled.
      RAY_LOG(INFO) << "Actor creation was cancelled, job id = " << actor_id.JobId()
                    << ", actor id = " << actor_id;
      reply->mutable_death_cause()->CopyFrom(
          actor->GetActorTableData().death_cause());
      GCS_RPC_SEND_REPLY(send_reply_callback,
                         reply,
                         Status::SchedulingCancelled("Actor creation cancelled."));
    } else if (status.ok()) {
      RAY_LOG(INFO) << "Finished creating actor, job id = " << actor_id.JobId()
                    << ", actor id = " << actor_id;
      reply->mutable_actor_address()->CopyFrom(actor->GetAddress());
      reply->mutable_borrowed_refs()->CopyFrom(task_reply.borrowed_refs());
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    } else {
      RAY_LOG(WARNING) << "Failed to create actor, job id = " << actor_id.JobId()
                       << ", actor id = " << actor_id << ", status: " << status.ToString();
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    }
  };
  GetShard(actor_id).CreateActor(std::move(request), std::move(cb));
  ++counts_[CountType::CREATE_ACTOR_REQUEST];
}

void GcsActorManager::HandleGetActorInfo(rpc::GetActorInfoRequest request,
                                         rpc::GetActorInfoReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Getting actor info"
                 << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;

  GetShard(actor_id).GetActorInfo(actor_id, [reply, send_reply_callback] (GcsActor* ptr){
    if(ptr != nullptr) {
      *reply->mutable_actor_table_data() = ptr->GetActorTableData();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  });

  ++counts_[CountType::GET_ACTOR_INFO_REQUEST];
}

void GcsActorManager::HandleGetAllActorInfo(rpc::GetAllActorInfoRequest request,
                                            rpc::GetAllActorInfoReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  // auto limit = request.has_limit() ? request.limit() : -1;
  // RAY_LOG(DEBUG) << "Getting all actor info.";
  // ++counts_[CountType::GET_ALL_ACTOR_INFO_REQUEST];
  // if (request.show_dead_jobs() == false) {
  //   auto total_actors = registered_actors_.size() + destroyed_actors_.size();
  //   reply->set_total(total_actors);

  //   auto count = 0;
  //   for (const auto &iter : registered_actors_) {
  //     if (limit != -1 && count >= limit) {
  //       break;
  //     }
  //     count += 1;
  //     *reply->add_actor_table_data() = iter.second->GetActorTableData();
  //   }

  //   for (const auto &iter : destroyed_actors_) {
  //     if (limit != -1 && count >= limit) {
  //       break;
  //     }
  //     count += 1;
  //     *reply->add_actor_table_data() = iter.second->GetActorTableData();
  //   }
  //   RAY_LOG(DEBUG) << "Finished getting all actor info.";
  //   GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  //   return;
  // }

  // RAY_CHECK(request.show_dead_jobs());
  // // We don't maintain an in-memory cache of all actors which belong to dead
  // // jobs, so fetch it from redis.
  // Status status = gcs_table_storage_->ActorTable().GetAll(
  //     [reply, send_reply_callback, limit](
  //         absl::flat_hash_map<ActorID, rpc::ActorTableData> &&result) {
  //       auto total_actors = result.size();

  //       reply->set_total(total_actors);
  //       auto arena = reply->GetArena();
  //       RAY_CHECK(arena != nullptr);
  //       auto ptr = google::protobuf::Arena::Create<
  //           absl::flat_hash_map<ActorID, rpc::ActorTableData>>(arena, std::move(result));
  //       auto count = 0;
  //       for (const auto &pair : *ptr) {
  //         if (limit != -1 && count >= limit) {
  //           break;
  //         }
  //         count += 1;

  //         // TODO yic: Fix const cast
  //         reply->mutable_actor_table_data()->UnsafeArenaAddAllocated(
  //             const_cast<rpc::ActorTableData *>(&pair.second));
  //       }
  //       GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  //       RAY_LOG(DEBUG) << "Finished getting all actor info.";
  //     }, executor_);
  // if (!status.ok()) {
  //   // Send the response to unblock the sender and free the request.
  //   GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  // }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::NotImplemented("Get all actor info is not implemented"));
}

void GcsActorManager::HandleGetNamedActorInfo(
    rpc::GetNamedActorInfoRequest request,
    rpc::GetNamedActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  return;
  // const std::string &name = request.name();
  // const std::string &ray_namespace = request.ray_namespace();
  // RAY_LOG(DEBUG) << "Getting actor info, name = " << name
  //                << " , namespace = " << ray_namespace;

  // // Try to look up the actor ID for the named actor.
  // ActorID actor_id = GetActorIDByName(name, ray_namespace);

  // Status status = Status::OK();
  // auto iter = registered_actors_.find(actor_id);
  // if (actor_id.IsNil() || iter == registered_actors_.end()) {
  //   // The named actor was not found or the actor is already removed.
  //   std::stringstream stream;
  //   stream << "Actor with name '" << name << "' was not found.";
  //   RAY_LOG(WARNING) << stream.str();
  //   status = Status::NotFound(stream.str());
  // } else {
  //   *reply->mutable_actor_table_data() = iter->second->GetActorTableData();
  //   *reply->mutable_task_spec() = *iter->second->GetMutableTaskSpec();
  //   RAY_LOG(DEBUG) << "Finished getting actor info, job id = " << actor_id.JobId()
  //                  << ", actor id = " << actor_id;
  // }
  // GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  // ++counts_[CountType::GET_NAMED_ACTOR_INFO_REQUEST];
}

void GcsActorManager::HandleListNamedActors(rpc::ListNamedActorsRequest request,
                                            rpc::ListNamedActorsReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  // const std::string &ray_namespace = request.ray_namespace();
  // RAY_LOG(DEBUG) << "Getting named actor names, namespace = " << ray_namespace;

  // std::vector<std::pair<std::string, std::string>> actors =
  //     ListNamedActors(request.all_namespaces(), ray_namespace);
  // for (const auto &actor : actors) {
  //   auto named_actor_indo = reply->add_named_actors_list();
  //   named_actor_indo->set_ray_namespace(actor.first);
  //   named_actor_indo->set_name(actor.second);
  // }
  // GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  // ++counts_[CountType::LIST_NAMED_ACTORS_REQUEST];
}

void GcsActorManager::HandleKillActorViaGcs(rpc::KillActorViaGcsRequest request,
                                            rpc::KillActorViaGcsReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  const auto &actor_id = ActorID::FromBinary(request.actor_id());
  bool force_kill = request.force_kill();
  bool no_restart = request.no_restart();

  GetShard(actor_id).KillActorViaGcs(actor_id, force_kill, no_restart, [=] {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    RAY_LOG(DEBUG) << "Finished killing actor, job id = " << actor_id.JobId()
                   << ", actor id = " << actor_id << ", force_kill = " << force_kill
                   << ", no_restart = " << no_restart;
  });
  ++counts_[CountType::KILL_ACTOR_REQUEST];
}



}  // namespace gcs
}  // namespace ray
