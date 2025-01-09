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

#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

#include "gcs_actor_manager.h"
#include "gcs_virtual_cluster_manager.h"

namespace ray {
namespace gcs {

void GcsVirtualClusterManager::Initialize(const GcsInitData &gcs_init_data) {
  primary_cluster_->Initialize(gcs_init_data);
  if (periodical_runner_ != nullptr) {
    // Periodically check and replenish all the dead node instances of all the virtual
    // clusters.
    periodical_runner_->RunFnPeriodically(
        [this]() { primary_cluster_->ReplenishAllClusterNodeInstances(); },
        RayConfig::instance().node_instances_replenish_interval_ms(),
        "ReplenishNodeInstances");
  }
  const auto &actor_task_specs = gcs_init_data.ActorTaskSpecs();
  for (const auto &[actor_id, actor_table_data] : gcs_init_data.Actors()) {
    if (OnInitializeActorShouldLoad(gcs_init_data, actor_id)) {
      if (actor_table_data.is_detached()) {
        // If it's a detached actor, we need to handle registration event on FO
        // by gcs virtual cluster manager.
        const auto &actor_task_spec = map_find_or_die(actor_task_specs, actor_id);
        OnDetachedActorRegistration(
            actor_task_spec.scheduling_strategy().virtual_cluster_id(), actor_id);
      }
    }
  }
  for (const auto &[job_id, job_table_data] : gcs_init_data.Jobs()) {
    if (job_table_data.is_dead()) {
      const auto &virtual_cluster_id = job_table_data.virtual_cluster_id();
      if (virtual_cluster_id.empty()) {
        continue;
      }
      auto job_cluster_id = VirtualClusterID::FromBinary(virtual_cluster_id);
      if (!job_cluster_id.IsJobClusterID()) {
        continue;
      }

      auto virtual_cluster = GetVirtualCluster(virtual_cluster_id);
      if (virtual_cluster == nullptr) {
        continue;
      }
      JobCluster *job_cluster = dynamic_cast<JobCluster *>(virtual_cluster.get());
      job_cluster->SetFinished();
    }
  }
  for (auto &[placement_group_id, placement_group_table_data] :
       gcs_init_data.PlacementGroups()) {
    if (placement_group_table_data.state() == rpc::PlacementGroupTableData::REMOVED) {
      // ignore this pg...
      continue;
    }
    if (placement_group_table_data.is_detached()) {
      // If it's a detached placement group, we need to handle registration event on FO
      // by gcs virtual cluster manager.
      OnDetachedPlacementGroupRegistration(
          placement_group_table_data.virtual_cluster_id(), placement_group_id);
    }
  }
}

void GcsVirtualClusterManager::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  primary_cluster_->OnNodeAdd(node);
}

void GcsVirtualClusterManager::OnNodeDead(const rpc::GcsNodeInfo &node) {
  primary_cluster_->OnNodeDead(node);
}

void GcsVirtualClusterManager::OnJobFinished(const rpc::JobTableData &job_data) {
  // exit early when job without a virtual cluster id.
  const auto &virtual_cluster_id = job_data.virtual_cluster_id();
  if (virtual_cluster_id.empty()) {
    return;
  }

  auto job_cluster_id = VirtualClusterID::FromBinary(virtual_cluster_id);

  if (!job_cluster_id.IsJobClusterID()) {
    // exit early when this job does not belong to an job cluster.
    return;
  }

  std::string divisible_cluster_id = job_cluster_id.ParentID().Binary();

  auto virtual_cluster = GetVirtualCluster(divisible_cluster_id);
  if (virtual_cluster == nullptr) {
    RAY_LOG(WARNING) << "Failed to remove job cluster " << job_cluster_id.Binary()
                     << " when handling job finished event,  parent cluster not exists.";
    return;
  }

  if (!virtual_cluster->Divisible()) {
    // this should not happen, virtual cluster should be divisible.
    return;
  }

  auto job_virtual_cluster = GetVirtualCluster(virtual_cluster_id);
  if (job_virtual_cluster == nullptr) {
    // this should not happen, job cluster should exist
    return;
  }
  JobCluster *job_cluster = dynamic_cast<JobCluster *>(job_virtual_cluster.get());
  job_cluster->SetFinished();
  if (job_cluster->InUse()) {
    // job cluster is detached, do not remove it
    RAY_LOG(INFO) << "Failed to remove job cluster " << job_cluster_id.Binary()
                  << " when handling job finished event, job cluster is detached.";
    return;
  }

  DivisibleCluster *divisible_cluster =
      dynamic_cast<DivisibleCluster *>(virtual_cluster.get());

  auto status = divisible_cluster->RemoveJobCluster(
      virtual_cluster_id,
      [job_cluster_id](const Status &status,
                       std::shared_ptr<rpc::VirtualClusterTableData> data,
                       const ReplicaSets *replica_sets_to_recommend) {
        if (!status.ok() || !data->is_removed()) {
          RAY_LOG(WARNING) << "Failed to remove job cluster " << job_cluster_id.Binary()
                           << " when handling job finished event. status: "
                           << status.message();
        } else {
          RAY_LOG(INFO) << "Successfully removed job cluster " << job_cluster_id.Binary()
                        << " after handling job finished event.";
        }
      });
  if (!status.ok()) {
    RAY_LOG(WARNING) << "Failed to remove job cluster " << job_cluster_id.Binary()
                     << " when handling job finished event. status: " << status.message();
  }
}

std::shared_ptr<VirtualCluster> GcsVirtualClusterManager::GetVirtualCluster(
    const std::string &virtual_cluster_id) {
  if (virtual_cluster_id.empty()) {
    return nullptr;
  }
  return primary_cluster_->GetVirtualCluster(virtual_cluster_id);
}

void GcsVirtualClusterManager::HandleCreateOrUpdateVirtualCluster(
    rpc::CreateOrUpdateVirtualClusterRequest request,
    rpc::CreateOrUpdateVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  RAY_LOG(INFO) << "Start creating or updating virtual cluster " << virtual_cluster_id;
  auto on_done = [reply, virtual_cluster_id, callback = std::move(send_reply_callback)](
                     const Status &status,
                     std::shared_ptr<rpc::VirtualClusterTableData> data,
                     const ReplicaSets *replica_sets_to_recommend) {
    if (status.ok()) {
      RAY_CHECK(data != nullptr);
      // Fill the node instances of the virtual cluster to the reply.
      reply->mutable_node_instances()->insert(data->node_instances().begin(),
                                              data->node_instances().end());
      // Fill the revision of the virtual cluster to the reply.
      reply->set_revision(data->revision());
      RAY_LOG(INFO) << "Succeed in creating or updating virtual cluster " << data->id();
    } else {
      RAY_LOG(WARNING) << "Failed to create or update virtual cluster "
                       << virtual_cluster_id << ", status = " << status.ToString();
      if (replica_sets_to_recommend) {
        reply->mutable_replica_sets_to_recommend()->insert(
            replica_sets_to_recommend->begin(), replica_sets_to_recommend->end());
      }
    }
    GCS_RPC_SEND_REPLY(callback, reply, status);
  };

  // Verify if the arguments in the request is valid.
  auto status = VerifyRequest(request);
  if (!status.ok()) {
    on_done(status, nullptr, nullptr);
    return;
  }

  ReplicaSets replica_sets_to_recommend;
  status = primary_cluster_->CreateOrUpdateVirtualCluster(
      std::move(request), on_done, &replica_sets_to_recommend);
  if (!status.ok()) {
    on_done(status, nullptr, &replica_sets_to_recommend);
  }
}

void GcsVirtualClusterManager::HandleRemoveVirtualCluster(
    rpc::RemoveVirtualClusterRequest request,
    rpc::RemoveVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  RAY_LOG(INFO) << "Start removing virtual cluster " << virtual_cluster_id;
  auto on_done = [reply, virtual_cluster_id, callback = std::move(send_reply_callback)](
                     const Status &status,
                     std::shared_ptr<rpc::VirtualClusterTableData> data,
                     const ReplicaSets *replica_sets_to_recommend) {
    if (status.ok()) {
      RAY_LOG(INFO) << "Succeed in removing virtual cluster " << virtual_cluster_id;
    } else {
      RAY_LOG(WARNING) << "Failed to remove virtual cluster " << virtual_cluster_id
                       << ", status = " << status.ToString();
    }
    GCS_RPC_SEND_REPLY(callback, reply, status);
  };

  auto status = VerifyRequest(request);

  if (status.ok()) {
    status = primary_cluster_->RemoveVirtualCluster(virtual_cluster_id, on_done);
  }
  if (!status.ok()) {
    on_done(status, nullptr, nullptr);
  }
}

void GcsVirtualClusterManager::HandleGetVirtualClusters(
    rpc::GetVirtualClustersRequest request,
    rpc::GetVirtualClustersReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting virtual clusters.";
  primary_cluster_->ForeachVirtualClustersData(
      std::move(request), [reply, send_reply_callback](auto data) {
        reply->add_virtual_cluster_data_list()->CopyFrom(*data);
      });
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsVirtualClusterManager::HandleCreateJobCluster(
    rpc::CreateJobClusterRequest request,
    rpc::CreateJobClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  RAY_LOG(INFO) << "Start creating job cluster in virtual cluster: "
                << virtual_cluster_id;
  auto on_done = [reply,
                  virtual_cluster_id,
                  job_id = request.job_id(),
                  callback = std::move(send_reply_callback)](
                     const Status &status,
                     std::shared_ptr<rpc::VirtualClusterTableData> data,
                     const ReplicaSets *replica_sets_to_recommend) {
    if (status.ok()) {
      reply->set_job_cluster_id(data->id());
      if (replica_sets_to_recommend) {
        reply->mutable_replica_sets_to_recommend()->insert(
            replica_sets_to_recommend->begin(), replica_sets_to_recommend->end());
      }
      RAY_LOG(INFO) << "Succeed in creating job cluster in virtual cluster: "
                    << virtual_cluster_id << " for job: " << job_id;
    } else {
      RAY_LOG(ERROR) << "Failed to create job cluster in virtual cluster: "
                     << virtual_cluster_id << " for job: " << job_id
                     << ", status = " << status.ToString();
    }
    GCS_RPC_SEND_REPLY(callback, reply, status);
  };

  auto virtual_cluster = GetVirtualCluster(virtual_cluster_id);
  if (virtual_cluster == nullptr) {
    std::ostringstream ostr;
    ostr << "virtual cluster not exists: " << virtual_cluster_id;
    std::string message = ostr.str();
    on_done(Status::NotFound(message), nullptr, nullptr);
    return;
  }
  if (!virtual_cluster->Divisible()) {
    std::ostringstream ostr;
    ostr << " virtual cluster is not exclusive: " << virtual_cluster_id;
    std::string message = ostr.str();
    on_done(Status::InvalidArgument(message), nullptr, nullptr);
    return;
  }
  ReplicaSets replica_sets(request.replica_sets().begin(), request.replica_sets().end());

  auto divisible_cluster = dynamic_cast<DivisibleCluster *>(virtual_cluster.get());
  std::string job_cluster_id = divisible_cluster->BuildJobClusterID(request.job_id());

  ReplicaSets replica_sets_to_recommend;
  auto status = divisible_cluster->CreateJobCluster(
      job_cluster_id, std::move(replica_sets), on_done, &replica_sets_to_recommend);
  if (!status.ok()) {
    on_done(status, nullptr, &replica_sets_to_recommend);
  }
}

Status GcsVirtualClusterManager::VerifyRequest(
    const rpc::CreateOrUpdateVirtualClusterRequest &request) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  if (virtual_cluster_id.empty()) {
    std::ostringstream ostr;
    ostr << "Invalid request, the virtual cluster id is empty.";
    std::string message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::InvalidArgument(message);
  }

  if (virtual_cluster_id == primary_cluster_->GetID()) {
    std::ostringstream ostr;
    ostr << "Invalid request, " << virtual_cluster_id
         << " can not be created or updated.";
    auto message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::InvalidArgument(message);
  }

  for (const auto &[template_id, replicas] : request.replica_sets()) {
    if (replicas < 0) {
      std::ostringstream ostr;
      ostr << "Invalid request, replicas(" << replicas
           << ") must >= 0, virtual_cluster_id: " << virtual_cluster_id;
      auto message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }

    if (template_id.empty()) {
      std::ostringstream ostr;
      ostr << "Invalid request, template_id is empty, virtual_cluster_id: "
           << virtual_cluster_id;
      auto message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }
  }

  if (auto logical_cluster =
          primary_cluster_->GetLogicalCluster(request.virtual_cluster_id())) {
    // Check if the revision of the virtual cluster is expired.
    if (request.revision() != logical_cluster->GetRevision()) {
      std::ostringstream ss;
      ss << "The revision (" << request.revision()
         << ") is expired, the latest revision of the virtual cluster "
         << request.virtual_cluster_id() << " is " << logical_cluster->GetRevision();
      std::string message = ss.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }

    // check if the request attributes are compatible with the virtual cluster.
    if (request.divisible() != logical_cluster->Divisible()) {
      std::ostringstream ostr;
      ostr << "The requested attributes are incompatible with virtual cluster "
           << request.virtual_cluster_id() << ". expect: ("
           << logical_cluster->Divisible() << "), actual: (" << request.divisible()
           << ").";
      std::string message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }
  }

  return Status::OK();
}

Status GcsVirtualClusterManager::VerifyRequest(
    const rpc::RemoveVirtualClusterRequest &request) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  if (virtual_cluster_id.empty()) {
    std::ostringstream ostr;
    ostr << "Invalid request, the virtual cluster id is empty.";
    std::string message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::InvalidArgument(message);
  }

  if (virtual_cluster_id == primary_cluster_->GetID()) {
    std::ostringstream ostr;
    ostr << "Invalid request, " << virtual_cluster_id << " can not be removed.";
    auto message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::InvalidArgument(message);
  }
  return Status::OK();
}

Status GcsVirtualClusterManager::FlushAndPublish(
    std::shared_ptr<rpc::VirtualClusterTableData> data,
    CreateOrUpdateVirtualClusterCallback callback) {
  auto on_done = [this, data, callback = std::move(callback)](const Status &status) {
    // The backend storage is supposed to be reliable, so the status must be ok.
    RAY_CHECK_OK(status);
    if (data->divisible()) {
      // Tasks can only be scheduled on the nodes in the indivisible cluster, so we just
      // need to publish the indivisible cluster data.
      if (callback) {
        callback(status, std::move(data), nullptr);
      }
      return;
    }

    RAY_CHECK_OK(gcs_publisher_.PublishVirtualCluster(
        VirtualClusterID::FromBinary(data->id()), *data, nullptr));
    if (callback) {
      callback(status, std::move(data), nullptr);
    }
  };

  if (data->is_removed()) {
    return gcs_table_storage_.VirtualClusterTable().Delete(
        VirtualClusterID::FromBinary(data->id()), on_done);
  }

  // Write the virtual cluster data to the storage.
  return gcs_table_storage_.VirtualClusterTable().Put(
      VirtualClusterID::FromBinary(data->id()), *data, on_done);
}

void GcsVirtualClusterManager::OnDetachedActorRegistration(
    const std::string &virtual_cluster_id, const ActorID &actor_id) {
  if (VirtualClusterID::FromBinary(virtual_cluster_id).IsJobClusterID()) {
    auto virtual_cluster = GetVirtualCluster(virtual_cluster_id);
    if (virtual_cluster == nullptr) {
      RAY_LOG(ERROR) << "Failed to process the registration of detached actor "
                     << actor_id << " as the virtual cluster " << virtual_cluster_id
                     << " does not exist.";
      return;
    }
    JobCluster *job_cluster = dynamic_cast<JobCluster *>(virtual_cluster.get());
    job_cluster->OnDetachedActorRegistration(actor_id);
  }
}

void GcsVirtualClusterManager::OnDetachedActorDestroy(
    const std::string &virtual_cluster_id, const ActorID &actor_id) {
  if (VirtualClusterID::FromBinary(virtual_cluster_id).IsJobClusterID()) {
    auto virtual_cluster = GetVirtualCluster(virtual_cluster_id);
    if (virtual_cluster == nullptr) {
      RAY_LOG(ERROR) << "Failed to process the destroy of detached actor " << actor_id
                     << " as the virtual cluster " << virtual_cluster_id
                     << " does not exist.";
      return;
    }
    JobCluster *job_cluster = dynamic_cast<JobCluster *>(virtual_cluster.get());
    job_cluster->OnDetachedActorDestroy(actor_id);
    if (!job_cluster->InUse() && job_cluster->IsFinished()) {
      auto status = primary_cluster_->RemoveVirtualCluster(virtual_cluster_id, nullptr);
      if (!status.ok()) {
        RAY_LOG(WARNING) << "Failed to remove virtual cluster " << virtual_cluster_id
                         << " after handling detached actor destroy event. status: "
                         << status.message();
      }
    }
  }
}

void GcsVirtualClusterManager::OnDetachedPlacementGroupRegistration(
    const std::string &virtual_cluster_id, const PlacementGroupID &placement_group_id) {
  if (VirtualClusterID::FromBinary(virtual_cluster_id).IsJobClusterID()) {
    auto virtual_cluster = GetVirtualCluster(virtual_cluster_id);
    if (virtual_cluster == nullptr) {
      RAY_LOG(ERROR) << "Failed to process the registration of detached placement group "
                     << placement_group_id << " as the virtual cluster "
                     << virtual_cluster_id << " does not exist.";
      return;
    }
    JobCluster *job_cluster = dynamic_cast<JobCluster *>(virtual_cluster.get());
    job_cluster->OnDetachedPlacementGroupRegistration(placement_group_id);
  }
}

void GcsVirtualClusterManager::OnDetachedPlacementGroupDestroy(
    const std::string &virtual_cluster_id, const PlacementGroupID &placement_group_id) {
  if (VirtualClusterID::FromBinary(virtual_cluster_id).IsJobClusterID()) {
    auto virtual_cluster = GetVirtualCluster(virtual_cluster_id);
    if (virtual_cluster == nullptr) {
      RAY_LOG(ERROR) << "Failed to process the destroy of detached placement group "
                     << placement_group_id << " as the virtual cluster "
                     << virtual_cluster_id << " does not exist.";
      return;
    }
    JobCluster *job_cluster = dynamic_cast<JobCluster *>(virtual_cluster.get());
    job_cluster->OnDetachedPlacementGroupDestroy(placement_group_id);
    if (!job_cluster->InUse() && job_cluster->IsFinished()) {
      auto status = primary_cluster_->RemoveVirtualCluster(virtual_cluster_id, nullptr);
      if (!status.ok()) {
        RAY_LOG(WARNING)
            << "Failed to remove virtual cluster " << virtual_cluster_id
            << " after handling detached placement group destroy event. status: "
            << status.message();
      }
    }
  }
}

}  // namespace gcs
}  // namespace ray