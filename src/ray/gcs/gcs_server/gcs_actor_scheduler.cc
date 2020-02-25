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

#include "gcs_actor_scheduler.h"
#include <ray/common/ray_config.h>
#include <ray/gcs/redis_gcs_client.h>
#include <ray/protobuf/node_manager.pb.h>
#include <ray/util/asio_util.h>
#include "gcs_actor_manager.h"

namespace ray {
namespace gcs {

void GcsActorScheduler::GcsLeasedWorker::CreateActor(
    const TaskSpecification &actor_creation_task, const std::function<void()> &on_done) {
  RAY_CHECK(on_done);
  assigned_actor_id_ = actor_creation_task.ActorCreationId();

  std::unique_ptr<rpc::PushTaskRequest> request(new rpc::PushTaskRequest());
  request->set_intended_worker_id(address_.worker_id());
  request->mutable_task_spec()->CopyFrom(actor_creation_task.GetMessage());

  google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> resources;
  for (auto resource : resources_) {
    resources.Add(std::move(resource));
  }
  request->mutable_resource_mapping()->CopyFrom(resources);

  auto retry_creating_actor = [this, actor_creation_task, on_done] {
    // Reset the client so that we can reconnect when retry.
    ResetClient();
    // The purpose of using the weak_ptr here is to make the worker's life cycle easier to
    // manage.
    // Capturing the shared_ptr in lambda is another choice, but it is a little more
    // complexity because it need add a flag to the worker, if the worker is dead we
    // should mark the flag to dead, and in the callback of io event we should do like
    // this:
    //    if (!shared_worker->is_dead) { do_something(); }
    std::weak_ptr<GcsLeasedWorker> weak_this(shared_from_this());
    execute_after(io_context_,
                  [weak_this, actor_creation_task, on_done] {
                    if (auto worker = weak_this.lock()) {
                      worker->CreateActor(actor_creation_task, on_done);
                    }
                  },
                  RayConfig::instance().gcs_create_actor_retry_interval_ms());
  };

  std::weak_ptr<GcsLeasedWorker> weak_this(shared_from_this());
  auto client = GetOrCreateClient();
  auto status = client->PushNormalTask(
      std::move(request), [weak_this, retry_creating_actor, on_done](
                              Status status, const rpc::PushTaskReply &reply) {
        RAY_UNUSED(reply);
        if (auto worker = weak_this.lock()) {
          if (!status.ok()) {
            retry_creating_actor();
            return;
          }
          on_done();
        }
      });
  if (!status.ok()) {
    retry_creating_actor();
  }
}

GcsActorScheduler::GcsActorScheduler(
    boost::asio::io_context &io_context,
    std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client,
    const gcs::GcsNodeManager &gcs_node_manager)
    : io_context_(io_context),
      client_call_manager_(io_context_),
      redis_gcs_client_(redis_gcs_client),
      gcs_node_manager_(gcs_node_manager) {}

void GcsActorScheduler::Schedule(std::shared_ptr<GcsActor> actor) {
  auto node_id = actor->GetNodeID();
  if (!node_id.IsNil()) {
    if (auto node = gcs_node_manager_.GetNode(node_id)) {
      // If the actor is already tied to a node and the node is available, then record
      // the relationship of the node and actor and then lease worker directly from the
      // node.
      AddActorInPhaseOfLeasing(actor);
      LeaseWorkerFromNode(actor, node);
      return;
    }

    // The actor is already tied to a node which is unavailable now, so we should reset
    // the address.
    actor->UpdateAddress(rpc::Address());
  }

  // Select a node to lease worker for the actor.
  auto node = SelectNodeRandomly();
  if (node == nullptr) {
    // There are no available nodes to schedule the actor, so just trigger the failed
    // handler.
    RAY_CHECK(schedule_failed_handler_);
    schedule_failed_handler_(std::move(actor));
    return;
  }

  // Update the address of the actor as it is tied to a new node.
  auto address = actor->GetAddress();
  address.set_raylet_id(node->node_id());
  actor->UpdateAddress(address);
  auto actor_table_data =
      std::make_shared<rpc::ActorTableData>(actor->GetActorTableData());
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(redis_gcs_client_->Actors().AsyncUpdate(
      actor->GetActorID(), actor_table_data, [this, actor](Status status) {
        RAY_CHECK_OK(status);
        // There is no promise that the node the actor tied to is till alive as the flush
        // is asynchronously, so just invoke `Schedule` which will lease worker directly
        // if the node is still available or select a new one if not.
        Schedule(actor);
      }));
}

std::vector<ActorID> GcsActorScheduler::CancelOnNode(const ClientID &node_id) {
  // Remove all the actors from the map associated with this node, and return them as they
  // will be reconstructed later.
  std::vector<ActorID> actor_ids;

  // Remove all actors in phase of leasing.
  {
    auto iter = node_to_actors_when_leasing_.find(node_id);
    if (iter != node_to_actors_when_leasing_.end()) {
      actor_ids.insert(actor_ids.end(), iter->second.begin(), iter->second.end());
      node_to_actors_when_leasing_.erase(iter);
    }
  }

  // Remove all actors in phase of creating.
  {
    auto iter = node_to_workers_when_creating_.find(node_id);
    if (iter != node_to_workers_when_creating_.end()) {
      for (auto &entry : iter->second) {
        actor_ids.emplace_back(entry.second->GetAssignedActorID());
      }
      node_to_workers_when_creating_.erase(iter);
    }
  }

  // Remove the related node_client from node_to_client_.
  node_to_client_.erase(node_id);
  return actor_ids;
}

ActorID GcsActorScheduler::CancelOnWorker(const ClientID &node_id,
                                          const WorkerID &worker_id) {
  // Remove the worker which is creating actor and return the related actor.
  return RemoveWorkerInPhaseOfCreating(node_id, worker_id);
}

void GcsActorScheduler::LeaseWorkerFromNode(std::shared_ptr<GcsActor> actor,
                                            std::shared_ptr<rpc::GcsNodeInfo> node) {
  RAY_CHECK(actor && node);

  auto node_id = ClientID::FromBinary(node->node_id());
  RAY_LOG(DEBUG) << "Start leasing worker from node " << node_id << " for actor "
                 << actor->GetActorID();

  rpc::RequestWorkerLeaseRequest request;
  request.mutable_resource_spec()->CopyFrom(
      actor->GetCreationTaskSpecification().GetMessage());

  auto retry_leasing_worker = [this, node_id, actor] {
    // Reset the client so that we can reconnect when retry.
    node_to_client_.erase(node_id);
    execute_after(io_context_,
                  [this, node_id, actor] {
                    if (auto node = gcs_node_manager_.GetNode(node_id)) {
                      LeaseWorkerFromNode(actor, node);
                    }
                  },
                  RayConfig::instance().gcs_lease_worker_retry_interval_ms());
  };

  auto client = GetOrCreateNodeClient(node_id, *node);
  auto status = client->RequestWorkerLease(
      request, [this, node_id, retry_leasing_worker, actor](
                   const Status &status, const rpc::RequestWorkerLeaseReply &reply) {
        if (auto node = gcs_node_manager_.GetNode(node_id)) {
          if (!status.ok()) {
            retry_leasing_worker();
            return;
          }
          RAY_LOG(INFO) << "Finished leasing worker from " << node_id << " for actor "
                        << actor->GetActorID();
          HandleWorkerLeasedReply(actor, reply);
        }
      });

  if (!status.ok()) {
    retry_leasing_worker();
  }
}

void GcsActorScheduler::HandleWorkerLeasedReply(
    std::shared_ptr<GcsActor> actor, const ray::rpc::RequestWorkerLeaseReply &reply) {
  // Remove the actor from the leasing queue.
  RemoveActorInPhaseOfLeasing(actor);

  const auto &retry_at_raylet_address = reply.retry_at_raylet_address();
  const auto &worker_address = reply.worker_address();
  if (worker_address.raylet_id().empty()) {
    RAY_CHECK(!retry_at_raylet_address.raylet_id().empty());
    auto address = actor->GetAddress();
    address.set_raylet_id(retry_at_raylet_address.raylet_id());
    actor->UpdateAddress(address);
    auto actor_table_data =
        std::make_shared<rpc::ActorTableData>(actor->GetActorTableData());
    // The backend storage is reliable in the future, so the status must be ok.
    RAY_CHECK_OK(redis_gcs_client_->Actors().AsyncUpdate(
        actor->GetActorID(), actor_table_data, [this, actor](Status status) {
          RAY_CHECK_OK(status);
          Schedule(actor);
        }));
  } else {
    std::vector<rpc::ResourceMapEntry> resources;
    for (auto &resource : reply.resource_mapping()) {
      resources.emplace_back(resource);
    }
    auto leased_worker = CreateLeasedWorker(worker_address, std::move(resources));
    RAY_LOG(INFO) << "Worker " << leased_worker->GetWorkerID()
                  << " is responsible for creating actor " << actor->GetActorID();
    AddWorkerInPhaseOfCreating(leased_worker);
    CreateActorOnWorker(actor, leased_worker);
  }
}

void GcsActorScheduler::CreateActorOnWorker(std::shared_ptr<GcsActor> actor,
                                            std::shared_ptr<GcsLeasedWorker> worker) {
  RAY_CHECK(actor && worker);
  actor->UpdateAddress(worker->GetAddress());
  RAY_LOG(INFO) << "Start creating actor " << actor->GetActorID() << " on worker "
                << worker->GetWorkerID() << " at node " << actor->GetNodeID();
  std::weak_ptr<GcsLeasedWorker> weak_worker(worker);
  worker->CreateActor(actor->GetCreationTaskSpecification(), [this, actor, weak_worker] {
    // This lambda should not capture shared_ptr of the worker, otherwise the worker may
    // never be destroyed as this lambda will be held by `retry_creating_actor` inner
    // `GcsLeasedWorker::CreateActor`.
    auto worker = weak_worker.lock();
    // This callback is invoked only when the actor is created successfully, so the worker
    // must still exist.
    RAY_CHECK(worker != nullptr);
    RAY_LOG(INFO) << "Succeeded in creating actor " << actor->GetActorID()
                  << " on worker " << worker->GetWorkerID() << " at node "
                  << actor->GetNodeID();
    RAY_CHECK(actor->GetActorID() ==
              RemoveWorkerInPhaseOfCreating(worker->GetNodeID(), worker->GetWorkerID()));
    RAY_CHECK(schedule_success_handler_);
    schedule_success_handler_(actor);
  });
}

std::shared_ptr<rpc::GcsNodeInfo> GcsActorScheduler::SelectNodeRandomly() const {
  auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  if (alive_nodes.empty()) {
    return nullptr;
  }

  static std::mt19937_64 gen_(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<int> distribution(0, alive_nodes.size() - 1);
  int key_index = distribution(gen_);
  int index = 0;
  auto iter = alive_nodes.begin();
  for (; index != key_index && iter != alive_nodes.end(); ++index, ++iter)
    ;
  return iter->second;
}

std::shared_ptr<rpc::NodeManagerWorkerClient> GcsActorScheduler::GetOrCreateNodeClient(
    const ClientID &node_id, const rpc::GcsNodeInfo &node) {
  auto iter = node_to_client_.find(node_id);
  if (iter == node_to_client_.end()) {
    auto node_client = rpc::NodeManagerWorkerClient::make(
        node.node_manager_address(), node.node_manager_port(), client_call_manager_);
    iter = node_to_client_.emplace(node_id, std::move(node_client)).first;
  }
  return iter->second;
}

void GcsActorScheduler::AddActorInPhaseOfLeasing(std::shared_ptr<GcsActor> actor) {
  node_to_actors_when_leasing_[actor->GetNodeID()].emplace(actor->GetActorID());
}

void GcsActorScheduler::RemoveActorInPhaseOfLeasing(std::shared_ptr<GcsActor> actor) {
  auto iter = node_to_actors_when_leasing_.find(actor->GetNodeID());
  if (iter != node_to_actors_when_leasing_.end()) {
    iter->second.erase(actor->GetActorID());
    if (iter->second.empty()) {
      node_to_actors_when_leasing_.erase(iter);
    }
  }
}

void GcsActorScheduler::AddWorkerInPhaseOfCreating(
    std::shared_ptr<GcsLeasedWorker> leased_worker) {
  RAY_CHECK(leased_worker != nullptr);
  auto node_id = leased_worker->GetNodeID();
  node_to_workers_when_creating_[node_id].emplace(leased_worker->GetWorkerID(),
                                                  leased_worker);
}

ActorID GcsActorScheduler::RemoveWorkerInPhaseOfCreating(const ClientID &node_id,
                                                         const WorkerID &worker_id) {
  ActorID assigned_actor_id;
  auto iter = node_to_workers_when_creating_.find(node_id);
  if (iter != node_to_workers_when_creating_.end()) {
    auto actor_iter = iter->second.find(worker_id);
    if (actor_iter != iter->second.end()) {
      assigned_actor_id = actor_iter->second->GetAssignedActorID();
      iter->second.erase(actor_iter);
      if (iter->second.empty()) {
        node_to_workers_when_creating_.erase(iter);
      }
    }
  }
  return assigned_actor_id;
}

std::shared_ptr<GcsActorScheduler::GcsLeasedWorker> GcsActorScheduler::CreateLeasedWorker(
    rpc::Address address, std::vector<rpc::ResourceMapEntry> resources) {
  return std::make_shared<GcsLeasedWorker>(std::move(address), std::move(resources),
                                           io_context_, client_call_manager_);
}

}  // namespace gcs
}  // namespace ray
