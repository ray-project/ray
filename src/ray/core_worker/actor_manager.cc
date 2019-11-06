#include "ray/core_worker/actor_manager.h"

namespace ray {

Status ActorManager::GetActorHandle(const ActorID &actor_id,
                                    ActorHandle **actor_handle) const {
  auto it = actor_handles_.find(actor_id);
  if (it == actor_handles_.end()) {
    return Status::Invalid("Handle for actor does not exist");
  }
  *actor_handle = it->second.get();
  return Status::OK();
}

void ActorManager::RegisterChildActor(const ActorID &actor_id,
                                      const ray::TaskSpecification &spec) {
  auto inserted = children_actors_.insert({actor_id, ChildActor(spec)});
  RAY_CHECK(inserted.second) << "Child actor already exists";
}

bool ActorManager::AddActorHandle(std::unique_ptr<ActorHandle> actor_handle) {
  const auto &actor_id = actor_handle->GetActorID();
  auto inserted = actor_handles_.emplace(actor_id, std::move(actor_handle)).second;
  return inserted;
}

void ActorManager::Clear() {
  actor_handles_.clear();

  if (!children_actors_.empty()) {
    RAY_LOG(WARNING) << "Clearing actor state, but some children actors are still alive. "
                        "They will not be restarted.";
    children_actors_.clear();
  }
}

void ActorManager::OnActorLocationChanged(const ActorID &actor_id,
                                          const ClientID &node_id,
                                          const std::string &ip_address, const int port) {
  auto it = actor_handles_.find(actor_id);
  RAY_CHECK(it != actor_handles_.end());

  it->second->UpdateLocation(node_id);
  direct_actor_clients_.ConnectActor(actor_id, ip_address, port);
}

void ActorManager::OnActorFailed(const ActorID &actor_id) {
  auto it = actor_handles_.find(actor_id);
  RAY_CHECK(it != actor_handles_.end());

  // We have to reset the actor handle since the next instance of the
  // actor will not have the last sequence number that we sent.
  // TODO: Remove the flag for direct calls. We do not reset for the
  // raylet codepath because it tries to replay all tasks since the last
  // actor checkpoint.
  it->second->MarkFailed(/*reset_task_counter=*/it->second->IsDirectCallActor());
  direct_actor_clients_.DisconnectActor(actor_id);

  // If we are the actor's creator, restart it.
  auto child_it = children_actors_.find(actor_id);
  if (child_it != children_actors_.end()) {
    child_it->second.num_lifetimes++;
    // Restart the actor.
    RAY_LOG(ERROR) << "Attempting to restart failed actor " << actor_id << ", attempt #"
                   << child_it->second.num_lifetimes;
    actor_creation_callback_(actor_id, child_it->second.actor_creation_spec,
                             child_it->second.num_lifetimes);
  }
}

void ActorManager::OnActorDead(const ActorID &actor_id) {
  auto it = actor_handles_.find(actor_id);
  RAY_CHECK(it != actor_handles_.end());
  // We cannot erase the actor handle here because clients can still
  // submit tasks to dead actors.
  it->second->MarkDead();
  direct_actor_clients_.DisconnectActor(actor_id);
}

}  // namespace ray
