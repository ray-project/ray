#include "ray/raylet/event/raylet_events.h"

namespace ray {

namespace events {

void EventEdgeEndpoint::Finish() {
  EventEdge::Finish();
  raylet_events_.Unsubscribe(subscription_id_);
}

void ObjectLocalEndpoint::Finish() {
  EventEdgeEndpoint::Finish();
  // Write the data.
  flatbuffers::FlatBufferBuilder fbb;
  auto object_ready_event = protocol::CreateObjectLocalEvent(
      fbb, to_flatbuf(fbb, subscription_id_), to_flatbuf(fbb, object_id_));
  fbb.Finish(object_ready_event);

  event_client_->WriteMessageAsync(
      static_cast<int64_t>(MessageType::ObjectLocalEvent), fbb.GetSize(),
      fbb.GetBufferPointer(), [this](const ray::Status &status) {
        // The null ID means the client has been deactivated,
        // so we just ignore errors here.
        if (!status.ok() && !event_client_->GetClientId().is_nil()) {
          RAY_LOG(ERROR) << status.ToString()
                         << ". Failed to send ObjectLocalEvent to client.";
        }
      });
}

RayletEvents::RayletEvents(ObjectManager &object_manager)
    : object_manager_(object_manager) {
  objectlocal_events_manager_.reset(new ObjectLocalEventsManager());
  RAY_CHECK_OK(object_manager_.SubscribeObjAdded(
      [this](const object_manager::protocol::ObjectInfoT &object_info) {
        ObjectID object_id = ObjectID::from_binary(object_info.object_id);
        HandleObjectLocal(object_id);
      }));

  RAY_CHECK_OK(object_manager_.SubscribeObjDeleted(
      [this](const ObjectID &object_id) { HandleObjectMissing(object_id); }));
}

void RayletEvents::Unsubscribe(const SubscriptionID &subscription_id) {
  auto search = endpoints_.find(subscription_id);
  if (search != endpoints_.end()) {
    endpoints_.erase(subscription_id);
    // Only pending tasks will be cancelled.
    search->second->Cancel();
  }
}

ray::Status RayletEvents::SubscribeObjectLocalEvent(
    const SubscriptionID &subscription_id,
    const std::shared_ptr<LocalClientConnection> &client, const ObjectLocalEvent &event) {
  auto events_client = GetEventConnection(client);
  if (!events_client)
    return ray::Status::KeyError("The events client has not been activated.");

  auto objectlocal_endpoint = std::make_shared<ObjectLocalEndpoint>(
      *this, subscription_id, client, events_client, event);

  if (local_objects_.find(event) == local_objects_.end()) {
    auto event_edge =
        std::static_pointer_cast<EventEdge, ObjectLocalEndpoint>(objectlocal_endpoint);
    objectlocal_events_manager_->SubscribeEvent(event, event_edge);
  } else {
    // The object ID has been local.
    objectlocal_endpoint->Finish();
  }
  return ray::Status::OK();
}

std::shared_ptr<LocalClientConnection> RayletEvents::GetEventConnection(
    const std::shared_ptr<LocalClientConnection> &client) {
  const auto &client_id = client->GetClientId();
  auto search = event_socket_connections_.find(client_id);
  if (search != event_socket_connections_.end()) {
    auto events_client = search->second;
    RAY_CHECK(client_id == events_client->GetClientId());
    return events_client;
  } else {
    return nullptr;
  }
}

};  // namespace events
};  // namespace ray