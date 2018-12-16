#ifndef RAY_EVENT_RAYLET_EVENTS_H
#define RAY_EVENT_RAYLET_EVENTS_H

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "ray/common/client_connection.h"
#include "ray/common/common_protocol.h"

#include "ray/id.h"
#include "ray/object_manager/object_manager.h"
#include "ray/raylet/event/events.h"
#include "ray/raylet/format/node_manager_generated.h"

namespace ray {

namespace events {

typedef ObjectID ObjectLocalEvent;
using ray::protocol::MessageType;

/// This event manager controls ObjectID ready events coming from the plasma store.
class ObjectLocalEventsManager : public EventsManager<ObjectLocalEvent> {
 private:
  /// It is a nop function here.
  virtual void InitializeEventNode(
      std::shared_ptr<EventNode<ObjectLocalEvent>> &event_edge) override {}
};

class RayletEvents;

/// The endpoint is a special event edge that keeps a subscription ID
/// and interacts with the client. They are leaves in the event-driven DAG.
class EventEdgeEndpoint : public EventEdge {
 public:
  /// Create an event edge endpoint.
  ///
  /// \param raylet_events The raylet events main controller.
  /// \param subscription_id The subscription ID this endpoint holds.
  /// \param client The connection to the client through raylet socket.
  /// \param events_client The connection to the client through raylet events socket.
  EventEdgeEndpoint(RayletEvents &raylet_events, const SubscriptionID &subscription_id,
                    const std::shared_ptr<LocalClientConnection> &client,
                    const std::shared_ptr<LocalClientConnection> &events_client)
      : raylet_events_(raylet_events),
        subscription_id_(subscription_id),
        client_(client),
        event_client_(events_client) {}

  virtual void Finish() override;

 protected:
  /// The raylet events main controller it belongs to.
  RayletEvents &raylet_events_;
  /// The subscription ID this endpoint holds.
  const SubscriptionID subscription_id_;
  /// The connection to the client through raylet socket.
  std::shared_ptr<LocalClientConnection> client_;
  /// The connection to the client through raylet events socket.
  std::shared_ptr<LocalClientConnection> event_client_;
};

/// This class represents the endpoint for the ObjectLocal event.
class ObjectLocalEndpoint : public EventEdgeEndpoint {
 public:
  /// Create an ObjectLocal event edge endpoint.
  ///
  /// \param raylet_events The raylet events main controller.
  /// \param subscription_id The subscription ID this endpoint holds.
  /// \param client The connection to the client through raylet socket.
  /// \param events_client The connection to the client through raylet events socket.
  /// \param object_id The object ID this endpoint deals with.
  ObjectLocalEndpoint(RayletEvents &raylet_events, const SubscriptionID &subscription_id,
                      const std::shared_ptr<LocalClientConnection> &client,
                      const std::shared_ptr<LocalClientConnection> &events_client,
                      const ray::ObjectID &object_id)
      : EventEdgeEndpoint(raylet_events, subscription_id, client, events_client),
        object_id_(object_id) {}

  virtual void Finish() override;

 private:
  /// The ObjectID this endpoint holds.
  const ray::ObjectID object_id_;
};

/// This class is the main controller of raylet events.
class RayletEvents {
 public:
  /// Create an instance of this class.
  /// \param object_manager The object manager instance.
  RayletEvents(ObjectManager &object_manager);

  /// The handler of the ObjectLocal event.
  ///
  /// \param object_id The object ID of the ObjectLocal event.
  /// \return Void
  void HandleObjectLocal(const ObjectID &object_id) {
    // Add the object to the table of locally available objects.
    auto inserted = local_objects_.insert(object_id);
    RAY_CHECK(inserted.second);
    ProcessObjectLocalEvent(object_id);
  }

  /// The handler of the ObjectMissing event.
  ///
  /// \param object_id The object ID of the ObjectMissing event.
  /// \return Void
  void HandleObjectMissing(const ObjectID &object_id) {
    // Remove the object from the table of locally available objects.
    auto erased = local_objects_.erase(object_id);
    RAY_CHECK(erased == 1);
  }

  /// Process the ObjectLocal event.
  ///
  /// \param event The ObjectLocal event.
  /// \return Void
  void ProcessObjectLocalEvent(const ObjectLocalEvent &event) {
    objectlocal_events_manager_->FinishEvent(event);
  }

  /// Activate event socket connection.
  ///
  /// \param events_client The event socket connection to be activated.
  /// \param client_id The ID to be assigned to the event socket connection.
  /// \return Void
  void ActivateEventConnection(
      const std::shared_ptr<LocalClientConnection> &events_client,
      const ClientID &client_id) {
    events_client->SetClientID(client_id);
    event_socket_connections_[client_id] = events_client;
  }

  /// Deactivate event socket connection.
  ///
  /// \param client_id The ID of the event socket connection.
  /// \return Void
  void DeactivateEventConnection(const ClientID &client_id) {
    auto search = event_socket_connections_.find(client_id);
    if (search != event_socket_connections_.end()) {
      auto events_client = search->second;
      // Replace with a null ID.
      events_client->SetClientID(UniqueID::nil());
      event_socket_connections_.erase(client_id);
    }
  }

  /// Cancel a subscription.
  ///
  /// \param subscription_id The subscription ID to cancel.
  /// \return Void
  void Unsubscribe(const SubscriptionID &subscription_id);

  /// Subscribe the ObjectLocal event.
  ///
  /// \param subscription_id The subscription ID.
  /// \param client The connection to the client through raylet socket.
  /// \param event The event to subscribe.
  ray::Status SubscribeObjectLocalEvent(
      const SubscriptionID &subscription_id,
      const std::shared_ptr<LocalClientConnection> &client,
      const ObjectLocalEvent &event);

 private:
  /// Get the connection to the raylet event socket through the default connection.
  ///
  /// \param client The default connection.
  /// \return The connection to the raylet event socket.
  std::shared_ptr<LocalClientConnection> GetEventConnection(
      const std::shared_ptr<LocalClientConnection> &client);

  /// The set of locally available objects.
  std::unordered_set<ray::ObjectID> local_objects_;
  /// The object manager instance.
  ObjectManager &object_manager_;
  /// A mapping between subscription ID and event edge endpoints.
  std::unordered_map<SubscriptionID, std::shared_ptr<EventEdge>> endpoints_;
  /// The ObjectLocal event manager.
  std::unique_ptr<ObjectLocalEventsManager> objectlocal_events_manager_;
  /// A mapping from client ID to clients connection through the events socket.
  std::unordered_map<ClientID, std::shared_ptr<LocalClientConnection>>
      event_socket_connections_;
};

};  // namespace events
};  // namespace ray

#endif  // RAY_EVENT_RAYLET_EVENTS_H
