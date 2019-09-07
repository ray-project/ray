#ifndef RAY_OBJECT_MANAGER_OBJECT_DIRECTORY_H
#define RAY_OBJECT_MANAGER_OBJECT_DIRECTORY_H

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "plasma/client.h"

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/object_manager/format/object_manager_generated.h"

namespace ray {

/// Connection information for remote object managers.
struct RemoteConnectionInfo {
  RemoteConnectionInfo(const ClientID &id) : client_id(id) {}

  // Returns whether there is enough information to connect to the remote
  // object manager.
  bool Connected() const { return !ip.empty(); }

  ClientID client_id;
  std::string ip;
  uint16_t port;
};

class ObjectDirectoryInterface {
 public:
  virtual ~ObjectDirectoryInterface() {}

  virtual void RegisterBackend() = 0;

  /// Lookup how to connect to a remote object manager.
  ///
  /// \param connection_info The connection information to fill out. This
  /// should be pre-populated with the requested client ID. If the directory
  /// has information about the requested client, then the rest of the fields
  /// in this struct will be populated accordingly.
  virtual void LookupRemoteConnectionInfo(
      RemoteConnectionInfo &connection_info) const = 0;

  /// Get information for all connected remote object managers.
  ///
  /// \return A vector of information for all connected remote object managers.
  virtual std::vector<RemoteConnectionInfo> LookupAllRemoteConnections() const = 0;

  /// Callback for object location notifications.
  using OnLocationsFound = std::function<void(const ray::ObjectID &object_id,
                                              const std::unordered_set<ray::ClientID> &)>;

  /// Lookup object locations. Callback may be invoked with empty list of client ids.
  ///
  /// \param object_id The object's ObjectID.
  /// \param callback Invoked with (possibly empty) list of client ids and object_id.
  /// \return Status of whether async call to backend succeeded.
  virtual ray::Status LookupLocations(const ObjectID &object_id,
                                      const OnLocationsFound &callback) = 0;

  /// Handle the removal of an object manager client. This updates the
  /// locations of all subscribed objects that have the removed client as a
  /// location, and fires the subscribed callbacks for those objects.
  ///
  /// \param client_id The object manager client that was removed.
  virtual void HandleClientRemoved(const ClientID &client_id) = 0;

  /// Subscribe to be notified of locations (ClientID) of the given object.
  /// The callback will be invoked with the complete list of known locations
  /// whenever the set of locations changes. The callback will also be fired if
  /// the list of known locations is empty. The callback provided to this
  /// method may fire immediately, within the call to this method, if any other
  /// listener is subscribed to the same object: This occurs when location data
  /// for the object has already been obtained.
  //
  /// \param callback_id The id associated with the specified callback. This is
  /// needed when UnsubscribeObjectLocations is called.
  /// \param object_id The required object's ObjectID.
  /// \param success_cb Invoked with non-empty list of client ids and object_id.
  /// \return Status of whether subscription succeeded.
  virtual ray::Status SubscribeObjectLocations(const UniqueID &callback_id,
                                               const ObjectID &object_id,
                                               const OnLocationsFound &callback) = 0;

  /// Unsubscribe to object location notifications.
  ///
  /// \param callback_id The id associated with a callback. This was given
  /// at subscription time, and unsubscribes the corresponding callback from
  /// further notifications about the given object's location.
  /// \param object_id The object id invoked with Subscribe.
  /// \return Status of unsubscribing from object location notifications.
  virtual ray::Status UnsubscribeObjectLocations(const UniqueID &callback_id,
                                                 const ObjectID &object_id) = 0;

  /// Report objects added to this node's store to the object directory.
  ///
  /// \param object_id The object id that was put into the store.
  /// \param client_id The client id corresponding to this node.
  /// \param object_info Additional information about the object.
  /// \return Status of whether this method succeeded.
  virtual ray::Status ReportObjectAdded(
      const ObjectID &object_id, const ClientID &client_id,
      const object_manager::protocol::ObjectInfoT &object_info) = 0;

  /// Report objects removed from this client's store to the object directory.
  ///
  /// \param object_id The object id that was removed from the store.
  /// \param client_id The client id corresponding to this node.
  /// \param object_info Additional information about the object.
  /// \return Status of whether this method succeeded.
  virtual ray::Status ReportObjectRemoved(
      const ObjectID &object_id, const ClientID &client_id,
      const object_manager::protocol::ObjectInfoT &object_info) = 0;

  /// Get local client id
  ///
  /// \return ClientID
  virtual ray::ClientID GetLocalClientID() = 0;

  /// Returns debug string for class.
  ///
  /// \return string.
  virtual std::string DebugString() const = 0;
};

/// Ray ObjectDirectory declaration.
class ObjectDirectory : public ObjectDirectoryInterface {
 public:
  /// Create an object directory.
  ///
  /// \param io_service The event loop to dispatch callbacks to. This should
  /// usually be the same event loop that the given gcs_client runs on.
  /// \param gcs_client A Ray GCS client to request object and client
  /// information from.
  ObjectDirectory(boost::asio::io_service &io_service,
                  std::shared_ptr<gcs::RedisGcsClient> &gcs_client);

  virtual ~ObjectDirectory() {}

  void RegisterBackend() override;

  void LookupRemoteConnectionInfo(RemoteConnectionInfo &connection_info) const override;

  std::vector<RemoteConnectionInfo> LookupAllRemoteConnections() const override;

  ray::Status LookupLocations(const ObjectID &object_id,
                              const OnLocationsFound &callback) override;

  void HandleClientRemoved(const ClientID &client_id) override;

  ray::Status SubscribeObjectLocations(const UniqueID &callback_id,
                                       const ObjectID &object_id,
                                       const OnLocationsFound &callback) override;
  ray::Status UnsubscribeObjectLocations(const UniqueID &callback_id,
                                         const ObjectID &object_id) override;

  ray::Status ReportObjectAdded(
      const ObjectID &object_id, const ClientID &client_id,
      const object_manager::protocol::ObjectInfoT &object_info) override;
  ray::Status ReportObjectRemoved(
      const ObjectID &object_id, const ClientID &client_id,
      const object_manager::protocol::ObjectInfoT &object_info) override;

  ray::ClientID GetLocalClientID() override;

  std::string DebugString() const override;

  /// ObjectDirectory should not be copied.
  RAY_DISALLOW_COPY_AND_ASSIGN(ObjectDirectory);

 private:
  /// Callbacks associated with a call to GetLocations.
  struct LocationListenerState {
    /// The callback to invoke when object locations are found.
    std::unordered_map<UniqueID, OnLocationsFound> callbacks;
    /// The current set of known locations of this object.
    std::unordered_set<ClientID> current_object_locations;
    /// This flag will get set to true if received any notification of the object.
    /// It means current_object_locations is up-to-date with GCS. It
    /// should never go back to false once set to true. If this is true, and
    /// the current_object_locations is empty, then this means that the object
    /// does not exist on any nodes due to eviction or the object never getting created.
    bool subscribed;
  };

  /// Reference to the event loop.
  boost::asio::io_service &io_service_;
  /// Reference to the gcs client.
  std::shared_ptr<gcs::RedisGcsClient> gcs_client_;
  /// Info about subscribers to object locations.
  std::unordered_map<ObjectID, LocationListenerState> listeners_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_DIRECTORY_H
