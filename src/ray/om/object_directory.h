#ifndef RAY_OBJECTDIRECTORY_H
#define RAY_OBJECTDIRECTORY_H

#include <memory>
#include <vector>
#include <unordered_set>
#include <unordered_map>

#include "ray/id.h"
#include "ray/status.h"
#include <ray/raylet/mock_gcs_client.h>

namespace ray {

struct RemoteConnectionInfo {
  RemoteConnectionInfo(const ClientID &id, const std::string &ipaddr, ushort portnum):
    client_id(id), ip(ipaddr), port(portnum) {}
  ClientID client_id;
  std::string ip;
  ushort port;
};

/// Connection information for remote object managers.
class ObjectDirectoryInterface {

 public:

  ObjectDirectoryInterface() = default;
  virtual ~ObjectDirectoryInterface() = default;

  /// Callbacks for GetInformation.
  using InfoSuccCB = std::function<void(const ray::RemoteConnectionInfo &info)>;
  using InfoFailCB = std::function<void(ray::Status status)>;

  /// This is used to establish object manager client connections.
  /// \param client_id The client for which information is required.
  /// \param success_cb A callback which handles the success of this method.
  /// \param fail_cb A callback which handles the failure of this method.
  /// \return Status of whether this asynchronous request succeeded.
  virtual ray::Status GetInformation(const ClientID &client_id,
                                     const InfoSuccCB &success_cb,
                                     const InfoFailCB &fail_cb) = 0;

  // Callbacks for GetLocations.
  using LocSuccCB = std::function<void(const std::vector<ray::RemoteConnectionInfo> &v,
                                       const ray::ObjectID &object_id)>;
  using LocFailCB = std::function<void(ray::Status status,
                                       const ray::ObjectID &object_id)>;

  /// Asynchronously obtain the locations of an object by ObjectID.
  /// This is used to handle object pulls.
  /// \param object_id The required object's ObjectID.
  /// \param success_cb Invoked upon success with list of remote connection info.
  /// \param fail_cb Invoked upon failure with ray status and object id.
  /// \return Status of whether this asynchronous request succeeded.
  virtual ray::Status GetLocations(const ObjectID &object_id,
                                   const LocSuccCB &success_cb,
                                   const LocFailCB &fail_cb) = 0;

  /// Cancels the invocation of the callback associated with callback_id.
  /// \param object_id The object id invoked with GetLocations.
  /// \return Status of whether this method succeeded.
  virtual ray::Status Cancel(const ObjectID &object_id) = 0;

  /// Report objects added to this node's store to the object directory.
  /// \param object_id The object id that was put into the store.
  /// \param client_id The client id corresponding to this node.
  /// \return Status of whether this method succeeded.
  virtual ray::Status ObjectAdded(const ObjectID &object_id,
                                  const ClientID &client_id) = 0;

  /// Report objects removed from this client's store to the object directory.
  /// \param object_id The object id that was removed from the store.
  /// \param client_id The client id corresponding to this node.
  /// \return Status of whether this method succeeded.
  virtual ray::Status ObjectRemoved(const ObjectID &object_id,
                                    const ClientID &client_id) = 0;

  /// Terminate this object.
  /// \return Status of whether termination succeeded.
  virtual ray::Status Terminate() = 0;

};

/// Ray ObjectDirectory declaration.
class ObjectDirectory : public ObjectDirectoryInterface {

 public:

  ObjectDirectory();
  ~ObjectDirectory() override;

  ray::Status GetInformation(const ClientID &client_id,
                             const InfoSuccCB &success_cb,
                             const InfoFailCB &fail_cb) override;
  ray::Status GetLocations(const ObjectID &object_id,
                           const LocSuccCB &success_cb,
                           const LocFailCB &fail_cb) override;
  ray::Status Cancel(const ObjectID &object_id) override;
  ray::Status Terminate() override;
  ray::Status ObjectAdded(const ObjectID &object_id,
                          const ClientID &client_id) override;
  ray::Status ObjectRemoved(const ObjectID &object_id,
                            const ClientID &client_id) override;
  /// Ray only (not part of the OD interface).
  ObjectDirectory(std::shared_ptr<GcsClient> gcs_client);

 private:

  /// Reference to the gcs client.
  std::shared_ptr<GcsClient> gcs_client;

  /// Callbacks associated with a call to GetLocations.
  // TODO(hme): I think these can be removed.
  struct ODCallbacks {
    LocSuccCB success_cb;
    LocFailCB fail_cb;
  };

  std::unordered_map<ObjectID, ODCallbacks, UniqueIDHasher> existing_requests_;

  /// GetLocations registers a request for locations.
  /// This function actually carries out that request.
  ray::Status ExecuteGetLocations(const ObjectID &object_id);
  /// Invoked when call to ExecuteGetLocations completes.
  ray::Status GetLocationsComplete(const ray::Status &status,
                                   const ObjectID &object_id,
                                   const std::vector<RemoteConnectionInfo> &v);

};

} // namespace ray

#endif // RAY_OBJECTDIRECTORY_H
