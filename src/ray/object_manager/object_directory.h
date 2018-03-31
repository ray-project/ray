#ifndef RAY_OBJECT_MANAGER_OBJECT_DIRECTORY_H
#define RAY_OBJECT_MANAGER_OBJECT_DIRECTORY_H

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/gcs/client.h"
#include "ray/id.h"
#include "ray/status.h"

namespace ray {

struct RemoteConnectionInfo {
  RemoteConnectionInfo() = default;
  RemoteConnectionInfo(const ClientID &id, const std::string &ip_address,
                       uint16_t port_num)
      : client_id(id), ip(ip_address), port(port_num) {}
  ClientID client_id;
  std::string ip;
  uint16_t port;
};

/// Connection information for remote object managers.
class ObjectDirectoryInterface {
 public:
  ObjectDirectoryInterface() = default;
  virtual ~ObjectDirectoryInterface() = default;

  /// Callbacks for GetInformation.
  using InfoSuccessCallback = std::function<void(const ray::RemoteConnectionInfo &info)>;
  using InfoFailureCallback = std::function<void(ray::Status status)>;

  /// This is used to establish object manager client connections.
  ///
  /// \param client_id The client for which information is required.
  /// \param success_cb A callback which handles the success of this method.
  /// \param fail_cb A callback which handles the failure of this method.
  /// \return Status of whether this asynchronous request succeeded.
  virtual ray::Status GetInformation(const ClientID &client_id,
                                     const InfoSuccessCallback &success_cb,
                                     const InfoFailureCallback &fail_cb) = 0;

  // Callbacks for GetLocations.
  using OnLocationsSuccess = std::function<void(const std::vector<ray::ClientID> &v,
                                                const ray::ObjectID &object_id)>;
  using OnLocationsFailure = std::function<void(const ray::ObjectID &object_id)>;

  /// Asynchronously obtain the locations of an object by ObjectID.
  /// This is used to handle object pulls.
  ///
  /// \param object_id The required object's ObjectID.
  /// \param success_cb Invoked upon success with list of remote connection info.
  /// \param fail_cb Invoked upon failure with ray status and object id.
  /// \return Status of whether this asynchronous request succeeded.
  virtual ray::Status GetLocations(const ObjectID &object_id,
                                   const OnLocationsSuccess &success_cb,
                                   const OnLocationsFailure &fail_cb) = 0;

  /// Cancels the invocation of the callback associated with callback_id.
  ///
  /// \param object_id The object id invoked with GetLocations.
  /// \return Status of whether this method succeeded.
  virtual ray::Status Cancel(const ObjectID &object_id) = 0;

  /// Report objects added to this node's store to the object directory.
  ///
  /// \param object_id The object id that was put into the store.
  /// \param client_id The client id corresponding to this node.
  /// \return Status of whether this method succeeded.
  virtual ray::Status ReportObjectAdded(const ObjectID &object_id,
                                        const ClientID &client_id) = 0;

  /// Report objects removed from this client's store to the object directory.
  ///
  /// \param object_id The object id that was removed from the store.
  /// \param client_id The client id corresponding to this node.
  /// \return Status of whether this method succeeded.
  virtual ray::Status ReportObjectRemoved(const ObjectID &object_id,
                                          const ClientID &client_id) = 0;

  /// Terminate this object.
  ///
  /// \return Status of whether termination succeeded.
  virtual ray::Status Terminate() = 0;
};

/// Ray ObjectDirectory declaration.
class ObjectDirectory : public ObjectDirectoryInterface {
 public:
  ObjectDirectory() = default;
  ~ObjectDirectory() override = default;

  ray::Status GetInformation(const ClientID &client_id,
                             const InfoSuccessCallback &success_callback,
                             const InfoFailureCallback &fail_callback) override;
  ray::Status GetLocations(const ObjectID &object_id,
                           const OnLocationsSuccess &success_callback,
                           const OnLocationsFailure &fail_callback) override;
  ray::Status Cancel(const ObjectID &object_id) override;
  ray::Status Terminate() override;
  ray::Status ReportObjectAdded(const ObjectID &object_id,
                                const ClientID &client_id) override;
  ray::Status ReportObjectRemoved(const ObjectID &object_id,
                                  const ClientID &client_id) override;
  /// Ray only (not part of the OD interface).
  ObjectDirectory(std::shared_ptr<gcs::AsyncGcsClient> gcs_client);

  /// This object cannot be copied for thread-safety.
  ObjectDirectory &operator=(const ObjectDirectory &o) {
    throw std::runtime_error("Can't copy ObjectDirectory.");
  }

 private:
  /// Callbacks associated with a call to GetLocations.
  // TODO(hme): I think these can be removed.
  struct ODCallbacks {
    OnLocationsSuccess success_cb;
    OnLocationsFailure fail_cb;
  };

  /// GetLocations registers a request for locations.
  /// This function actually carries out that request.
  ray::Status ExecuteGetLocations(const ObjectID &object_id);
  /// Invoked when call to ExecuteGetLocations completes.
  void GetLocationsComplete(const ObjectID &object_id,
                            const std::vector<ObjectTableDataT> &location_entries);

  /// Maintain map of in-flight GetLocation requests.
  std::unordered_map<ObjectID, ODCallbacks, UniqueIDHasher> existing_requests_;
  /// Reference to the gcs client.
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_DIRECTORY_H
