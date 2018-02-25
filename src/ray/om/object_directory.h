#ifndef RAY_OBJECTDIRECTORY_H
#define RAY_OBJECTDIRECTORY_H

#include "memory"
#include "vector"
#include "unordered_set"
#include "unordered_map"

#include "ray/id.h"
#include "ray/status.h"
#include <ray/raylet/mock_gcs_client.h>

// TODO(hme): comment everything doxygen-style.

namespace ray {

struct ODRemoteConnectionInfo {
  ODRemoteConnectionInfo(const ClientID &id, const std::string &ipaddr, int portnum):
    client_id(id), ip(ipaddr), port(portnum) {}
  ClientID client_id;
  std::string ip;
  int port;
};

// Connection information for remote object managers.
class ObjectDirectoryInterface {

 public:

  // Callback for GetLocations.
  using SuccessCallback = std::function<void(const std::vector<ray::ODRemoteConnectionInfo> &v,
                                             const ray::ObjectID &object_id)>;

  using FailureCallback = std::function<void(ray::Status status,
                                             const ray::ObjectID &object_id)>;

  ObjectDirectoryInterface() = default;

  // Asynchronously obtain the locations of an object by ObjectID.
  // If the invocation fails, the failure callback is invoked with
  // ray status and object_id.
  virtual ray::Status GetLocations(const ObjectID &object_id,
                                   const SuccessCallback &success_cb,
                                   const FailureCallback &fail_cb) = 0;

  // Cancels the invocation of the callback associated with callback_id.
  virtual ray::Status Cancel(const ObjectID &object_id) = 0;

  virtual ray::Status Terminate() = 0;

};

// Ray ObjectDirectory declaration.
class ObjectDirectory : public ObjectDirectoryInterface {

 public:

  ObjectDirectory();


  ray::Status GetLocations(const ObjectID &object_id,
                           const SuccessCallback &success_cb,
                           const FailureCallback &fail_cb) override;

  ray::Status Cancel(const ObjectID &object_id) override;

  ray::Status Terminate() override;

  void InitGcs(std::shared_ptr<GcsClient> gcs_client);

 private:

  struct ODCallbacks {
    SuccessCallback success_cb;
    FailureCallback fail_cb;
  };

  std::shared_ptr<GcsClient> gcs_client;

  std::unordered_map<ObjectID, ODCallbacks, UniqueIDHasher> existing_requests_;

  ray::Status ExecuteGetLocations(const ObjectID &object_id);
  ray::Status GetLocationsComplete(ray::Status status,
                                   const ObjectID &object_id,
                                   const std::vector<ODRemoteConnectionInfo> &v);

};

} // namespace ray

#endif // RAY_OBJECTDIRECTORY_H
