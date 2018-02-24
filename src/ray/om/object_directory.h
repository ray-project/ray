#ifndef RAY_OBJECTDIRECTORY_H
#define RAY_OBJECTDIRECTORY_H

#include "memory"
#include "vector"
#include "unordered_set"
#include "unordered_map"
#include "ray/id.h"
#include "ray/status.h"

using namespace std;

namespace ray {

struct ODRemoteConnectionInfo {
  ClientID dbc_id;
  string ip;
  string port;
};

// Connection information for remote object managers.
class ObjectDirectoryInterface {

 public:

  // Callback for GetLocations.
  using SuccessCallback = function<void(const vector<ray::ODRemoteConnectionInfo> &v,
                                        const ray::ObjectID &object_id)>;

  using FailureCallback = function<void(ray::Status status,
                                        const ray::ObjectID &object_id)>;

  ObjectDirectoryInterface() = default;

  // Asynchronously obtain the locations of an object by ObjectID.
  // If the invocation fails, the failure callback is invoked with
  // ray status and object_id.
  virtual void GetLocations(const ObjectID &object_id,
                            const SuccessCallback &success_cb,
                            const FailureCallback &fail_cb) = 0;

  // Cancels the invocation of the callback associated with callback_id.
  virtual void Cancel(const ObjectID &object_id) = 0;

  virtual void Terminate() = 0;

};

// Ray ObjectDirectory declaration.
class ObjectDirectory : public ObjectDirectoryInterface {

 public:
  ObjectDirectory();

  void GetLocations(const ObjectID &object_id,
                    const SuccessCallback &success_cb,
                    const FailureCallback &fail_cb) override;

  void Cancel(const ObjectID &object_id) override;

  void Terminate() override;

 private:

  struct ODCallbacks {
    SuccessCallback success_cb;
    FailureCallback fail_cb;
  };

  unordered_map<ObjectID, ODCallbacks, UniqueIDHasher> existing_requests;
  unordered_map<ObjectID, vector<ODRemoteConnectionInfo>, UniqueIDHasher> info_cache;

  void ExecuteGetLocations(const ObjectID &object_id);
  void GetLocationsComplete(Status status,
                            const ObjectID &object_id,
                            vector<ODRemoteConnectionInfo> v);
  void InitGCS();

};

} // namespace ray

#endif // RAY_OBJECTDIRECTORY_H
