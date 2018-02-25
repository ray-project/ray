#include "ray/om/object_directory.h"

using namespace std;

namespace ray {

  ObjectDirectory::ObjectDirectory(){}

  void ObjectDirectory::InitGcs(std::shared_ptr<GcsClient> gcs_client){
    this->gcs_client = gcs_client;
  };

  ray::Status ObjectDirectory::GetLocations(const ObjectID &object_id,
                                            const SuccessCallback &success_cb,
                                            const FailureCallback &fail_cb) {
    if (existing_requests_.count(object_id) == 0) {
      existing_requests_[object_id] = ODCallbacks({success_cb, fail_cb});;
      ExecuteGetLocations(object_id);
    } else {
      // Do nothing. A request is in progress.
    }
    return ray::Status::OK();
  };

  ray::Status ObjectDirectory::ExecuteGetLocations(const ObjectID &object_id){
    std::unordered_set<ClientID, UniqueIDHasher> client_ids = this->gcs_client->object_table()->GetObjectClientIDs(object_id);
    vector<ODRemoteConnectionInfo> v;
    for (const auto& client_id: client_ids) {
      ClientInformation client_info = this->gcs_client->client_table()->GetClientInformation(client_id);
      ODRemoteConnectionInfo info = ODRemoteConnectionInfo();
      info.ip = client_info.GetIpAddress();
      info.port = client_info.GetIpPort();
      v.push_back(info);
    }
    GetLocationsComplete(Status::OK(), object_id, v);
    return ray::Status::OK();
  };

  ray::Status ObjectDirectory::GetLocationsComplete(ray::Status status,
                                                    const ObjectID &object_id,
                                                    const std::vector<ODRemoteConnectionInfo> &v){

    bool success = status.ok();
    // Only invoke a callback if the request was not cancelled.
    if (existing_requests_.count(object_id) > 0) {
      ODCallbacks cbs = existing_requests_[object_id];
      if (success) {
        cbs.success_cb(v, object_id);
      } else {
        cbs.fail_cb(ray::Status::IOError("Something went wrong."), object_id);
      }
    }
    return status;
  };

  ray::Status ObjectDirectory::Cancel(const ObjectID &object_id) {
    existing_requests_.erase(object_id);
    return ray::Status::OK();
  };

  ray::Status ObjectDirectory::Terminate(){
    return ray::Status::OK();
  };

} // namespace ray
