#include <iostream>

#include "ray/om/object_directory.h"

using namespace std;

namespace ray {

ObjectDirectory::ObjectDirectory() = default;
ObjectDirectory::~ObjectDirectory() = default;

void ObjectDirectory::InitGcs(std::shared_ptr<GcsClient> gcs_client){
  this->gcs_client = gcs_client;
};

ray::Status ObjectDirectory::ObjectAdded(const ObjectID &object_id,
                                         const ClientID &client_id){
  return this->gcs_client->object_table().Add(object_id, client_id, []{});
};

ray::Status ObjectDirectory::ObjectRemoved(const ObjectID &object_id,
                                           const ClientID &client_id){
  return this->gcs_client->object_table().Remove(object_id, client_id, []{});
};

ray::Status ObjectDirectory::GetInformation(const ClientID &client_id,
                                            const InfoSuccCB &success_cb,
                                            const InfoFailCB &fail_cb){
  this->gcs_client->client_table().GetClientInformation(
      client_id,
      [this, success_cb, client_id](ClientInformation client_info){
        const auto &info = RemoteConnectionInfo(client_id,
                                                  client_info.GetIp(),
                                                  client_info.GetPort());
        success_cb(info);
      },
      fail_cb
  );
  return ray::Status::OK();
};

ray::Status ObjectDirectory::GetLocations(const ObjectID &object_id,
                                          const LocSuccCB &success_cb,
                                          const LocFailCB &fail_cb) {
  ray::Status status_code = ray::Status::OK();
  if (existing_requests_.count(object_id) == 0) {
    existing_requests_[object_id] = ODCallbacks({success_cb, fail_cb});;
    status_code = ExecuteGetLocations(object_id);
  } else {
    // Do nothing. A request is in progress.
  }
  return status_code;
};

ray::Status ObjectDirectory::ExecuteGetLocations(const ObjectID &object_id){
  // TODO(hme): Avoid callback hell.
  vector<RemoteConnectionInfo> v;
  ray::Status status = this->gcs_client->object_table().GetObjectClientIDs(
      object_id,
      [this, object_id, &v](const vector<ClientID> &client_ids){
          this->gcs_client->client_table().GetClientInformationSet(
              client_ids,
              [this, object_id, &v](const vector<ClientInformation> &info_vec){
                for (const auto& client_info: info_vec) {
                  RemoteConnectionInfo info = RemoteConnectionInfo(
                      client_info.GetClientId(), client_info.GetIp(), client_info.GetPort());
                  v.push_back(info);
                }
                ray::Status cb_completion_status =
                    GetLocationsComplete(Status::OK(), object_id, v);
              },
              [this, object_id, &v](const Status &status){
                ray::Status cb_completion_status =
                    GetLocationsComplete(status, object_id, v);
              }
          );
      },
      [this, object_id, &v](const Status &status){
        ray::Status cb_completion_status =
            GetLocationsComplete(status, object_id, v);
      }
  );
  return status;
};

ray::Status ObjectDirectory::GetLocationsComplete(const ray::Status &status,
                                                  const ObjectID &object_id,
                                                  const std::vector<RemoteConnectionInfo> &v){
  bool success = status.ok();
  // Only invoke a callback if the request was not cancelled.
  if (existing_requests_.count(object_id) > 0) {
    ODCallbacks cbs = existing_requests_[object_id];
    if (success) {
      cbs.success_cb(v, object_id);
    } else {
      cbs.fail_cb(status, object_id);
    }
  }
  existing_requests_.erase(object_id);
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
