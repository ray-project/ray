#include "object_directory.h"

namespace ray {

ObjectDirectory::ObjectDirectory(std::shared_ptr<gcs::AsyncGcsClient> gcs_client) {
  gcs_client_ = gcs_client;
};

ray::Status ObjectDirectory::ReportObjectAdded(const ObjectID &object_id,
                                               const ClientID &client_id) {
  // TODO(hme): Determine whether we need to do lookup to append.
  JobID job_id = JobID::from_random();
  auto data = std::make_shared<ObjectTableDataT>();
  data->managers.push_back(client_id.binary());
  ray::Status status = gcs_client_->object_table().Add(
      job_id,
      object_id,
      data,
      [](
      gcs::AsyncGcsClient *client,
      const UniqueID &id,
      std::shared_ptr<ObjectTableDataT> data){
        // Do nothing.
      });
  return status;
};

ray::Status ObjectDirectory::ReportObjectRemoved(const ObjectID &object_id,
                                                 const ClientID &client_id) {
  //TODO(hme): uncomment when Remove is implemented.
//  JobID job_id = JobID::from_random();
//  auto data = std::make_shared<ObjectTableDataT>();
//  data->managers.push_back(client_id.binary());
//  ray::Status status = gcs_client_->object_table().Remove(
//      job_id,
//      object_id,
//      [](
//          gcs::AsyncGcsClient *client,
//          const UniqueID &id,
//          std::shared_ptr<ObjectTableDataT> data){
//        std::cout << "Removed: " << id << std::endl;
//      });
//  return status;
  return Status::OK();
};

ray::Status ObjectDirectory::GetInformation(const ClientID &client_id,
                                            const InfoSuccessCallback &success_cb,
                                            const InfoFailureCallback &fail_cb) {
  const ClientTableDataT &data = gcs_client_->client_table().GetClient(client_id);
  ClientID result_client_id = ClientID::from_binary(data.client_id);
  if(result_client_id == ClientID::nil()){
    fail_cb(ray::Status::RedisError("ClientID not found."));
  } else {
    const auto &info =
        RemoteConnectionInfo(client_id,
                             data.node_manager_address,
                             (uint16_t) data.object_manager_port);
    success_cb(info);
  }
  return ray::Status::OK();
};

ray::Status ObjectDirectory::GetLocations(const ObjectID &object_id,
                                          const OnLocationsSuccess &success_cb,
                                          const OnLocationsFailure &fail_cb) {
  ray::Status status_code = ray::Status::OK();
  if (existing_requests_.count(object_id) == 0) {
    existing_requests_[object_id] = ODCallbacks({success_cb, fail_cb});
    status_code = ExecuteGetLocations(object_id);
  } else {
    // Do nothing. A request is in progress.
  }
  return status_code;
};

ray::Status ObjectDirectory::ExecuteGetLocations(const ObjectID &object_id) {
  std::vector<ClientID> remote_connections;
  JobID job_id = JobID::from_random();
  ray::Status status = gcs_client_->object_table().Lookup(
  job_id,
  object_id,
  [this, object_id, &remote_connections](gcs::AsyncGcsClient *client,
                                         const UniqueID &id,
                                         std::shared_ptr<ObjectTableDataT> data){
    for(auto client_id_binary : data->managers){
      ClientID client_id = ClientID::from_binary(client_id_binary);
      remote_connections.push_back(client_id);
    }
    GetLocationsComplete(Status::OK(), object_id, remote_connections);
  });
  return status;
};

ray::Status ObjectDirectory::GetLocationsComplete(
    const ray::Status &status, const ObjectID &object_id,
    const std::vector<ClientID> &remote_connections) {
  bool success = status.ok();
  // Only invoke a callback if the request was not cancelled.
  if (existing_requests_.count(object_id) > 0) {
    ODCallbacks cbs = existing_requests_[object_id];
    if (success) {
      cbs.success_cb(remote_connections, object_id);
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

ray::Status ObjectDirectory::Terminate() { return ray::Status::OK(); };

}  // namespace ray
