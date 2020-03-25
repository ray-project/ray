// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "object_info_handler_impl.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultObjectInfoHandler::HandleGetObjectLocations(
    const GetObjectLocationsRequest &request, GetObjectLocationsReply *reply,
    SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  RAY_LOG(DEBUG) << "Getting object locations, job id = " << object_id.TaskId().JobId()
                 << ", object id = " << object_id;

  auto on_done = [reply, object_id, send_reply_callback](
                     Status status, const std::vector<rpc::ObjectTableData> &result) {
    if (status.ok()) {
      for (const rpc::ObjectTableData &object_table_data : result) {
        reply->add_object_table_data_list()->CopyFrom(object_table_data);
      }
    } else {
      RAY_LOG(ERROR) << "Failed to get object locations: " << status.ToString()
                     << ", job id = " << object_id.TaskId().JobId()
                     << ", object id = " << object_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Objects().AsyncGetLocations(object_id, on_done);
  if (!status.ok()) {
    on_done(status, std::vector<rpc::ObjectTableData>());
  }

  RAY_LOG(DEBUG) << "Finished getting object locations, job id = "
                 << object_id.TaskId().JobId() << ", object id = " << object_id;
}

void DefaultObjectInfoHandler::HandleAddObjectLocation(
    const AddObjectLocationRequest &request, AddObjectLocationReply *reply,
    SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Adding object location, job id = " << object_id.TaskId().JobId()
                 << ", object id = " << object_id << ", node id = " << node_id;

  auto on_done = [object_id, node_id, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add object location: " << status.ToString()
                     << ", job id = " << object_id.TaskId().JobId()
                     << ", object id = " << object_id << ", node id = " << node_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Objects().AsyncAddLocation(object_id, node_id, on_done);
  if (!status.ok()) {
    on_done(status);
  }

  RAY_LOG(DEBUG) << "Finished adding object location, job id = "
                 << object_id.TaskId().JobId() << ", object id = " << object_id
                 << ", node id = " << node_id;
}

void DefaultObjectInfoHandler::HandleRemoveObjectLocation(
    const RemoveObjectLocationRequest &request, RemoveObjectLocationReply *reply,
    SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Removing object location, job id = " << object_id.TaskId().JobId()
                 << ", object id = " << object_id << ", node id = " << node_id;

  auto on_done = [object_id, node_id, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to remove object location: " << status.ToString()
                     << ", job id = " << object_id.TaskId().JobId()
                     << ", object id = " << object_id << ", node id = " << node_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Objects().AsyncRemoveLocation(object_id, node_id, on_done);
  if (!status.ok()) {
    on_done(status);
  }

  RAY_LOG(DEBUG) << "Finished removing object location, job id = "
                 << object_id.TaskId().JobId() << ", object id = " << object_id
                 << ", node id = " << node_id;
}

}  // namespace rpc
}  // namespace ray
