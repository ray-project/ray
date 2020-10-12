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

#pragma once

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

class GcsInitData {
 public:
  explicit GcsInitData(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
      : gcs_table_storage_(std::move(gcs_table_storage)) {}

  void AsyncLoad(const EmptyCallback &on_done);

  const std::unordered_map<JobID, rpc::JobTableData> &Jobs() const {
    return job_table_data_;
  }

  const std::unordered_map<NodeID, rpc::GcsNodeInfo> &Nodes() const {
    return node_table_data_;
  }

  const std::unordered_map<ObjectID, rpc::ObjectLocationInfo> &Objects() const {
    return object_table_data_;
  }

  const std::unordered_map<NodeID, rpc::ResourceMap> &ClusterResources() const {
    return resource_table_data_;
  }

  const std::unordered_map<ActorID, rpc::ActorTableData> &Actors() const {
    return actor_table_data_;
  }

 private:
  void AsyncLoadJobTableData(const EmptyCallback &on_done);

  void AsyncLoadNodeTableData(const EmptyCallback &on_done);

  void AsyncLoadObjectTableData(const EmptyCallback &on_done);

  void AsyncLoadResourceTableData(const EmptyCallback &on_done);

  void AsyncLoadActorTableData(const EmptyCallback &on_done);

 protected:
  /// The gcs table storage.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;

  std::unordered_map<JobID, rpc::JobTableData> job_table_data_;

  std::unordered_map<NodeID, rpc::GcsNodeInfo> node_table_data_;

  std::unordered_map<ObjectID, rpc::ObjectLocationInfo> object_table_data_;

  std::unordered_map<NodeID, rpc::ResourceMap> resource_table_data_;

  std::unordered_map<ActorID, rpc::ActorTableData> actor_table_data_;
};

}  // namespace gcs
}  // namespace ray
