// Copyright 2026 The Ray Authors.
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

#include "ray/asio/instrumented_io_context.h"
#include "ray/common/scheduling/cluster_resource_storage_interface.h"
#include "ray/gcs/gcs_table_storage.h"

namespace ray {
namespace gcs {

class ClusterResourceStorage : public ClusterResourceStorageInterface {
 public:
  ClusterResourceStorage(GcsTableStorage *gcs_table_storage,
                         instrumented_io_context &io_context);

  ~ClusterResourceStorage();

  void UpdateStoredResources(const scheduling::NodeID node_id,
                             const NodeResources &node_resources) override;

  void DeleteStoredResources(const scheduling::NodeID node_id) override;

 private:
  void Put(const ray::NodeID node_id, const rpc::ResourcesData &data);
  GcsTableStorage *gcs_table_storage_;
  instrumented_io_context &io_context_;
};

}  // namespace gcs
}  // namespace ray
