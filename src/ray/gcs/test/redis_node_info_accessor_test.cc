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

#include <memory>
#include "gtest/gtest.h"
#include "ray/gcs/redis_accessor.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/gcs/test/accessor_test_base.h"

namespace ray {

namespace gcs {

class NodeDynamicResourceTest : public AccessorTestBase<ClientID, ResourceTableData> {
 protected:
  typedef NodeInfoAccessor::ResourceMap ResourceMap;
  virtual void GenTestData() {
    for (size_t node_index = 0; node_index < node_number_; ++node_index) {
      ClientID id = ClientID::FromRandom();
      ResourceMap resource_map;
      for (size_t rs_index = 0; rs_index < resource_type_number_; ++rs_index) {
        std::shared_ptr<ResourceTableData> rs_data =
            std::make_shared<ResourceTableData>();
        rs_data->set_resource_capacity(rs_index);
        std::string resource_name = std::to_string(rs_index);
        resource_map[resource_name] = rs_data;
        if (resource_to_delete_.empty()) {
          resource_to_delete_.emplace_back(resource_name);
        }
      }
      id_to_resource_map_[id] = std::move(resource_map);
    }
  }

  std::unordered_map<ClientID, ResourceMap> id_to_resource_map_;

  size_t node_number_{100};
  size_t resource_type_number_{5};

  std::vector<std::string> resource_to_delete_;

  std::atomic<int> sub_pending_count_{0};
  std::atomic<int> do_sub_pending_count_{0};
};

TEST_F(NodeDynamicResourceTest, UpdateAndGet) {
  NodeInfoAccessor &node_accessor = gcs_client_->Nodes();
  for (const auto &node_rs : id_to_resource_map_) {
    ++pending_count_;
    const ClientID &id = node_rs.first;
    // Update
    Status status = node_accessor.AsyncUpdateResources(
        node_rs.first, node_rs.second, [this, &node_accessor, id](Status status) {
          RAY_CHECK_OK(status);
          auto get_callback = [this, id](Status status,
                                         const boost::optional<ResourceMap> &result) {
            --pending_count_;
            RAY_CHECK_OK(status);
            const auto it = id_to_resource_map_.find(id);
            ASSERT_TRUE(result);
            ASSERT_EQ(it->second.size(), result->size());
          };
          // Get
          status = node_accessor.AsyncGetResources(id, get_callback);
          RAY_CHECK_OK(status);
        });
  }
  WaitPendingDone(wait_pending_timeout_);
}

TEST_F(NodeDynamicResourceTest, Delete) {
  NodeInfoAccessor &node_accessor = gcs_client_->Nodes();
  for (const auto &node_rs : id_to_resource_map_) {
    ++pending_count_;
    // Update
    Status status = node_accessor.AsyncUpdateResources(node_rs.first, node_rs.second,
                                                       [this](Status status) {
                                                         RAY_CHECK_OK(status);
                                                         --pending_count_;
                                                       });
  }
  WaitPendingDone(wait_pending_timeout_);

  for (const auto &node_rs : id_to_resource_map_) {
    ++pending_count_;
    const ClientID &id = node_rs.first;
    // Delete
    Status status = node_accessor.AsyncDeleteResources(
        id, resource_to_delete_, [this, &node_accessor, id](Status status) {
          RAY_CHECK_OK(status);
          // Get
          status = node_accessor.AsyncGetResources(
              id, [this, id](Status status, const boost::optional<ResourceMap> &result) {
                --pending_count_;
                RAY_CHECK_OK(status);
                const auto it = id_to_resource_map_.find(id);
                ASSERT_TRUE(result);
                ASSERT_EQ(it->second.size() - resource_to_delete_.size(), result->size());
              });
        });
  }
  WaitPendingDone(wait_pending_timeout_);
}

TEST_F(NodeDynamicResourceTest, Subscribe) {
  NodeInfoAccessor &node_accessor = gcs_client_->Nodes();
  for (const auto &node_rs : id_to_resource_map_) {
    ++pending_count_;
    // Update
    Status status = node_accessor.AsyncUpdateResources(node_rs.first, node_rs.second,
                                                       [this](Status status) {
                                                         RAY_CHECK_OK(status);
                                                         --pending_count_;
                                                       });
  }
  WaitPendingDone(wait_pending_timeout_);

  auto subscribe = [this](const ClientID &id,
                          const ResourceChangeNotification &notification) {
    RAY_LOG(INFO) << "receive client id=" << id;
    auto it = id_to_resource_map_.find(id);
    ASSERT_TRUE(it != id_to_resource_map_.end());
    if (notification.IsAdded()) {
      ASSERT_EQ(notification.GetData().size(), it->second.size());
    } else {
      ASSERT_EQ(notification.GetData().size(), resource_to_delete_.size());
    }
    --sub_pending_count_;
  };

  auto done = [this](Status status) {
    RAY_CHECK_OK(status);
    --pending_count_;
  };

  // Subscribe
  ++pending_count_;
  Status status = node_accessor.AsyncSubscribeToResources(subscribe, done);
  RAY_CHECK_OK(status);

  for (const auto &node_rs : id_to_resource_map_) {
    // Delete
    ++pending_count_;
    ++sub_pending_count_;
    Status status = node_accessor.AsyncDeleteResources(node_rs.first, resource_to_delete_,
                                                       [this](Status status) {
                                                         RAY_CHECK_OK(status);
                                                         --pending_count_;
                                                       });
    RAY_CHECK_OK(status);
  }

  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(sub_pending_count_, wait_pending_timeout_);
}

}  // namespace gcs

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
