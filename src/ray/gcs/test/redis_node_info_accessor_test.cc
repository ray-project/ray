#include "ray/gcs/redis_accessor.h"
#include <memory>
#include "gtest/gtest.h"
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
        std::shared_ptr<ResourceTableData> rs_data = std::make_shared<ResourceTableData>();
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
};

TEST_F(NodeDynamicResourceTest, UpdateAndGet) {
  NodeInfoAccessor &node_accessor = gcs_client_->Nodes();
  for (const auto &node_rs : id_to_resource_map_) {
    ++pending_count_;
    const ClientID &id = node_rs.first;
    // Update
    Status status = node_accessor.AsyncUpdateResource(
        node_rs.first, node_rs.second, [this, &node_accessor, id](Status status) {
          RAY_CHECK_OK(status);
          auto get_callback = [this, id](
                                  Status status,
                                  const boost::optional<ResourceMap> &result) {
            --pending_count_;
            RAY_CHECK_OK(status);
            const auto it = id_to_resource_map_.find(id);
            ASSERT_TRUE(result);
            ASSERT_EQ(it->second.size(), result->size());
          };
          // Get
          status = node_accessor.AsyncGetResource(id, get_callback);
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
    Status status = node_accessor.AsyncUpdateResource(node_rs.first, node_rs.second,
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
    Status status = node_accessor.AsyncDeleteResource(
        id, resource_to_delete_, [this, &node_accessor, id](Status status) {
          RAY_CHECK_OK(status);
          // Get
          status = node_accessor.AsyncGetResource(
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
    Status status = node_accessor.AsyncUpdateResource(node_rs.first, node_rs.second,
                                                      [this](Status status) {
                                                        RAY_CHECK_OK(status);
                                                        --pending_count_;
                                                      });
  }
  WaitPendingDone(wait_pending_timeout_);

  for (const auto &node_rs : id_to_resource_map_) {
    const ClientID &id = node_rs.first;

    auto subscribe = [this](const ClientID &id,
                            const ResourceChangeNotification &notification) {
      auto it = id_to_resource_map_.find(id);
      ASSERT_TRUE(it != id_to_resource_map_.end());
      if (notification.GetGcsChangeMode() == rpc::GcsChangeMode::APPEND_OR_ADD) {
        ASSERT_EQ(notification.GetData().size(), it->second.size());
      } else {
        ASSERT_EQ(notification.GetData().size(), resource_to_delete_.size());
      }
      --sub_pending_count_;
    };

    auto done = [this, &node_accessor, id](Status status) {
      RAY_CHECK_OK(status);
      // Delete
      ++sub_pending_count_;
      status = node_accessor.AsyncDeleteResource(id, resource_to_delete_, 
                                                 [this](Status status) {
                                                   --pending_count_;
                                                   RAY_CHECK_OK(status);
                                                 });
    };

    ++pending_count_;
    ++sub_pending_count_;
    // Subscribe
    Status status = node_accessor.AsyncSubscribeResource(subscribe, done);
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
