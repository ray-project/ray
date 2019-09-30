#include "ray/gcs/dynamic_resource_state_accessor.h"
#include "gtest/gtest.h"
#include "ray/gcs/accessor_test_base.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

class DynamicResourceStateAccessorTest
    : public AccessorTestBase<ClientID, ResourceTableData> {
 protected:
  virtual void GenTestData() {
    for (size_t node_index = 0; node_index < node_number_; ++node_index) {
      ClientID id = ClientID::FromRandom();
      ResourceMap resource_map;
      for (size_t rs_index = 0; rs_index < resource_type_number_; ++rs_index) {
        ResourceTableData rs_data;
        rs_data.set_resource_capacity(rs_index);
        resource_map[std::to_string(rs_index)] = std::move(rs_data);
        if (resource_to_delete_.empty()) {
          resource_to_delete_.emplace_back(rs_index);
        }
      }
      id_to_resource_map_[id] = std::move(resource_map);
    }
  }

  typedef DynamicResourceStateAccessor::ResourceMap ResourceMap;
  std::unordered_map<ClientID, ResourceMap> id_to_resource_map_;

  size_t node_number_{100};
  size_t resource_type_number_{5};

  std::vector<std::string> resource_to_delete_;

  std::atomic<int> sub_pending_count_{0};
};

TEST_F(DynamicResourceStateAccessorTest, UpdateAndGet) {
  DynamicResourceStateAccessor &resource_accessor = gcs_client_->DynamicResources();
  for (const auto &node_rs : id_to_resource_map_) {
    ++pending_count_;
    const ClientID &id = node_rs.first;
    // Update
    Status status = resource_accessor->AsyncUpdate(
        node_rs.first, node_rs.second, [this, id](Status status) {
          RAY_CHECK_OK(status);
          auto get_callback = [this, &resource_accessor, id](
                                  Status status,
                                  const boost::optional<ResourceMap> &result) {
            --pending_count_;
            RAY_CHECK_OK(status);
            const it = id_to_resource_map_.find(id);
            ASSERT(result);
            ASSERT_EQ(it->second.size(), result->size());
          };
          // Get
          status = resource_accessor->AsyncGet(id, get_callback);
          RAY_CHECK_OK(status);
        });
  }
  WaitPendingDone(wait_pending_timeout_);
}

TEST_F(DynamicResourceStateAccessorTest, Delete) {
  DynamicResourceStateAccessor &resource_accessor = gcs_client_->DynamicResources();
  for (const auto &node_rs : id_to_resource_map_) {
    ++pending_count_;
    // Update
    Status status =
        resource_accessor->AsyncUpdate(node_rs.first, node_rs.second, [](Status status) {
          RAY_CHECK_OK(status);
          --pending_count_;
        });
  }
  WaitPendingDone(wait_pending_timeout_);

  for (const auto &node_rs : id_to_resource_map_) {
    ++pending_count_;
    const ClientID &id = node_rs.first;
    // Delete
    Status status = resource_accessor->AsyncDelete(
        id, resource_to_delete_, [this, &resource_accessor, id](Status status) {
          RAY_CHECK_OK(status);
          // Get
          status = resource_accessor->AsyncGet(
              id, [this, id](Status status, const boost::optional<ResourceMap> &result) {
                --pending_count_;
                RAY_CHECK_OK(status);
                const it = id_to_resource_map_.find(id);
                ASSERT(result);
                ASSERT_EQ(it->second.size() - resource_to_delete_.size(), result->size());
              });
        });
  }
  WaitPendingDone(wait_pending_timeout_);
}

TEST_F(DynamicResourceStateAccessorTest, Subscribe) {
  DynamicResourceStateAccessor &resource_accessor = gcs_client_->DynamicResources();
  for (const auto &node_rs : id_to_resource_map_) {
    ++pending_count_;
    // Update
    Status status = resource_accessor->AsyncUpdate(node_rs.first, node_rs.second,
                                                   [this](Status status) {
                                                     RAY_CHECK_OK(status);
                                                     --pending_count_;
                                                   });
  }
  WaitPendingDone(wait_pending_timeout_);

  for (const auto &node_rs : id_to_resource_map_) {
    const ClientID &id = node_rs.first;

    auto subscribe = [this](const ClientID &id,
                            const DynamicResourceNotification &notification) {
      auto it = id_to_resource_map_.find(id);
      ASSERT(it != id_to_resource_map_.end());
      if (notification.GetGcsChangeMode() == rpc::GcsChangeMode::APPEND_OR_ADD) {
        ASSERT_EQ(notification.GetData(), size(), it->second.size());
      } else {
        ASSERT_EQ(notification.GetData(), size(), resource_to_delete_.size());
      }
      --sub_pending_count_;
    };

    auto done = [this, &resource_accessor, id](Status status) {
      RAY_CHECK_OK(status);
      // Delete
      ++sub_pending_count_;
      status = resource_accessor.AsyncDelete(id, resource_to_delete_, [this](status) {
        --pending_count_;
        RAY_CHECK_OK(status);
      };
    };

    ++pending_count_;
    ++sub_pending_count_;
    // Subscribe
    Status status = resource_accessor.AsyncSubscribe(subscribe, done);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(sub_pending_count_, wait_pending_timeout_);
}

}  // namespace gcs

}  // namespace ray
