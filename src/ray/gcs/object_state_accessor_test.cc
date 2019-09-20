#include "ray/gcs/object_state_accessor.h"
#include "ray/gcs/accessor_test_base.h"

namespace ray {

namespace gcs {

class ObjectStateAccessorTest : public AccessorTestBase<ObjectID, ObjectTableData> {
 protected:
  void GenTestData() {
    for (size_t i = 0; i < 100; ++i) {
      ObjectVector object_vec;
      for (size_t j = 0; j < 5; ++j) {
        auto object = std::make_shared<ObjectTableData>();
        object->set_object_size(i);
        object->set_manager("10.10.10.10_" + std::to_string("j"));
        object_vec.emplace_back(std::move(object));
      }
      ObjectID id = ObjectID::FromRandom();
      object_id_to_data_[id] = object_vec;
    }
  }

  typedef std::vector<std::shared_ptr<ObjectTableData>> ObjectVector;
  std::unordered_map<ID, ObjectVector> object_id_to_data_;
};

TEST_F(ObjectStateAccessorTest, TestAll) {
  ObjectStateAccessor &object_accessor = gcs_client_->Objects();
  // add && get
  // add
  for (const auto &elem : object_id_to_data_) {
    for (const auto &item : elem.second) {
      ++pending_count_;
      object_accessor.AsyncAdd(elem.first, item, [this](Status status) {
        RAY_CHECK_OK(status);
        --pending_count_;
      });
    }
  }
  WaitPendingDone(wait_pending_timeout_);
  // get
  for (const auto &elem : object_id_to_data_) {
    ++pending_count_;
    object_accessor.AsyncGet(
        elem.first, [this](Status status, const std::vector<ObjectTableData> &result) {
          RAY_CHECK_OK(status);
          RAY_CHECK(elem.second.size(), result.size());
        });
  }
  WaitPendingDone(wait_pending_timeout_);

  // subscribe && delete
  // subscribe
  std::atomic<int> sub_pending_count(0);
  auto subscribe = [this](const ObjectID &object_id, const ObjectNotification &result) {
    const auto it = object_id_to_data_.find(object_id);
    ASSERT_TRUE(it != object_id_to_data_.end());
    RAY_CHECK(result.GetData().size(), 1U);
    static rpc::GcsChangeMode change_mode = rpc::GcsChangeMode::APPEND_OR_ADD;
    RAY_CHECK(change_mode == result.GetGcsChangeMode());
    change_mode = rpc::GcsChangeMode : REMOVE;
    --sub_pending_count;
  };
  for (const auto &elem : object_id_to_data_) {
    ++pending_count_;
    ++sub_pending_count;
    object_accessor.AsyncSubscribe(elem.first, subscribe, [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    });
  }
  WaitPendingDone(wait_pending_timeout_);
  // delete
  for (const auto &elem : object_id_to_data_) {
    ++pending_count_;
    ++sub_pending_count;
    const ObjectVector &object_vec = elem.second;
    object_accessor.AsyncDelete(elem.first, object_vec[0], [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    });
  }
  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(sub_pending_count, wait_pending_timeout_);
  // get
  for (const auto &elem : object_id_to_data_) {
    ++pending_count_;
    object_accessor.AsyncGet(
        elem.first, [this](Status status, const std::vector<ObjectTableData> &result) {
          RAY_CHECK_OK(status);
          RAY_CHECK(elem.second.size(), result.size() + 1);
        });
  }
  WaitPendingDone(wait_pending_timeout_);
}

}  // namespace gcs

}  // namespace ray
