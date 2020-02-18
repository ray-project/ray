#include "ray/core_worker/reference_count.h"

#include <vector>

#include "gtest/gtest.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"

namespace ray {

static const rpc::Address empty_borrower;
static const ReferenceCounter::ReferenceTableProto empty_refs;

class ReferenceCountTest : public ::testing::Test {
 protected:
  std::unique_ptr<ReferenceCounter> rc;
  virtual void SetUp() { rc = std::unique_ptr<ReferenceCounter>(new ReferenceCounter); }

  virtual void TearDown() {}
};

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  MockWorkerClient(ReferenceCounter &rc, const std::string &addr)
      : rc_(rc), task_id_(TaskID::ForFakeTask()) {
    address_.set_ip_address(addr);
    address_.set_raylet_id(ClientID::FromRandom().Binary());
    address_.set_worker_id(WorkerID::FromRandom().Binary());
  }

  ray::Status WaitForRefRemoved(
      const rpc::WaitForRefRemovedRequest &request,
      const rpc::ClientCallback<rpc::WaitForRefRemovedReply> &callback) override {
    auto r = num_requests_;
    requests_[r] = {
        std::make_shared<rpc::WaitForRefRemovedReply>(),
        callback,
    };

    auto send_reply_callback = [this, r](Status status, std::function<void()> success,
                                         std::function<void()> failure) {
      requests_[r].second(status, *requests_[r].first);
    };
    auto borrower_callback = [=]() {
      const ObjectID &object_id = ObjectID::FromBinary(request.reference().object_id());
      ObjectID contained_in_id = ObjectID::FromBinary(request.contained_in_id());
      const auto owner_id = TaskID::FromBinary(request.reference().owner_id());
      const auto owner_address = request.reference().owner_address();
      auto ref_removed_callback =
          boost::bind(&ReferenceCounter::HandleRefRemoved, &rc_, _1,
                      requests_[r].first.get(), send_reply_callback);
      rc_.SetRefRemovedCallback(object_id, contained_in_id, owner_id, owner_address,
                                ref_removed_callback);
    };
    borrower_callbacks_[r] = borrower_callback;

    num_requests_++;
    return Status::OK();
  }

  bool FlushBorrowerCallbacks() {
    if (borrower_callbacks_.empty()) {
      return false;
    } else {
      for (auto &callback : borrower_callbacks_) {
        callback.second();
      }
      borrower_callbacks_.clear();
      return true;
    }
  }

  // The below methods mirror a core worker's operations, e.g., `Put` simulates
  // a ray.put().
  void Put(const ObjectID &object_id) {
    rc_.AddOwnedObject(object_id, {}, task_id_, address_);
    rc_.AddLocalReference(object_id);
  }

  void PutWrappedId(const ObjectID outer_id, const ObjectID &inner_id) {
    rc_.AddOwnedObject(outer_id, {inner_id}, task_id_, address_);
    rc_.AddLocalReference(outer_id);
  }

  void GetSerializedObjectId(const ObjectID outer_id, const ObjectID &inner_id,
                             const TaskID &owner_id, const rpc::Address &owner_address) {
    rc_.AddLocalReference(inner_id);
    rc_.AddBorrowedObject(inner_id, outer_id, owner_id, owner_address);
  }

  void ExecuteTaskWithArg(const ObjectID &arg_id, const ObjectID &inner_id,
                          const TaskID &owner_id, const rpc::Address &owner_address) {
    // Add a sentinel reference to keep the argument ID in scope even though
    // the frontend won't have a reference.
    rc_.AddLocalReference(arg_id);
    GetSerializedObjectId(arg_id, inner_id, owner_id, owner_address);
  }

  ObjectID SubmitTaskWithArg(const ObjectID &arg_id) {
    rc_.AddSubmittedTaskReferences({arg_id});
    ObjectID return_id = ObjectID::FromRandom();
    rc_.AddOwnedObject(return_id, {}, task_id_, address_);
    // Add a sentinel reference to keep all nested object IDs in scope.
    rc_.AddLocalReference(return_id);
    return return_id;
  }

  ReferenceCounter::ReferenceTableProto FinishExecutingTask(
      const ObjectID &arg_id, const ObjectID &return_id,
      const ObjectID *return_wrapped_id = nullptr,
      const rpc::WorkerAddress *owner_address = nullptr) {
    if (return_wrapped_id) {
      rc_.WrapObjectIds(return_id, {*return_wrapped_id}, *owner_address);
    }

    ReferenceCounter::ReferenceTableProto refs;
    if (!arg_id.IsNil()) {
      rc_.GetAndClearLocalBorrowers({arg_id}, &refs);
      // Remove the sentinel reference.
      rc_.RemoveLocalReference(arg_id, nullptr);
    }
    return refs;
  }

  void HandleSubmittedTaskFinished(
      const ObjectID &arg_id, const rpc::Address &borrower_address = empty_borrower,
      const ReferenceCounter::ReferenceTableProto &borrower_refs = empty_refs) {
    if (!arg_id.IsNil()) {
      rc_.UpdateSubmittedTaskReferences({arg_id}, borrower_address, borrower_refs,
                                        nullptr);
    }
  }

  // Global map from Worker ID -> MockWorkerClient.
  // Global map from Object ID -> owner worker ID, list of objects that it depends on,
  // worker address that it's scheduled on. Worker map of pending return IDs.

  // The ReferenceCounter at the "client".
  ReferenceCounter &rc_;
  TaskID task_id_;
  rpc::Address address_;
  std::unordered_map<int, std::function<void()>> borrower_callbacks_;
  std::unordered_map<int, std::pair<std::shared_ptr<rpc::WaitForRefRemovedReply>,
                                    rpc::ClientCallback<rpc::WaitForRefRemovedReply>>>
      requests_;
  int num_requests_ = 0;
};

// Tests basic incrementing/decrementing of direct/submitted task reference counts. An
// entry should only be removed once both of its reference counts reach zero.
TEST_F(ReferenceCountTest, TestBasic) {
  std::vector<ObjectID> out;

  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();

  // Local references.
  rc->AddLocalReference(id1);
  rc->AddLocalReference(id1);
  rc->AddLocalReference(id2);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveLocalReference(id2, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 1);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 2);
  out.clear();

  // Submitted task references.
  rc->AddSubmittedTaskReferences({id1});
  rc->AddSubmittedTaskReferences({id1, id2});
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  rc->UpdateSubmittedTaskReferences({id1}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->UpdateSubmittedTaskReferences({id2}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 1);
  rc->UpdateSubmittedTaskReferences({id1}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 2);
  out.clear();

  // Local & submitted task references.
  rc->AddLocalReference(id1);
  rc->AddSubmittedTaskReferences({id1, id2});
  rc->AddLocalReference(id2);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->UpdateSubmittedTaskReferences({id2}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->UpdateSubmittedTaskReferences({id1}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 1);
  rc->RemoveLocalReference(id2, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 2);
  out.clear();
}

// Tests that we can get the owner address correctly for objects that we own,
// objects that we borrowed via a serialized object ID, and objects whose
// origin we do not know.
TEST_F(ReferenceCountTest, TestOwnerAddress) {
  auto object_id = ObjectID::FromRandom();
  TaskID task_id = TaskID::ForFakeTask();
  rpc::Address address;
  address.set_ip_address("1234");
  rc->AddOwnedObject(object_id, {}, task_id, address);

  TaskID added_id;
  rpc::Address added_address;
  ASSERT_TRUE(rc->GetOwner(object_id, &added_id, &added_address));
  ASSERT_EQ(task_id, added_id);
  ASSERT_EQ(address.ip_address(), added_address.ip_address());

  auto object_id2 = ObjectID::FromRandom();
  task_id = TaskID::ForFakeTask();
  address.set_ip_address("5678");
  rc->AddOwnedObject(object_id2, {}, task_id, address);
  ASSERT_TRUE(rc->GetOwner(object_id2, &added_id, &added_address));
  ASSERT_EQ(task_id, added_id);
  ASSERT_EQ(address.ip_address(), added_address.ip_address());

  auto object_id3 = ObjectID::FromRandom();
  ASSERT_FALSE(rc->GetOwner(object_id3, &added_id, &added_address));
  rc->AddLocalReference(object_id3);
  ASSERT_FALSE(rc->GetOwner(object_id3, &added_id, &added_address));
}

// Tests that the ref counts are properly integrated into the local
// object memory store.
TEST(MemoryStoreIntegrationTest, TestSimple) {
  ObjectID id1 = ObjectID::FromRandom().WithDirectTransportType();
  ObjectID id2 = ObjectID::FromRandom().WithDirectTransportType();
  uint8_t data[] = {1, 2, 3, 4, 5, 6, 7, 8};
  RayObject buffer(std::make_shared<LocalMemoryBuffer>(data, sizeof(data)), nullptr, {});

  auto rc = std::shared_ptr<ReferenceCounter>(new ReferenceCounter());
  CoreWorkerMemoryStore store(nullptr, rc);

  // Tests putting an object with no references is ignored.
  RAY_CHECK_OK(store.Put(buffer, id2));
  ASSERT_EQ(store.Size(), 0);

  // Tests ref counting overrides remove after get option.
  rc->AddLocalReference(id1);
  RAY_CHECK_OK(store.Put(buffer, id1));
  ASSERT_EQ(store.Size(), 1);
  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext ctx(WorkerType::WORKER, JobID::Nil());
  RAY_CHECK_OK(store.Get({id1}, /*num_objects*/ 1, /*timeout_ms*/ -1, ctx,
                         /*remove_after_get*/ true, &results));
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(store.Size(), 1);
}

// A borrower is given a reference to an object ID, submits a task, waits for
// it to finish, then returns.
//
// @ray.remote
// def borrower(inner_ids):
//     inner_id = inner_ids[0]
//     ray.get(foo.remote(inner_id))
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestNoBorrow) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) { return borrower; });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner_rc.HasReference(outer_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  // The borrower submits a task that depends on the inner object.
  borrower->SubmitTaskWithArg(inner_id);
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The borrower waits for the task to finish before returning to the owner.
  borrower->HandleSubmittedTaskFinished(inner_id);
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  // Check that the borrower's ref count is now 0 for all objects.
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Check that owner's ref count is now 0 for all objects.
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(outer_id));
}

// A borrower is given a reference to an object ID, submits a task, does not
// wait for it to finish.
//
// @ray.remote
// def borrower(inner_ids):
//     inner_id = inner_ids[0]
//     foo.remote(inner_id)
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestSimpleBorrower) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) { return borrower; });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner_rc.HasReference(outer_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  // The borrower submits a task that depends on the inner object.
  borrower->SubmitTaskWithArg(inner_id);
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  // ASSERT_FALSE(borrower_rc.HasReference(outer_id));
  // Check that the borrower's ref count for inner_id > 0 because of the
  // pending task.
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));

  // The task submitted by the borrower returns. Everyone's ref count should go
  // to 0.
  borrower->HandleSubmittedTaskFinished(inner_id);
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(outer_id));
}

// A borrower is given a reference to an object ID, keeps the reference past
// the task's lifetime, then deletes the reference before it hears from the
// owner.
//
// @ray.remote
// class Borrower:
//     def __init__(self, inner_ids):
//        self.inner_id = inner_ids[0]
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = Borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestSimpleBorrowerReferenceRemoved) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) { return borrower; });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner_rc.HasReference(outer_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The borrower task returns to the owner while still using inner_id.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));

  // The borrower is no longer using inner_id, but it hasn't received the
  // message from the owner yet.
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower receives the owner's wait message. It should return a reply
  // to the owner immediately saying that it is no longer using inner_id.
  borrower->FlushBorrowerCallbacks();
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

// A borrower is given a reference to an object ID, passes the reference to
// another borrower by submitting a task, and does not wait for it to finish.
//
// @ray.remote
// def borrower2(inner_ids):
//     pass
//
// @ray.remote
// def borrower(inner_ids):
//     borrower2.remote(inner_ids)
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestBorrowerTree) {
  ReferenceCounter borrower_rc1;
  auto borrower1 = std::make_shared<MockWorkerClient>(borrower_rc1, "1");
  ReferenceCounter borrower_rc2;
  auto borrower2 = std::make_shared<MockWorkerClient>(borrower_rc2, "2");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) {
    if (addr.ip_address() == borrower1->address_.ip_address()) {
      return borrower1;
    } else {
      return borrower2;
    }
  });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "3");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner_rc.HasReference(outer_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // Borrower 1 is given a reference to the inner object.
  borrower1->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  // The borrower submits a task that depends on the inner object.
  auto outer_id2 = ObjectID::FromRandom();
  borrower1->PutWrappedId(outer_id2, inner_id);
  borrower1->SubmitTaskWithArg(outer_id2);
  borrower_rc1.RemoveLocalReference(inner_id, nullptr);
  borrower_rc1.RemoveLocalReference(outer_id2, nullptr);
  ASSERT_TRUE(borrower_rc1.HasReference(inner_id));
  ASSERT_TRUE(borrower_rc1.HasReference(outer_id2));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs = borrower1->FinishExecutingTask(outer_id, ObjectID::Nil());
  ASSERT_TRUE(borrower_rc1.HasReference(inner_id));
  ASSERT_TRUE(borrower_rc1.HasReference(outer_id2));
  ASSERT_FALSE(borrower_rc1.HasReference(outer_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower1->address_, borrower_refs);
  borrower1->FlushBorrowerCallbacks();
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));

  // Borrower 2 starts executing. It is given a reference to the inner object
  // when it gets outer_id2 as an argument.
  borrower2->ExecuteTaskWithArg(outer_id2, inner_id, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc2.HasReference(inner_id));
  // Borrower 2 finishes but it is still using inner_id.
  borrower_refs = borrower2->FinishExecutingTask(outer_id2, ObjectID::Nil());
  ASSERT_TRUE(borrower_rc2.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc2.HasReference(outer_id2));
  ASSERT_FALSE(borrower_rc2.HasReference(outer_id));

  borrower1->HandleSubmittedTaskFinished(outer_id2, borrower2->address_, borrower_refs);
  borrower2->FlushBorrowerCallbacks();
  // Borrower 1 no longer has a reference to any objects.
  ASSERT_FALSE(borrower_rc1.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc1.HasReference(outer_id2));
  // The owner should now have borrower 2 in its count.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  borrower_rc2.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower_rc2.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

// A task is given a reference to an object ID, whose value contains another
// object ID. The task gets a reference to the innermost object ID, but deletes
// it by the time the task finishes.
//
// @ray.remote
// def borrower(mid_ids):
//     inner_id = ray.get(mid_ids[0])
//     del inner_id
//
// inner_id = ray.put(1)
// mid_id = ray.put([inner_id])
// outer_id = ray.put([mid_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestNestedObjectNoBorrow) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) { return borrower; });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto mid_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(mid_id, inner_id);
  owner->PutWrappedId(outer_id, mid_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to mid_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(mid_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for all objects.
  ASSERT_TRUE(owner_rc.HasReference(outer_id));
  ASSERT_TRUE(owner_rc.HasReference(mid_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower is given a reference to the middle object.
  borrower->ExecuteTaskWithArg(outer_id, mid_id, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc.HasReference(mid_id));
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));

  // The borrower unwraps the inner object with ray.get.
  borrower->GetSerializedObjectId(mid_id, inner_id, owner->task_id_, owner->address_);
  borrower_rc.RemoveLocalReference(mid_id, nullptr);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  // The borrower's reference to inner_id goes out of scope.
  borrower_rc.RemoveLocalReference(inner_id, nullptr);

  // The borrower task returns to the owner.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));
  ASSERT_FALSE(borrower_rc.HasReference(mid_id));
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  // Check that owner now has nothing in scope.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));
  ASSERT_FALSE(owner_rc.HasReference(mid_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

// A task is given a reference to an object ID, whose value contains another
// object ID. The task gets a reference to the innermost object ID, and is
// still borrowing it by the time the task finishes.
//
// @ray.remote
// def borrower(mid_ids):
//     inner_id = ray.get(mid_ids[0])
//     foo.remote(inner_id)
//
// inner_id = ray.put(1)
// mid_id = ray.put([inner_id])
// outer_id = ray.put([mid_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestNestedObject) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) { return borrower; });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto mid_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(mid_id, inner_id);
  owner->PutWrappedId(outer_id, mid_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to mid_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(mid_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for all objects.
  ASSERT_TRUE(owner_rc.HasReference(outer_id));
  ASSERT_TRUE(owner_rc.HasReference(mid_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower is given a reference to the middle object.
  borrower->ExecuteTaskWithArg(outer_id, mid_id, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc.HasReference(mid_id));
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));

  // The borrower unwraps the inner object with ray.get.
  borrower->GetSerializedObjectId(mid_id, inner_id, owner->task_id_, owner->address_);
  borrower_rc.RemoveLocalReference(mid_id, nullptr);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The borrower task returns to the owner while still using inner_id.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));
  ASSERT_FALSE(borrower_rc.HasReference(mid_id));
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  // Check that owner's ref count for outer and mid are 0 since the borrower
  // task returned and there were no local references to outer_id.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));
  ASSERT_FALSE(owner_rc.HasReference(mid_id));

  // The borrower receives the owner's wait message. It should return a reply
  // to the owner immediately saying that it is no longer using inner_id.
  borrower->FlushBorrowerCallbacks();
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower is no longer using inner_id, but it hasn't received the
  // message from the owner yet.
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

// A borrower is given a reference to an object ID, whose value contains
// another object ID. The borrower passes the reference again to another
// borrower and waits for it to finish. The nested borrower unwraps the outer
// object and gets a reference to the innermost ID.
//
// @ray.remote
// def borrower2(owner_id2):
//     owner_id1 = ray.get(owner_id2[0])[0]
//     foo.remote(owner_id1)
//
// @ray.remote
// def borrower1(owner_id2):
//     ray.get(borrower2.remote(owner_id2))
//
// owner_id1 = ray.put(1)
// owner_id2 = ray.put([owner_id1])
// owner_id3 = ray.put([owner_id2])
// res = borrower1.remote(owner_id3)
TEST(DistributedReferenceCountTest, TestNestedObjectDifferentOwners) {
  ReferenceCounter borrower_rc1;
  auto borrower1 = std::make_shared<MockWorkerClient>(borrower_rc1, "1");
  ReferenceCounter borrower_rc2;
  auto borrower2 = std::make_shared<MockWorkerClient>(borrower_rc2, "2");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) {
    if (addr.ip_address() == borrower1->address_.ip_address()) {
      return borrower1;
    } else {
      return borrower2;
    }
  });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "3");

  // The owner creates an inner object and wraps it.
  auto owner_id1 = ObjectID::FromRandom();
  auto owner_id2 = ObjectID::FromRandom();
  auto owner_id3 = ObjectID::FromRandom();
  owner->Put(owner_id1);
  owner->PutWrappedId(owner_id2, owner_id1);
  owner->PutWrappedId(owner_id3, owner_id2);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to owner_id2.
  owner->SubmitTaskWithArg(owner_id3);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(owner_id1, nullptr);
  owner_rc.RemoveLocalReference(owner_id2, nullptr);
  owner_rc.RemoveLocalReference(owner_id3, nullptr);

  // The borrower is given a reference to the middle object.
  borrower1->ExecuteTaskWithArg(owner_id3, owner_id2, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc1.HasReference(owner_id2));
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id1));

  // The borrower wraps the object ID again.
  auto borrower_id = ObjectID::FromRandom();
  borrower1->PutWrappedId(borrower_id, owner_id2);
  borrower_rc1.RemoveLocalReference(owner_id2, nullptr);

  // Borrower 1 submits a task that depends on the wrapped object. The task
  // will be given a reference to owner_id2.
  borrower1->SubmitTaskWithArg(borrower_id);
  borrower_rc1.RemoveLocalReference(borrower_id, nullptr);
  borrower2->ExecuteTaskWithArg(borrower_id, owner_id2, owner->task_id_, owner->address_);

  // The nested task returns while still using owner_id1.
  borrower2->GetSerializedObjectId(owner_id2, owner_id1, owner->task_id_,
                                   owner->address_);
  borrower_rc2.RemoveLocalReference(owner_id2, nullptr);
  auto borrower_refs = borrower2->FinishExecutingTask(borrower_id, ObjectID::Nil());
  ASSERT_TRUE(borrower_rc2.HasReference(owner_id1));
  ASSERT_FALSE(borrower_rc2.HasReference(owner_id2));

  // Borrower 1 should now know that borrower 2 is borrowing the inner object
  // ID.
  borrower1->HandleSubmittedTaskFinished(borrower_id, borrower2->address_, borrower_refs);
  ASSERT_TRUE(borrower_rc1.HasReference(owner_id1));

  // Borrower 1 finishes. It should not have any references now because all
  // state has been merged into the owner.
  borrower_refs = borrower1->FinishExecutingTask(owner_id3, ObjectID::Nil());
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id1));
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id2));
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id3));
  ASSERT_FALSE(borrower_rc1.HasReference(borrower_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(owner_id3, borrower1->address_, borrower_refs);
  // Check that owner now has borrower2 in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(owner_id1));
  ASSERT_FALSE(owner_rc.HasReference(owner_id2));
  ASSERT_FALSE(owner_rc.HasReference(owner_id3));

  // The borrower receives the owner's wait message.
  borrower2->FlushBorrowerCallbacks();
  ASSERT_TRUE(owner_rc.HasReference(owner_id1));
  borrower_rc2.RemoveLocalReference(owner_id1, nullptr);
  ASSERT_FALSE(borrower_rc2.HasReference(owner_id1));
  ASSERT_FALSE(owner_rc.HasReference(owner_id1));
}

// A borrower is given a reference to an object ID, whose value contains
// another object ID. The borrower passes the reference again to another
// borrower but does not wait for it to finish. The nested borrower unwraps the
// outer object and gets a reference to the innermost ID.
//
// @ray.remote
// def borrower2(owner_id2):
//     owner_id1 = ray.get(owner_id2[0])[0]
//     foo.remote(owner_id1)
//
// @ray.remote
// def borrower1(owner_id2):
//     borrower2.remote(owner_id2)
//
// owner_id1 = ray.put(1)
// owner_id2 = ray.put([owner_id1])
// owner_id3 = ray.put([owner_id2])
// res = borrower1.remote(owner_id3)
TEST(DistributedReferenceCountTest, TestNestedObjectDifferentOwners2) {
  ReferenceCounter borrower_rc1;
  auto borrower1 = std::make_shared<MockWorkerClient>(borrower_rc1, "1");
  ReferenceCounter borrower_rc2;
  auto borrower2 = std::make_shared<MockWorkerClient>(borrower_rc2, "2");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) {
    if (addr.ip_address() == borrower1->address_.ip_address()) {
      return borrower1;
    } else {
      return borrower2;
    }
  });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "3");

  // The owner creates an inner object and wraps it.
  auto owner_id1 = ObjectID::FromRandom();
  auto owner_id2 = ObjectID::FromRandom();
  auto owner_id3 = ObjectID::FromRandom();
  owner->Put(owner_id1);
  owner->PutWrappedId(owner_id2, owner_id1);
  owner->PutWrappedId(owner_id3, owner_id2);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to owner_id2.
  owner->SubmitTaskWithArg(owner_id3);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(owner_id1, nullptr);
  owner_rc.RemoveLocalReference(owner_id2, nullptr);
  owner_rc.RemoveLocalReference(owner_id3, nullptr);

  // The borrower is given a reference to the middle object.
  borrower1->ExecuteTaskWithArg(owner_id3, owner_id2, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc1.HasReference(owner_id2));
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id1));

  // The borrower wraps the object ID again.
  auto borrower_id = ObjectID::FromRandom();
  borrower1->PutWrappedId(borrower_id, owner_id2);
  borrower_rc1.RemoveLocalReference(owner_id2, nullptr);

  // Borrower 1 submits a task that depends on the wrapped object. The task
  // will be given a reference to owner_id2.
  borrower1->SubmitTaskWithArg(borrower_id);
  borrower2->ExecuteTaskWithArg(borrower_id, owner_id2, owner->task_id_, owner->address_);

  // The nested task returns while still using owner_id1.
  borrower2->GetSerializedObjectId(owner_id2, owner_id1, owner->task_id_,
                                   owner->address_);
  borrower_rc2.RemoveLocalReference(owner_id2, nullptr);
  auto borrower_refs = borrower2->FinishExecutingTask(borrower_id, ObjectID::Nil());
  ASSERT_TRUE(borrower_rc2.HasReference(owner_id1));
  ASSERT_FALSE(borrower_rc2.HasReference(owner_id2));

  // Borrower 1 should now know that borrower 2 is borrowing the inner object
  // ID.
  borrower1->HandleSubmittedTaskFinished(borrower_id, borrower2->address_, borrower_refs);
  ASSERT_TRUE(borrower_rc1.HasReference(owner_id1));
  ASSERT_TRUE(borrower_rc1.HasReference(owner_id2));

  // Borrower 1 finishes. It should only have its reference to owner_id2 now.
  borrower_refs = borrower1->FinishExecutingTask(owner_id3, ObjectID::Nil());
  ASSERT_TRUE(borrower_rc1.HasReference(owner_id2));
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id3));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(owner_id3, borrower1->address_, borrower_refs);
  // Check that owner now has borrower2 in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(owner_id1));
  ASSERT_TRUE(owner_rc.HasReference(owner_id2));
  ASSERT_FALSE(owner_rc.HasReference(owner_id3));

  // The borrower receives the owner's wait message.
  borrower2->FlushBorrowerCallbacks();
  ASSERT_TRUE(owner_rc.HasReference(owner_id1));
  borrower_rc2.RemoveLocalReference(owner_id1, nullptr);
  ASSERT_FALSE(borrower_rc2.HasReference(owner_id1));
  ASSERT_TRUE(owner_rc.HasReference(owner_id1));

  // The borrower receives the owner's wait message.
  borrower1->FlushBorrowerCallbacks();
  ASSERT_TRUE(owner_rc.HasReference(owner_id2));
  borrower_rc1.RemoveLocalReference(borrower_id, nullptr);
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id2));
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id1));
  ASSERT_FALSE(owner_rc.HasReference(owner_id2));
}

// A borrower is given a reference to an object ID and passes the reference to
// another task. The nested task executes on the object's owner.
//
// @ray.remote
// def executes_on_owner(inner_ids):
//     inner_id = inner_ids[0]
//
// @ray.remote
// def borrower(inner_ids):
//     outer_id2 = ray.put(inner_ids)
//     executes_on_owner.remote(outer_id2)
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestBorrowerPingPong) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) {
    RAY_CHECK(addr.ip_address() == borrower->address_.ip_address());
    return borrower;
  });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);

  // Borrower 1 is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  // The borrower submits a task that depends on the inner object.
  auto outer_id2 = ObjectID::FromRandom();
  borrower->PutWrappedId(outer_id2, inner_id);
  borrower->SubmitTaskWithArg(outer_id2);
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  borrower_rc.RemoveLocalReference(outer_id2, nullptr);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  ASSERT_TRUE(borrower_rc.HasReference(outer_id2));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  ASSERT_TRUE(borrower_rc.HasReference(outer_id2));
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Check that owner now has a borrower for inner.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));

  // Owner starts executing the submitted task. It is given a second reference
  // to the inner object when it gets outer_id2 as an argument.
  owner->ExecuteTaskWithArg(outer_id2, inner_id, owner->task_id_, owner->address_);
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  // Owner finishes but it is still using inner_id.
  borrower_refs = owner->FinishExecutingTask(outer_id2, ObjectID::Nil());
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  borrower->HandleSubmittedTaskFinished(outer_id2, owner->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Borrower no longer has a reference to any objects.
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc.HasReference(outer_id2));
  // The owner should now have borrower 2 in its count.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

// A borrower is given two references to the same object ID. `task` and `Actor`
// execute on the same process.
//
// @ray.remote
// def task(inner_ids):
//     foo.remote(inner_ids[0])
//
// @ray.remote
// class Actor:
//     def __init__(self, inner_ids):
//         self.inner_id = inner_ids[0]
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = task.remote(outer_id)
// Actor.remote(outer_id)
TEST(DistributedReferenceCountTest, TestDuplicateBorrower) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) { return borrower; });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  // The borrower submits a task that depends on the inner object.
  borrower->SubmitTaskWithArg(inner_id);
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs1 = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  // Check that the borrower's ref count for inner_id > 0 because of the
  // pending task.
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The borrower is given a 2nd reference to the inner object.
  owner->SubmitTaskWithArg(outer_id);
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  auto borrower_refs2 = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());

  // The owner receives the borrower's replies and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs1);
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs2);
  borrower->FlushBorrowerCallbacks();
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));

  // The task submitted by the borrower returns and its second reference goes
  // out of scope. Everyone's ref count should go to 0.
  borrower->HandleSubmittedTaskFinished(inner_id);
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));
  ASSERT_FALSE(owner_rc.HasReference(outer_id));
}

// A borrower is given references to 2 different objects, which each contain a
// reference to an object ID. The borrower unwraps both objects and receives a
// duplicate reference to the inner ID.
TEST(DistributedReferenceCountTest, TestDuplicateNestedObject) {
  ReferenceCounter borrower_rc1;
  auto borrower1 = std::make_shared<MockWorkerClient>(borrower_rc1, "1");
  ReferenceCounter borrower_rc2;
  auto borrower2 = std::make_shared<MockWorkerClient>(borrower_rc2, "2");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) {
    if (addr.ip_address() == borrower1->address_.ip_address()) {
      return borrower1;
    } else {
      return borrower2;
    }
  });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "3");

  // The owner creates an inner object and wraps it.
  auto owner_id1 = ObjectID::FromRandom();
  auto owner_id2 = ObjectID::FromRandom();
  auto owner_id3 = ObjectID::FromRandom();
  owner->Put(owner_id1);
  owner->PutWrappedId(owner_id2, owner_id1);
  owner->PutWrappedId(owner_id3, owner_id2);

  owner->SubmitTaskWithArg(owner_id3);
  owner->SubmitTaskWithArg(owner_id2);
  owner_rc.RemoveLocalReference(owner_id1, nullptr);
  owner_rc.RemoveLocalReference(owner_id2, nullptr);
  owner_rc.RemoveLocalReference(owner_id3, nullptr);

  borrower2->ExecuteTaskWithArg(owner_id3, owner_id2, owner->task_id_, owner->address_);
  borrower2->GetSerializedObjectId(owner_id2, owner_id1, owner->task_id_,
                                   owner->address_);
  borrower_rc2.RemoveLocalReference(owner_id2, nullptr);
  // The nested task returns while still using owner_id1.
  auto borrower_refs = borrower2->FinishExecutingTask(owner_id3, ObjectID::Nil());
  owner->HandleSubmittedTaskFinished(owner_id3, borrower2->address_, borrower_refs);
  ASSERT_TRUE(borrower2->FlushBorrowerCallbacks());

  // The owner submits a task that is given a reference to owner_id1.
  borrower1->ExecuteTaskWithArg(owner_id2, owner_id1, owner->task_id_, owner->address_);
  // The borrower wraps the object ID again.
  auto borrower_id = ObjectID::FromRandom();
  borrower1->PutWrappedId(borrower_id, owner_id1);
  borrower_rc1.RemoveLocalReference(owner_id1, nullptr);
  // Borrower 1 submits a task that depends on the wrapped object. The task
  // will be given a reference to owner_id1.
  borrower1->SubmitTaskWithArg(borrower_id);
  borrower_rc1.RemoveLocalReference(borrower_id, nullptr);
  borrower2->ExecuteTaskWithArg(borrower_id, owner_id1, owner->task_id_, owner->address_);
  // The nested task returns while still using owner_id1.
  // It should now have 2 local references to owner_id1, one from the owner and
  // one from the borrower.
  borrower_refs = borrower2->FinishExecutingTask(borrower_id, ObjectID::Nil());
  borrower1->HandleSubmittedTaskFinished(borrower_id, borrower2->address_, borrower_refs);

  // Borrower 1 finishes. It should not have any references now because all
  // state has been merged into the owner.
  borrower_refs = borrower1->FinishExecutingTask(owner_id2, ObjectID::Nil());
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id1));
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id2));
  ASSERT_FALSE(borrower_rc1.HasReference(owner_id3));
  ASSERT_FALSE(borrower_rc1.HasReference(borrower_id));
  // Borrower 1 should not have merge any refs into the owner because borrower 2's ref was
  // already merged into the owner.
  owner->HandleSubmittedTaskFinished(owner_id2, borrower1->address_, borrower_refs);

  // The borrower receives the owner's wait message.
  borrower2->FlushBorrowerCallbacks();
  ASSERT_TRUE(owner_rc.HasReference(owner_id1));
  borrower_rc2.RemoveLocalReference(owner_id1, nullptr);
  ASSERT_TRUE(owner_rc.HasReference(owner_id1));
  borrower_rc2.RemoveLocalReference(owner_id1, nullptr);
  ASSERT_FALSE(borrower_rc2.HasReference(owner_id1));
  ASSERT_FALSE(owner_rc.HasReference(owner_id1));
}

// We submit a task and immediately delete the reference to the return ID. The
// submitted task returns an object ID.
//
// @ray.remote
// def returns_id():
//     inner_id = ray.put()
//     return inner_id
//
// returns_id.remote()
TEST(DistributedReferenceCountTest, TestReturnObjectIdNoBorrow) {
  ReferenceCounter caller_rc;
  auto caller = std::make_shared<MockWorkerClient>(caller_rc, "1");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) {
    RAY_CHECK(addr.ip_address() == caller->address_.ip_address());
    return caller;
  });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "3");

  // Caller submits a task.
  auto return_id = caller->SubmitTaskWithArg(ObjectID::Nil());

  // Task returns inner_id as its return value.
  auto inner_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  rpc::WorkerAddress addr(caller->address_);
  auto refs = owner->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(refs.empty());
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // Caller's ref to the task's return ID goes out of scope before it hears
  // from the owner of inner_id.
  caller->HandleSubmittedTaskFinished(ObjectID::Nil());
  caller_rc.RemoveLocalReference(return_id, nullptr);
  ASSERT_FALSE(caller_rc.HasReference(return_id));
  ASSERT_FALSE(caller_rc.HasReference(inner_id));

  // Caller should respond to the owner's message immediately.
  ASSERT_TRUE(caller->FlushBorrowerCallbacks());
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

// We submit a task and keep the reference to the return ID. The submitted task
// returns an object ID.
//
// @ray.remote
// def returns_id():
//     inner_id = ray.put()
//     return inner_id
//
// return_id = returns_id.remote()
TEST(DistributedReferenceCountTest, TestReturnObjectIdBorrow) {
  ReferenceCounter caller_rc;
  auto caller = std::make_shared<MockWorkerClient>(caller_rc, "1");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) {
    RAY_CHECK(addr.ip_address() == caller->address_.ip_address());
    return caller;
  });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "3");

  // Caller submits a task.
  auto return_id = caller->SubmitTaskWithArg(ObjectID::Nil());

  // Task returns inner_id as its return value.
  auto inner_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  rpc::WorkerAddress addr(caller->address_);
  auto refs = owner->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(refs.empty());
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // Caller receives the owner's message, but inner_id is still in scope
  // because caller has a reference to return_id.
  caller->HandleSubmittedTaskFinished(ObjectID::Nil());
  ASSERT_TRUE(caller->FlushBorrowerCallbacks());
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // Caller's reference to return_id goes out of scope. The caller should
  // respond to the owner of inner_id so that inner_id can be deleted.
  caller_rc.RemoveLocalReference(return_id, nullptr);
  ASSERT_FALSE(caller_rc.HasReference(return_id));
  ASSERT_FALSE(caller_rc.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

// We submit a task and submit another task that depends on the return ID. The
// submitted task returns an object ID, which will get borrowed by the second
// task.
//
// @ray.remote
// def returns_id():
//     inner_id = ray.put()
//     return inner_id
//
// return_id = returns_id.remote()
// borrow.remote(return_id)
TEST(DistributedReferenceCountTest, TestReturnObjectIdBorrowChain) {
  ReferenceCounter caller_rc;
  auto caller = std::make_shared<MockWorkerClient>(caller_rc, "1");
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "2");
  ReferenceCounter owner_rc(true, [&](const rpc::Address &addr) {
    if (addr.ip_address() == caller->address_.ip_address()) {
      return caller;
    } else {
      return borrower;
    }
  });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "3");

  // Caller submits a task.
  auto return_id = caller->SubmitTaskWithArg(ObjectID::Nil());

  // Task returns inner_id as its return value.
  auto inner_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  rpc::WorkerAddress addr(caller->address_);
  auto refs = owner->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(refs.empty());
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // Caller receives the owner's message, but inner_id is still in scope
  // because caller has a reference to return_id.
  caller->HandleSubmittedTaskFinished(ObjectID::Nil());
  caller->SubmitTaskWithArg(return_id);
  caller_rc.RemoveLocalReference(return_id, nullptr);
  ASSERT_TRUE(caller->FlushBorrowerCallbacks());
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // Borrower receives a reference to inner_id. It still has a reference when
  // the task returns.
  borrower->ExecuteTaskWithArg(return_id, inner_id, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  auto borrower_refs = borrower->FinishExecutingTask(return_id, return_id);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // Borrower merges ref count into the caller.
  caller->HandleSubmittedTaskFinished(return_id, borrower->address_, borrower_refs);
  // The caller should not have a ref count anymore because it was merged into
  // the owner.
  ASSERT_FALSE(caller_rc.HasReference(return_id));
  ASSERT_FALSE(caller_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower's receives the owner's message and its reference goes out of
  // scope.
  ASSERT_TRUE(borrower->FlushBorrowerCallbacks());
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower_rc.HasReference(return_id));
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

// TODO: Test returning an Object ID.
// TODO: Test Pop and Merge individually.

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
