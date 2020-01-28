#include "ray/core_worker/reference_count.h"

#include <vector>

#include "gtest/gtest.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"

namespace ray {

static const rpc::Address empty_borrower;
static const ReferenceCounter::ReferenceTable empty_refs;

class ReferenceCountTest : public ::testing::Test {
 protected:
  std::unique_ptr<ReferenceCounter> rc;
  virtual void SetUp() { rc = std::unique_ptr<ReferenceCounter>(new ReferenceCounter); }

  virtual void TearDown() {}
};

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  MockWorkerClient(ReferenceCounter &client) : client_(client) {}

  // TODO: Make message receive and reply async.
  ray::Status WaitForRefRemoved(const rpc::WaitForRefRemovedRequest &request,
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
    client_.HandleWaitForRefRemoved(ObjectID::FromBinary(request.object_id()), absl::optional<ObjectID>(), requests_[r].first.get(), send_reply_callback);

    num_requests_++;
    return Status::OK();
  }

  // The ReferenceCounter at the "client".
  ReferenceCounter &client_;
  std::unordered_map<int, std::pair<std::shared_ptr<rpc::WaitForRefRemovedReply>, rpc::ClientCallback<rpc::WaitForRefRemovedReply>>> requests_;
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
  rc->RemoveSubmittedTaskReferences({id1}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveSubmittedTaskReferences({id2}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 1);
  rc->RemoveSubmittedTaskReferences({id1}, empty_borrower, empty_refs, &out);
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
  rc->RemoveSubmittedTaskReferences({id2}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveSubmittedTaskReferences({id1}, empty_borrower, empty_refs, &out);
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
  rc->AddOwnedObject(object_id, task_id, address);

  TaskID added_id;
  rpc::Address added_address;
  ASSERT_TRUE(rc->GetOwner(object_id, &added_id, &added_address));
  ASSERT_EQ(task_id, added_id);
  ASSERT_EQ(address.ip_address(), added_address.ip_address());

  auto object_id2 = ObjectID::FromRandom();
  task_id = TaskID::ForFakeTask();
  address.set_ip_address("5678");
  rc->AddOwnedObject(object_id2, task_id, address);
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
  RayObject buffer(std::make_shared<LocalMemoryBuffer>(data, sizeof(data)), nullptr);

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

TEST(DistributedReferenceCountTest, TestSimpleBorrower) {
  ReferenceCounter borrower_rc;
  auto borrower_client = std::make_shared<MockWorkerClient>(borrower_rc);
  ReferenceCounter owner_rc([&](const std::string &addr, int port) { return borrower_client;});

  auto inner_id = ObjectID::FromRandom();
  TaskID owner_id = TaskID::ForFakeTask();
  rpc::Address owner_address;
  owner_address.set_ip_address("1234");
  owner_rc.AddOwnedObject(inner_id, owner_id, owner_address);
  owner_rc.AddLocalReference(inner_id);

  auto outer_id = ObjectID::FromRandom();
  owner_rc.AddOwnedObject(outer_id, owner_id, owner_address);
  owner_rc.WrapObjectId(outer_id, {inner_id});
  owner_rc.AddSubmittedTaskReferences({outer_id});
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(owner_rc.GetReference(outer_id).RefCount() > 0);
  // Check that the owner's ref count > 0.
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() == 0);

  borrower_rc.AddLocalReference(inner_id);
  borrower_rc.AddBorrowedObject(outer_id, inner_id, owner_id, owner_address);
  borrower_rc.AddSubmittedTaskReferences({inner_id});
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  // Check that the borrower's ref count > 0.
  ASSERT_TRUE(borrower_rc.GetReference(inner_id).RefCount() > 0);
  ASSERT_TRUE(borrower_rc.GetReference(outer_id).RefCount() == 0);

  rpc::Address borrower_address;
  auto borrower_id = WorkerID::FromRandom();
  borrower_address.set_ip_address("5678");
  borrower_address.set_worker_id(borrower_id.Binary());
  auto borrower_refs = borrower_rc.PopBorrowerRefs(outer_id);
  // Check that the borrower's ref count for inner_id > 0.
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  ASSERT_TRUE(borrower_rc.GetReference(inner_id).RefCount() > 0);
  // Check that the borrower's ref count for outer == 0.
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));

  owner_rc.RemoveSubmittedTaskReferences({outer_id}, borrower_address, borrower_refs, nullptr);
  // Check that owner now has borrower in inner's borrowers list.
  // Check that owner's ref count for outer == 0.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() == 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() > 0);
  ASSERT_FALSE(owner_rc.HasReference(outer_id));

  std::vector<ObjectID> deleted;
  borrower_rc.RemoveSubmittedTaskReferences({inner_id}, empty_borrower, empty_refs, nullptr);
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  // Check that the owner's ref count for inner == 0 after flushing the
  // WaitForRefRemoved reply.
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}


}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
