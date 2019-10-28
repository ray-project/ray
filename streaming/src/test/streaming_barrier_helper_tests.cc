#include "gtest/gtest.h"
#include "streaming_barrier_helper.h"

using namespace ray::streaming;
using namespace ray;

class StreamingBarrierHelperTest : public ::testing::Test {
 public:
  void SetUp() { barrier_helper_.reset(new StreamingBarrierHelper()); }
  void TearDown() { barrier_helper_.release(); }

 protected:
  std::unique_ptr<StreamingBarrierHelper> barrier_helper_;
  const plasma::ObjectID random_id = ray::ObjectID::FromRandom().ToPlasmaId();
  const plasma::ObjectID another_random_id = ray::ObjectID::FromRandom().ToPlasmaId();
};

TEST_F(StreamingBarrierHelperTest, SeqIdByBarrierId) {
  ASSERT_EQ(barrier_helper_->GetBarrierMapSize(), 0);
  uint64_t seq_id = 0;
  uint64_t init_seq_id = 10;
  ASSERT_EQ(StreamingStatus::NoSuchItem,
            barrier_helper_->GetSeqIdByBarrierId(random_id, 1, seq_id));

  barrier_helper_->SetSeqIdByBarrierId(random_id, 1, init_seq_id);

  ASSERT_EQ(StreamingStatus::QueueIdNotFound,
            barrier_helper_->GetSeqIdByBarrierId(another_random_id, 1, seq_id));

  ASSERT_EQ(StreamingStatus::OK,
            barrier_helper_->GetSeqIdByBarrierId(random_id, 1, seq_id));
  ASSERT_EQ(init_seq_id, seq_id);

  barrier_helper_->SetSeqIdByBarrierId(random_id, 2, init_seq_id + 1);

  ASSERT_EQ(StreamingStatus::OK,
            barrier_helper_->GetSeqIdByBarrierId(random_id, 2, seq_id));
  ASSERT_EQ(init_seq_id + 1, seq_id);

  ASSERT_EQ(barrier_helper_->GetBarrierMapSize(), 2);
  barrier_helper_->ReleaseBarrierMapSeqIdById(1);
  ASSERT_EQ(barrier_helper_->GetBarrierMapSize(), 1);
  barrier_helper_->ReleaseAllBarrierMapSeqId();
  ASSERT_EQ(barrier_helper_->GetBarrierMapSize(), 0);
}

TEST_F(StreamingBarrierHelperTest, BarrierIdByLastMessageId) {
  uint64_t barrier_id = 0;
  ASSERT_EQ(StreamingStatus::NoSuchItem,
            barrier_helper_->GetBarrierIdByLastMessageId(random_id, 1, barrier_id));

  barrier_helper_->SetBarrierIdByLastMessageId(random_id, 1, 10);

  ASSERT_EQ(
      StreamingStatus::QueueIdNotFound,
      barrier_helper_->GetBarrierIdByLastMessageId(another_random_id, 1, barrier_id));

  ASSERT_EQ(StreamingStatus::OK,
            barrier_helper_->GetBarrierIdByLastMessageId(random_id, 1, barrier_id));
  ASSERT_EQ(barrier_id, 10);

  barrier_helper_->SetBarrierIdByLastMessageId(random_id, 1, 11);
  ASSERT_EQ(StreamingStatus::OK,
            barrier_helper_->GetBarrierIdByLastMessageId(random_id, 1, barrier_id, true));
  ASSERT_EQ(barrier_id, 10);
  ASSERT_EQ(StreamingStatus::OK,
            barrier_helper_->GetBarrierIdByLastMessageId(random_id, 1, barrier_id, true));
  ASSERT_EQ(barrier_id, 11);
  ASSERT_EQ(StreamingStatus::NoSuchItem,
            barrier_helper_->GetBarrierIdByLastMessageId(random_id, 1, barrier_id, true));
}

TEST_F(StreamingBarrierHelperTest, CheckpointId) {
  uint64_t checkpoint_id = static_cast<uint64_t>(-1);
  barrier_helper_->GetCurrentMaxCheckpointIdInQueue(random_id, checkpoint_id);
  ASSERT_EQ(checkpoint_id, 0);
  barrier_helper_->SetCurrentMaxCheckpointIdInQueue(random_id, 2);
  barrier_helper_->GetCurrentMaxCheckpointIdInQueue(random_id, checkpoint_id);
  ASSERT_EQ(checkpoint_id, 2);
  barrier_helper_->SetCurrentMaxCheckpointIdInQueue(random_id, 3);
  barrier_helper_->GetCurrentMaxCheckpointIdInQueue(random_id, checkpoint_id);
  ASSERT_EQ(checkpoint_id, 3);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
