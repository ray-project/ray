#include <cstring>
#include <string>
#include "gtest/gtest.h"

#include "queue/message.h"
using namespace ray;
using namespace ray::streaming;

TEST(ProtoBufTest, MessageCommonTest) {
  JobID job_id = JobID::FromInt(0);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  ray::ActorID actor_id = ray::ActorID::Of(job_id, task_id, 0);
  ray::ActorID peer_actor_id = ray::ActorID::Of(job_id, task_id, 1);
  ObjectID queue_id = ray::ObjectID::FromRandom();

  uint8_t data[128];
  std::shared_ptr<LocalMemoryBuffer> buffer =
      std::make_shared<LocalMemoryBuffer>(data, 128, true);
  DataMessage msg(actor_id, peer_actor_id, queue_id, 100, 1000, 2000, buffer, true);
  std::unique_ptr<LocalMemoryBuffer> serilized_buffer = msg.ToBytes();
  std::shared_ptr<DataMessage> msg2 = DataMessage::FromBytes(serilized_buffer->Data());
  EXPECT_EQ(msg.ActorId(), msg2->ActorId());
  EXPECT_EQ(msg.PeerActorId(), msg2->PeerActorId());
  EXPECT_EQ(msg.QueueId(), msg2->QueueId());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
