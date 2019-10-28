#include <unistd.h>
#include "gtest/gtest.h"

#include "streaming.h"
#include "streaming_message.h"
#include "streaming_message_bundle.h"
#include "streaming_reader.h"
#include "streaming_ring_buffer.h"
#include "streaming_writer.h"
#include "test/test_utils.h"

using namespace ray;
using namespace ray::streaming;

const std::string STREAMING_STORE = "/tmp/store_streaming_tests";

void PingMessage(std::shared_ptr<StreamingWriter> writer,
                 std::shared_ptr<StreamingReader> reader,
                 std::vector<ray::ObjectID> queue_id_vec, uint8_t *data,
                 size_t data_size) {
  for (auto &q_id : queue_id_vec) {
    auto buffer = util::ToMessageBuffer(writer.get(), q_id, data, data_size);
    writer->WriteMessageToBufferRing(q_id, buffer.Data(), data_size,
                                     StreamingMessageType::Message);
  }
  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    std::shared_ptr<StreamingReaderBundle> msg;
    reader->GetBundle(1000, msg);
    STREAMING_CHECK(msg->data);
    StreamingMessageBundlePtr bundlePtr;
    bundlePtr = StreamingMessageBundle::FromBytes(msg->data);
    std::list<StreamingMessagePtr> message_list;
    bundlePtr->GetMessageList(message_list);
    EXPECT_EQ(std::memcmp(data, message_list.back()->RawData(), data_size), 0);
  }
}

void PingPartialBarrier(std::shared_ptr<StreamingWriter> writer,
                        std::shared_ptr<StreamingReader> reader, uint8_t *data,
                        size_t data_size, uint64_t global_barrier_id,
                        uint64_t barrier_id) {
  writer->BroadcastPartialBarrier(global_barrier_id, barrier_id, data, data_size);
  std::shared_ptr<StreamingReaderBundle> msg;
  reader->GetBundle(1000, msg);
  STREAMING_CHECK(msg->data);
  StreamingMessageBundlePtr partial_barrier_bundle =
      StreamingMessageBundle::FromBytes(msg->data);
  std::list<StreamingMessagePtr> barrier_message_list;
  partial_barrier_bundle->GetMessageList(barrier_message_list);
  STREAMING_LOG(INFO) << "partial barrier bundle => "
                      << partial_barrier_bundle->ToString() << ", from qid => "
                      << msg->from << ", barrier id => " << msg->last_barrier_id
                      << ", partial barrier id =>" << msg->last_partial_barrier_id;
  EXPECT_EQ(partial_barrier_bundle->IsBarrier(), true);
  EXPECT_EQ(barrier_message_list.back()->IsBarrier(), true);
  StreamingBarrierHeader barrier_header;
  StreamingMessage::GetBarrierIdFromRawData(barrier_message_list.back()->RawData(),
                                            &barrier_header);
  EXPECT_EQ(barrier_header.partial_barrier_id, barrier_id);
  EXPECT_EQ(barrier_header.IsPartialBarrier(), true);
  EXPECT_EQ(std::memcmp(data, barrier_message_list.back()->RawData() + kBarrierHeaderSize,
                        data_size),
            0);
}

void PingGlobalBarrier(std::shared_ptr<StreamingWriter> writer,
                       std::shared_ptr<StreamingReader> reader, uint8_t *data,
                       size_t data_size, uint64_t barrier_id) {
  writer->BroadcastBarrier(barrier_id, barrier_id, data, data_size);
  std::shared_ptr<StreamingReaderBundle> msg;
  reader->GetBundle(1000, msg);
  STREAMING_CHECK(msg->data);
  StreamingMessageBundlePtr partial_barrier_bundle =
      StreamingMessageBundle::FromBytes(msg->data);
  std::list<StreamingMessagePtr> barrier_message_list;
  partial_barrier_bundle->GetMessageList(barrier_message_list);
  STREAMING_LOG(INFO) << "global barrier bundle => " << partial_barrier_bundle->ToString()
                      << ", from qid => " << msg->from << ", global barrier id => "
                      << msg->last_barrier_id << ", partial barrier id =>"
                      << msg->last_partial_barrier_id;
  EXPECT_EQ(partial_barrier_bundle->IsBarrier(), true);
  EXPECT_EQ(barrier_message_list.back()->IsBarrier(), true);
  StreamingBarrierHeader barrier_header;
  StreamingMessage::GetBarrierIdFromRawData(barrier_message_list.back()->RawData(),
                                            &barrier_header);
  EXPECT_EQ(barrier_header.barrier_id, barrier_id);
  EXPECT_EQ(barrier_header.IsGlobalBarrier(), true);
  EXPECT_EQ(std::memcmp(data, barrier_message_list.back()->RawData() + kBarrierHeaderSize,
                        data_size),
            0);
}

void streaming_strategy_test(StreamingConfig &config, uint32_t queue_num) {
  std::vector<ray::ObjectID> queue_id_vec;
  for (uint32_t i = 0; i < queue_num; ++i) {
    ObjectID queue_id = ray::ObjectID::FromRandom();
    ConvertToValidQueueId(queue_id);
    queue_id_vec.emplace_back(queue_id);
  }

  std::string plasma_store_path = STREAMING_STORE;
  std::vector<uint64_t> channel_seq_id_vec(queue_num, 0);

  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    STREAMING_LOG(INFO) << " qid hex => " << queue_id_vec[i].Hex() << " store path => "
                        << plasma_store_path;
  }
  std::shared_ptr<StreamingWriter> writer(new StreamingWriter());
  uint64_t queue_size = 10 * 1000 * 1000;
  std::vector<ray::ObjectID> remain_id_vec;
  writer->SetConfig(config);
  writer->Init(queue_id_vec, plasma_store_path, channel_seq_id_vec,
               std::vector<uint64_t>(queue_id_vec.size(), queue_size), remain_id_vec);
  STREAMING_CHECK(remain_id_vec.empty()) << remain_id_vec.size();

  writer->Run();
  std::shared_ptr<StreamingReader> reader(new StreamingReader());
  reader->SetConfig(config);
  reader->Init(plasma_store_path, queue_id_vec, -1);

  size_t test_len = 8;
  uint8_t test_data1[8] = {0x01, 0x02, 0x04, 0x05, 0x07, 0x03, 0x08, 0x06};
  PingMessage(writer, reader, queue_id_vec, test_data1, test_len);
  PingGlobalBarrier(writer, reader, test_data1, test_len, 1);

  std::vector<ray::ObjectID> scaleup_vec(queue_id_vec);
  ObjectID queue_id = ray::ObjectID::FromRandom();
  ConvertToValidQueueId(queue_id);
  scaleup_vec.emplace_back(queue_id);

  STREAMING_LOG(INFO) << "streaming scaleup";
  writer->Rescale(scaleup_vec);
  reader->Rescale(scaleup_vec);
  PingPartialBarrier(writer, reader, test_data1, test_len, 1, 1);
  writer->ClearPartialCheckpoint(1, 1);
  reader->ClearPartialCheckpoint(1, 1);

  uint8_t test_data2[8] = {0x02, 0x01, 0x04, 0x05, 0x07, 0x03, 0x08, 0x06};
  PingMessage(writer, reader, scaleup_vec, test_data2, test_len);

  STREAMING_LOG(INFO) << "streaming scaledown";

  std::vector<ray::ObjectID> scaledown_vec(scaleup_vec);
  scaledown_vec.erase(scaledown_vec.begin());
  writer->Rescale(scaledown_vec);
  reader->Rescale(scaledown_vec);
  PingPartialBarrier(writer, reader, test_data2, test_len, 1, 2);
  writer->ClearPartialCheckpoint(1, 2);
  reader->ClearPartialCheckpoint(1, 2);

  uint8_t test_data3[8] = {0x02, 0x01, 0x04, 0x05, 0x07, 0x08, 0x09, 0x06};
  PingMessage(writer, reader, scaledown_vec, test_data3, test_len);
  PingGlobalBarrier(writer, reader, test_data3, test_len, 2);
  writer->ClearCheckpoint(2);
  PingMessage(writer, reader, scaledown_vec, test_data3, test_len);
}

TEST(StreamingRescaleTest, streaming_rescale_exactly_once_test) {
  StreamingConfig config;
  config.SetStreaming_empty_message_time_interval(100);
  uint32_t queue_num = 3;
  STREAMING_LOG(INFO) << "Streaming Rescale Strategy => EXACTLY ONCE";
  config.SetStreaming_strategy_(StreamingStrategy::EXACTLY_ONCE);
  streaming_strategy_test(config, queue_num);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
