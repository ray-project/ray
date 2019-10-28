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

const uint32_t MESSAGE_BOUND_SIZE = 10000;
const uint32_t DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE = 1000;
const std::string STREAMING_STORE = "/tmp/store_streaming_tests";
const uint32_t MESSAGE_BARRIER_INTERVAL = 1000;

void TestWriteMessageToBufferRing(std::shared_ptr<StreamingWriter> writer_client,
                                  std::vector<ray::ObjectID> &q_list) {
  const uint8_t temp_data[] = {1, 2, 4, 5};

  uint32_t i = 1;
  while (i <= MESSAGE_BOUND_SIZE) {
    for (auto &q_id : q_list) {
      uint64_t buffer_len =
          std::max<uint64_t>(1, (i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE));
      auto *data = new uint8_t[buffer_len];
      for (uint32_t j = 0; j < buffer_len; ++j) {
        data[j] = j % 128;
      }
      auto buffer = util::ToMessageBuffer(writer_client.get(), q_id, data, buffer_len);
      delete[] data;
      writer_client->WriteMessageToBufferRing(q_id, buffer.Data(), buffer.Size(),
                                              StreamingMessageType::Message);
    }
    if (i % MESSAGE_BARRIER_INTERVAL == 0) {
      STREAMING_LOG(DEBUG) << "broadcast barrier, barrier id => " << i;
      writer_client->BroadcastBarrier(i / MESSAGE_BARRIER_INTERVAL,
                                      i / MESSAGE_BARRIER_INTERVAL, temp_data, 4);
    }
    ++i;
  }
}

void ReaderLoopForward(std::shared_ptr<StreamingReader> reader_client,
                       std::shared_ptr<StreamingWriter> writer_client,
                       std::vector<ray::ObjectID> &queue_id_vec) {
  uint64_t recevied_message_cnt = 0;
  std::unordered_map<ray::ObjectID, uint64_t> queue_last_cp_id;

  for (auto &q_id : queue_id_vec) {
    queue_last_cp_id[q_id] = 0;
  }
  STREAMING_LOG(INFO) << "Start read message bundle";
  while (true) {
    std::shared_ptr<StreamingReaderBundle> msg;
    reader_client->GetBundle(100, msg);

    if (!msg->data) {
      STREAMING_LOG(DEBUG) << "read bundle timeout";
      continue;
    }

    STREAMING_CHECK(msg.get() && msg->meta.get())
        << "read null pointer message, queue id => " << msg->from.Hex();

    if (msg->meta->GetBundleType() == StreamingMessageBundleType::Barrier) {
      STREAMING_LOG(DEBUG) << "barrier message recevied => "
                           << msg->meta->GetMessageBundleTs();
      std::unordered_map<ray::ObjectID, ConsumerChannelInfo> *offset_map;
      reader_client->GetOffsetInfo(offset_map);

      for (auto &q_id : queue_id_vec) {
        reader_client->NotifyConsumedItem((*offset_map)[q_id],
                                          (*offset_map)[q_id].current_seq_id);
      }
      writer_client->ClearCheckpoint(msg->last_barrier_id);

      continue;
    } else if (msg->meta->GetBundleType() == StreamingMessageBundleType::Empty) {
      STREAMING_LOG(DEBUG) << "empty message recevied => "
                           << msg->meta->GetMessageBundleTs();
      continue;
    }

    StreamingMessageBundlePtr bundlePtr;
    bundlePtr = StreamingMessageBundle::FromBytes(msg->data);
    std::list<StreamingMessagePtr> message_list;
    bundlePtr->GetMessageList(message_list);
    STREAMING_LOG(DEBUG) << "message size => " << message_list.size()
                         << " from queue id => " << msg->from.Hex()
                         << " last message id => " << msg->meta->GetLastMessageId()
                         << " last barrier id => " << msg->last_barrier_id;
    if (reader_client->GetConfig().GetStreaming_strategy_() ==
        StreamingStrategy::EXACTLY_ONCE) {
      // check barrier for excatly once
      std::unordered_set<uint64_t> cp_id_set;
      cp_id_set.insert(msg->last_barrier_id);

      for (auto &q_id : queue_id_vec) {
        cp_id_set.insert(queue_last_cp_id[q_id]);
        STREAMING_LOG(DEBUG) << "q id " << q_id.Hex() << ", cp id=>"
                             << queue_last_cp_id[q_id];
      }
      STREAMING_LOG(DEBUG) << "cp set size =>" << cp_id_set.size();
      STREAMING_CHECK(cp_id_set.size() <= 2) << cp_id_set.size();

      queue_last_cp_id[msg->from] = msg->last_barrier_id;
    }

    recevied_message_cnt += message_list.size();
    for (auto &item : message_list) {
      uint64_t i = item->GetMessageSeqId();

      uint32_t buff_len =
          std::max<uint32_t>(1, i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE);
      if (i > MESSAGE_BOUND_SIZE) break;

      EXPECT_EQ(buff_len, item->GetDataSize());
      uint8_t *compared_data = new uint8_t[buff_len];
      for (uint32_t j = 0; j < item->GetDataSize(); ++j) {
        compared_data[j] = j % 128;
      }
      EXPECT_EQ(std::memcmp(compared_data, item->RawData(), item->GetDataSize()), 0);
      delete[] compared_data;
    }
    STREAMING_LOG(DEBUG) << "Received message count => " << recevied_message_cnt;
    if (recevied_message_cnt == queue_id_vec.size() * MESSAGE_BOUND_SIZE) {
      STREAMING_LOG(INFO) << "recevied message count => " << recevied_message_cnt
                          << ", break";
      break;
    }
  }
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

  STREAMING_LOG(INFO) << "start store first";
  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    STREAMING_LOG(INFO) << " qid hex => " << queue_id_vec[i].Hex() << " store path => "
                        << plasma_store_path;
  }
  STREAMING_LOG(INFO) << "Sub process: writer.";
  std::shared_ptr<StreamingWriter> streaming_writer_client(new StreamingWriter());
  uint64_t queue_size = 10 * 1000 * 1000;
  std::vector<ray::ObjectID> remain_id_vec;
  streaming_writer_client->SetConfig(config);
  streaming_writer_client->Init(queue_id_vec, plasma_store_path, channel_seq_id_vec,
                                std::vector<uint64_t>(queue_id_vec.size(), queue_size),
                                remain_id_vec);
  STREAMING_CHECK(remain_id_vec.empty()) << remain_id_vec.size();

  streaming_writer_client->Run();
  std::thread test_loop_thread(&TestWriteMessageToBufferRing, streaming_writer_client,
                               std::ref(queue_id_vec));
  test_loop_thread.detach();

  STREAMING_LOG(INFO) << "Main process: Reader.";
  std::shared_ptr<StreamingReader> reader(new StreamingReader());
  reader->Init(plasma_store_path, queue_id_vec, -1);
  reader->SetConfig(config);
  ReaderLoopForward(reader, streaming_writer_client, queue_id_vec);

  STREAMING_LOG(INFO) << "Reader exit";

  if (test_loop_thread.joinable()) {
    test_loop_thread.join();
  }
  STREAMING_LOG(INFO) << "Writer exit";
}

TEST(StreamingWriterTest, streaming_writer_exactly_once_test) {
  StreamingConfig config;
  config.SetStreaming_empty_message_time_interval(50);

  uint32_t queue_num = 5;

  STREAMING_LOG(INFO) << "Streaming Strategy => EXACTLY ONCE";
  config.SetStreaming_strategy_(StreamingStrategy::EXACTLY_ONCE);
  streaming_strategy_test(config, queue_num);
}

TEST(StreamingWriterTest, streaming_writer_at_least_once_test) {
  StreamingConfig config;
  config.SetStreaming_empty_message_time_interval(50);

  uint32_t queue_num = 5;

  STREAMING_LOG(INFO) << "Streaming Strategy => AT_LEAST_ONCE";
  config.SetStreaming_strategy_(StreamingStrategy::AT_LEAST_ONCE);
  streaming_strategy_test(config, queue_num);
}

TEST(StreamingWriterTest, streaming_recreate_test) {
  std::vector<ray::ObjectID> queue_id_vec;

  uint32_t queue_num = 1;
  for (uint32_t i = 0; i < queue_num; ++i) {
    ObjectID queue_id = ray::ObjectID::FromRandom();
    ConvertToValidQueueId(queue_id);
    queue_id_vec.emplace_back(queue_id);
  }

  std::string plasma_store_path = STREAMING_STORE;
  std::vector<uint64_t> channel_seq_id_vec(queue_num, 0);
  std::shared_ptr<StreamingWriter> streaming_writer_client(new StreamingWriter());
  uint64_t queue_size = 10 * 1000 * 1000;
  std::vector<ray::ObjectID> remain_id_vec;
  streaming_writer_client->Init(queue_id_vec, plasma_store_path, channel_seq_id_vec,
                                std::vector<uint64_t>(queue_id_vec.size(), queue_size),
                                remain_id_vec);

  STREAMING_CHECK(remain_id_vec.empty()) << remain_id_vec.size();

  streaming_writer_client->Run();

  uint8_t first_data[] = {1, 2, 3};
  auto buffer = util::ToMessageBuffer(streaming_writer_client.get(), queue_id_vec[0],
                                      first_data, 3);
  streaming_writer_client->WriteMessageToBufferRing(queue_id_vec[0], buffer.Data(),
                                                    buffer.Size());

  std::shared_ptr<StreamingReader> reader(new StreamingReader());
  reader->Init(plasma_store_path, queue_id_vec, -1);
  std::shared_ptr<StreamingReaderBundle> message =
      std::make_shared<StreamingReaderBundle>();
  reader->GetBundle(-1, message);
  StreamingMessageBundlePtr msg_ptr = StreamingMessageBundle::FromBytes(message->data);
  std::list<StreamingMessagePtr> message_list;
  msg_ptr->GetMessageList(message_list);
  EXPECT_EQ(message_list.size(), 1);
  EXPECT_EQ(std::memcmp(message_list.front()->RawData(), first_data, 3), 0);

  streaming_writer_client.reset(new StreamingWriter());
  streaming_writer_client->Init(
      queue_id_vec, plasma_store_path, channel_seq_id_vec,
      std::vector<uint64_t>(queue_id_vec.size(), queue_size), remain_id_vec,
      std::vector<StreamingQueueCreationType>(
          queue_id_vec.size(), StreamingQueueCreationType::RECREATE_AND_CLEAR));

  STREAMING_CHECK(remain_id_vec.empty()) << remain_id_vec.size();
  streaming_writer_client->Run();

  uint8_t second_data[] = {4, 5, 6};
  buffer = util::ToMessageBuffer(streaming_writer_client.get(), queue_id_vec[0],
                                 second_data, 3);
  streaming_writer_client->WriteMessageToBufferRing(queue_id_vec[0], buffer.Data(),
                                                    buffer.Size());

  reader = std::make_shared<StreamingReader>();
  reader->Init(plasma_store_path, queue_id_vec, -1);
  std::shared_ptr<StreamingReaderBundle> message2 =
      std::make_shared<StreamingReaderBundle>();
  reader->GetBundle(-1, message2);
  StreamingMessageBundlePtr msg_ptr2 = StreamingMessageBundle::FromBytes(message2->data);
  std::list<StreamingMessagePtr> message_list2;
  msg_ptr2->GetMessageList(message_list2);
  EXPECT_EQ(message_list2.size(), 1);
  EXPECT_EQ(std::memcmp(message_list2.front()->RawData(), second_data, 3), 0);
}

TEST(StreamingWriterTest, streaming_create_test) {
  STREAMING_LOG(INFO) << "Streaming Create Test";
  std::vector<ray::ObjectID> queue_id_vec;
  uint32_t queue_num = 1;
  for (uint32_t i = 0; i < queue_num; ++i) {
    ObjectID queue_id = ray::ObjectID::FromRandom();
    ConvertToValidQueueId(queue_id);
    queue_id_vec.emplace_back(queue_id);
  }

  std::string plasma_store_path = STREAMING_STORE;
  std::vector<uint64_t> channel_seq_id_vec(queue_num, 0);
  std::shared_ptr<StreamingWriter> streaming_writer_client(new StreamingWriter());
  uint64_t queue_size = 10 * 1000 * 1000;
  std::vector<ray::ObjectID> remain_id_vec;
  streaming_writer_client->Init(queue_id_vec, plasma_store_path, channel_seq_id_vec,
                                std::vector<uint64_t>(queue_id_vec.size(), queue_size),
                                remain_id_vec);
  STREAMING_CHECK(remain_id_vec.empty()) << remain_id_vec.size();

  streaming_writer_client->Run();

  uint8_t first_data[] = {1, 2, 3};
  auto buffer = util::ToMessageBuffer(streaming_writer_client.get(), queue_id_vec[0],
                                      first_data, 3);
  streaming_writer_client->WriteMessageToBufferRing(queue_id_vec[0], buffer.Data(), 3);

  std::shared_ptr<StreamingReader> reader(new StreamingReader());
  reader->Init(plasma_store_path, queue_id_vec, -1);

  std::shared_ptr<StreamingReaderBundle> message =
      std::make_shared<StreamingReaderBundle>();
  reader->GetBundle(-1, message);
  StreamingMessageBundlePtr msg_ptr = StreamingMessageBundle::FromBytes(message->data);
  std::list<StreamingMessagePtr> message_list;
  msg_ptr->GetMessageList(message_list);
  EXPECT_EQ(message_list.size(), 1);
  EXPECT_EQ(std::memcmp(message_list.front()->RawData(), first_data, 3), 0);

  reader.reset();
  streaming_writer_client.reset();

  std::shared_ptr<StreamingWriter> writer_recreate(new StreamingWriter());
  for (auto &item : channel_seq_id_vec) {
    item++;
  }
  writer_recreate->Init(
      queue_id_vec, plasma_store_path, channel_seq_id_vec,
      std::vector<uint64_t>(channel_seq_id_vec.size(), queue_size), remain_id_vec,
      std::vector<StreamingQueueCreationType>(channel_seq_id_vec.size(),
                                              StreamingQueueCreationType::RECREATE));
  STREAMING_CHECK(remain_id_vec.empty()) << remain_id_vec.size();
  writer_recreate->Run();

  uint8_t second_data[] = {4, 5, 6};
  buffer = util::ToMessageBuffer(writer_recreate.get(), queue_id_vec[0], second_data, 3);
  writer_recreate->WriteMessageToBufferRing(queue_id_vec[0], buffer.Data(), 3);

  std::shared_ptr<StreamingReader> recreate_reader(new StreamingReader());
  recreate_reader->Init(plasma_store_path, queue_id_vec, -1);
  std::shared_ptr<StreamingReaderBundle> message2 =
      std::make_shared<StreamingReaderBundle>();
  recreate_reader->GetBundle(-1, message2);
  StreamingMessageBundlePtr msg_ptr2 = StreamingMessageBundle::FromBytes(message2->data);
  std::list<StreamingMessagePtr> message_list2;
  msg_ptr2->GetMessageList(message_list2);
  EXPECT_EQ(message_list2.size(), 1);
  EXPECT_EQ(std::memcmp(message_list2.front()->RawData(), first_data, 3), 0);
}

TEST(StreamingWriterTest, no_skip_source_barrier) {
  STREAMING_LOG(INFO) << "Streaming no skip source barrier Test";
  std::vector<ray::ObjectID> queue_id_vec;
  uint32_t queue_num = 1;
  for (uint32_t i = 0; i < queue_num; ++i) {
    ObjectID queue_id = ray::ObjectID::FromRandom();
    ConvertToValidQueueId(queue_id);
    queue_id_vec.emplace_back(queue_id);
  }

  std::string plasma_store_path = STREAMING_STORE;
  std::vector<uint64_t> channel_seq_id_vec(queue_num, 0);
  StreamingConfig config;
  config.SetStreaming_role(streaming::fbs::StreamingRole::Source);
  config.SetStreaming_strategy_(StreamingStrategy::EXACTLY_ONCE);
  std::shared_ptr<StreamingWriter> streaming_writer_client(new StreamingWriter());
  streaming_writer_client->SetConfig(config);
  uint64_t queue_size = 10 * 1000 * 1000;
  std::vector<ray::ObjectID> remain_id_vec;
  streaming_writer_client->Init(queue_id_vec, plasma_store_path, channel_seq_id_vec,
                                std::vector<uint64_t>(queue_id_vec.size(), queue_size),
                                remain_id_vec);
  STREAMING_CHECK(remain_id_vec.empty()) << remain_id_vec.size();

  streaming_writer_client->Run();

  uint8_t first_data[] = {1, 2, 3};
  auto buffer = util::ToMessageBuffer(streaming_writer_client.get(), queue_id_vec[0],
                                      first_data, 3);
  streaming_writer_client->WriteMessageToBufferRing(queue_id_vec[0], buffer.Data(),
                                                    buffer.Size());

  std::shared_ptr<StreamingReader> reader(new StreamingReader());
  reader->Init(plasma_store_path, queue_id_vec, -1);

  std::shared_ptr<StreamingReaderBundle> message =
      std::make_shared<StreamingReaderBundle>();
  reader->GetBundle(-1, message);
  StreamingMessageBundlePtr msg_ptr = StreamingMessageBundle::FromBytes(message->data);
  std::list<StreamingMessagePtr> message_list;
  msg_ptr->GetMessageList(message_list);
  EXPECT_EQ(message_list.size(), 1);
  EXPECT_EQ(std::memcmp(message_list.front()->RawData(), first_data, 3), 0);

  reader.reset();
  streaming_writer_client.reset();

  std::shared_ptr<StreamingWriter> writer_recreate(new StreamingWriter());
  writer_recreate->SetConfig(config);
  writer_recreate->Init(
      queue_id_vec, plasma_store_path, channel_seq_id_vec,
      std::vector<uint64_t>(channel_seq_id_vec.size(), queue_size), remain_id_vec,
      std::vector<StreamingQueueCreationType>(channel_seq_id_vec.size(),
                                              StreamingQueueCreationType::RECREATE));
  STREAMING_CHECK(remain_id_vec.empty()) << remain_id_vec.size();
  writer_recreate->Run();

  const uint8_t second_data[] = {4, 5, 6};
  writer_recreate->BroadcastBarrier(1, 1, second_data, 3);

  std::shared_ptr<StreamingReader> recreate_reader(new StreamingReader());
  std::vector<uint64_t> queue_seq_id({1});
  std::vector<uint64_t> queue_message_id({1});
  std::vector<ObjectID> abnormal_queues;
  recreate_reader->Init(plasma_store_path, queue_id_vec, queue_seq_id, queue_message_id,
                        -1, true, abnormal_queues);
  std::shared_ptr<StreamingReaderBundle> message2 =
      std::make_shared<StreamingReaderBundle>();
  recreate_reader->GetBundle(-1, message2);
  StreamingMessageBundlePtr msg_ptr2 = StreamingMessageBundle::FromBytes(message2->data);
  std::list<StreamingMessagePtr> message_list2;
  msg_ptr2->GetMessageList(message_list2);
  EXPECT_EQ(message2->meta->IsBarrier(), true);
  EXPECT_EQ(message_list2.size(), 1);
  EXPECT_EQ(
      std::memcmp(message_list2.front()->RawData() + kBarrierHeaderSize, second_data, 3),
      0);
}

int main(int argc, char **argv) {
  // set_streaming_log_config("streaming_writer_test", StreamingLogLevel::INFO, 0);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
