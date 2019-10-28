#include "gtest/gtest.h"

#include "streaming_persistence.h"
#include "streaming_reader.h"
#include "streaming_writer.h"
#include "test/test_utils.h"

using namespace ray::streaming;
using namespace ray;

const uint32_t MESSAGE_BOUND_SIZE = 10000;
const uint32_t DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE = 1000;
const std::string STREAMING_STORE = "/tmp/store_streaming_tests";
const uint32_t MESSAGE_BARRIER_INTERVAL = 1000;

class StreamingExactlySameTest : public ::testing::TestWithParam<uint64_t> {
  virtual void SetUp() {
    // set_streaming_log_config("streaming_exactly_same_test", StreamingLogLevel::INFO,
    // 0);
  }

  virtual void TearDown() {
    if (reader_client) {
      delete reader_client;
    }
    if (writer_client) {
      delete writer_client;
    }
  }

 protected:
  StreamingWriter *writer_client = nullptr;
  StreamingReader *reader_client = nullptr;
};

void RemoveAllMetaFile(const std::vector<ray::ObjectID> &q_list,
                       uint64_t max_checkpoint_id) {
  std::string fake_dir = "/tmp/fake";
  std::shared_ptr<StreamingFileIO> delete_handler(
#ifdef USE_PANGU
      new StreamingPanguFileSystem(fake_dir, true));
  StreamingPanguFileSystem::Init();
  std::string store_prefix = "/zdfs_test/";
#else
      new StreamingLocalFileSystem(fake_dir, true));
  std::string store_prefix = "/tmp/";
#endif

  for (auto &q_item : q_list) {
    for (uint64_t i = 0; i <= max_checkpoint_id; ++i) {
      delete_handler->Delete(store_prefix + q_item.Hex() + "_" + std::to_string(i));
    }
  }

#ifdef USE_PANGU
  StreamingPanguFileSystem::Destory();
#endif
}

void TestWriteMessageToBufferRing(StreamingWriter *writer_client,
                                  const std::vector<ray::ObjectID> &q_list) {
  uint64_t rollback_checkpoint_id =
      writer_client->GetConfig().GetStreaming_rollback_checkpoint_id();
  uint32_t i = 1 + rollback_checkpoint_id * MESSAGE_BARRIER_INTERVAL;
  const uint8_t temp_data[] = {1};
  while (i <= MESSAGE_BOUND_SIZE) {
    for (auto &q_id : q_list) {
      uint64_t buffer_len = (i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE);
      uint8_t *data = new uint8_t[buffer_len];
      for (uint32_t j = 0; j < buffer_len; ++j) {
        data[j] = j % 128;
      }
      auto buffer = util::ToMessageBuffer(writer_client, q_id, data, buffer_len);
      delete[] data;
      writer_client->WriteMessageToBufferRing(q_id, buffer.Data(), buffer_len,
                                              StreamingMessageType::Message);
    }
    if (i % MESSAGE_BARRIER_INTERVAL == 0) {
      writer_client->BroadcastBarrier(i / MESSAGE_BARRIER_INTERVAL,
                                      i / MESSAGE_BARRIER_INTERVAL, temp_data, 1);
      // Sleep for generating empty message bundle
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ++i;
  }
}

void ReaderLoopForward(StreamingReader *reader_client, StreamingWriter *writer_client,
                       const std::vector<ray::ObjectID> &queue_id_vec,
                       std::vector<StreamingMessageBundlePtr> &bundle_vec) {
  uint64_t rollback_checkpoint_id =
      reader_client->GetConfig().GetStreaming_rollback_checkpoint_id();
  const uint64_t expect_recevied_message_cnt =
      queue_id_vec.size() *
      (MESSAGE_BOUND_SIZE - rollback_checkpoint_id * MESSAGE_BARRIER_INTERVAL);

  uint64_t recevied_message_cnt = 0;
  std::unordered_map<ray::ObjectID, uint64_t> queue_last_cp_id;

  for (auto &q_id : queue_id_vec) {
    queue_last_cp_id[q_id] = 0;
  }

  while (true) {
    std::shared_ptr<StreamingReaderBundle> msg;
    reader_client->GetBundle(1000, msg);

    if (!msg->data) {
      STREAMING_LOG(DEBUG) << "read bundle timeout";
      continue;
    }
    StreamingMessageBundlePtr bundle_ptr;
    bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);

    if (msg->meta->GetBundleType() == StreamingMessageBundleType::Barrier) {
      STREAMING_LOG(DEBUG) << "barrier message recevied => "
                           << msg->meta->GetMessageBundleTs();
      std::unordered_map<ObjectID, ConsumerChannelInfo> *offset_map;
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
      bundle_vec.push_back(bundle_ptr);
      continue;
    }

    std::list<StreamingMessagePtr> message_list;
    bundle_ptr->GetMessageList(message_list);
    bundle_vec.push_back(bundle_ptr);

    recevied_message_cnt += message_list.size();
    for (auto &item : message_list) {
      uint64_t i = item->GetMessageSeqId();

      uint32_t buff_len = i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE;
      if (i > MESSAGE_BOUND_SIZE) break;

      ASSERT_EQ(buff_len, item->GetDataSize());
      std::unique_ptr<uint8_t> compared_data(new uint8_t[buff_len]);
      for (uint32_t j = 0; j < item->GetDataSize(); ++j) {
        *(compared_data.get() + j) = j % 128;
      }
      ASSERT_EQ(std::memcmp(compared_data.get(), item->RawData(), item->GetDataSize()),
                0);
    }
    STREAMING_LOG(DEBUG) << "Received message count => " << recevied_message_cnt;
    if (recevied_message_cnt == expect_recevied_message_cnt) {
      break;
    }
  }
}

void streaming_strategy_test(
    StreamingConfig &config, const std::vector<ray::ObjectID> &queue_id_vec,
    StreamingWriter **writer_client_ptr, StreamingReader **reader_client_ptr,
    std::vector<StreamingMessageBundlePtr> &bundle_vec,
    StreamingQueueCreationType queue_creation_type = StreamingQueueCreationType::RECREATE,
    streaming::fbs::StreamingRole replay_role = streaming::fbs::StreamingRole::Operator,
    bool remove_meta_file = false) {
  std::string plasma_store_path = STREAMING_STORE;

  STREAMING_LOG(INFO) << "start store first";
  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    STREAMING_LOG(INFO) << " qid hex => " << queue_id_vec[i].Hex() << " store path => "
                        << plasma_store_path;
  }
  STREAMING_LOG(INFO) << "Writer Setup.";
  *writer_client_ptr = new StreamingWriter();
  uint64_t queue_size = 10 * 1000 * 1000;
  std::vector<ObjectID> remain_id_vec;
  std::vector<uint64_t> queue_size_vec(queue_id_vec.size(), queue_size);
  auto writer_client = *writer_client_ptr;
  uint64_t rollback_checkpoint_id = config.GetStreaming_rollback_checkpoint_id();
  std::vector<uint64_t> channel_seq_id_vec(
      queue_id_vec.size(), rollback_checkpoint_id * MESSAGE_BARRIER_INTERVAL);
  config.SetStreaming_role(replay_role);
  writer_client->SetConfig(config);
  writer_client->Init(queue_id_vec, plasma_store_path, channel_seq_id_vec, queue_size_vec,
                      remain_id_vec,
                      std::vector<StreamingQueueCreationType>(channel_seq_id_vec.size(),
                                                              queue_creation_type));
  STREAMING_CHECK(remain_id_vec.empty())
      << remain_id_vec.size() << "int streaming writer failed.";

  writer_client->Run();
  std::thread test_loop_thread(&TestWriteMessageToBufferRing, writer_client,
                               std::ref(queue_id_vec));

  test_loop_thread.detach();
  std::thread timeout_thread([]() {
    std::this_thread::sleep_for(std::chrono::seconds(3 * 60));
    STREAMING_LOG(WARNING) << "test timeout";
    exit(1);
  });
  timeout_thread.detach();

  STREAMING_LOG(INFO) << "Reader Setup.";
  *reader_client_ptr = new StreamingReader();
  auto reader_client = *reader_client_ptr;
  config.SetStreaming_role(streaming::fbs::StreamingRole::Sink);
  reader_client->SetConfig(config);

  reader_client->Init(plasma_store_path, queue_id_vec, -1);
  ReaderLoopForward(reader_client, writer_client, queue_id_vec, bundle_vec);
  if (test_loop_thread.joinable()) {
    test_loop_thread.join();
  }
  if (config.IsExactlySame() && remove_meta_file) {
    writer_client->Stop();
    // Sleep 50ms for crashing in pangu causeof write empty message and remove
    // file in multithreads
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    RemoveAllMetaFile(queue_id_vec, MESSAGE_BOUND_SIZE / MESSAGE_BARRIER_INTERVAL);
  }
}

TEST_P(StreamingExactlySameTest, streaming_exactly_same_source_test) {
  StreamingConfig config;
  config.SetStreaming_empty_message_time_interval(5);

  uint32_t queue_num = 2;
  STREAMING_LOG(INFO) << "Streaming Strategy => EXACTLY_SAME Source";
  config.SetStreaming_strategy_(StreamingStrategy::EXACTLY_SAME);

  std::vector<ray::ObjectID> queue_id_vec;
  for (uint32_t i = 0; i < queue_num; ++i) {
    queue_id_vec.push_back(ray::ObjectID::FromRandom());
  }

  config.SetStreaming_persistence_checkpoint_max_cnt(100);
  std::vector<StreamingMessageBundlePtr> first_bundle_vec;
  streaming_strategy_test(config, queue_id_vec, &writer_client, &reader_client,
                          first_bundle_vec);

  delete writer_client;
  delete reader_client;

  uint64_t checkpoint_id = GetParam();
  std::vector<StreamingMessageBundlePtr> second_bundle_vec;
  config.SetStreaming_rollback_checkpoint_id(checkpoint_id);

  STREAMING_LOG(INFO) << "Streaming Replay Start => EXACTLY_SAME Source";

  streaming_strategy_test(config, queue_id_vec, &writer_client, &reader_client,
                          second_bundle_vec,
                          StreamingQueueCreationType::RECREATE_AND_CLEAR,
                          streaming::fbs::StreamingRole::Source, true);

  // EXPECT_EQ(first_bundle_vec.size(), second_bundle_vec.size());
  uint32_t rollback_meta_vec_size = second_bundle_vec.size();
  uint32_t original_meta_vec_size = first_bundle_vec.size();
  uint64_t meta_ts = 0;
  STREAMING_LOG(INFO) << "original meta vec size " << original_meta_vec_size
                      << ", rollback_meta_vec_size " << rollback_meta_vec_size;

  for (uint32_t i = 0; i < rollback_meta_vec_size; ++i) {
    uint32_t index = original_meta_vec_size - rollback_meta_vec_size + i;
    if (!first_bundle_vec[index]->operator==(second_bundle_vec[i].get())) {
      STREAMING_LOG(INFO) << "i : " << i << " , index => " << index << ", "
                          << first_bundle_vec[index]->ToString() << "|"
                          << second_bundle_vec[i]->ToString();
      STREAMING_CHECK(false);
    }
    EXPECT_TRUE(first_bundle_vec[index]->operator==(second_bundle_vec[i].get()));
    EXPECT_TRUE(meta_ts <= first_bundle_vec[index]->GetMessageBundleTs());
    meta_ts = first_bundle_vec[index]->GetMessageBundleTs();
  }

  delete writer_client;
  delete reader_client;
  writer_client = nullptr;
  reader_client = nullptr;
}

TEST_P(StreamingExactlySameTest, streaming_exactly_same_operator_test) {
  StreamingConfig config;
  config.SetStreaming_empty_message_time_interval(5);

  uint32_t queue_num = 2;
  STREAMING_LOG(INFO) << "Streaming Strategy => EXACTLY_SAME Operator";
  config.SetStreaming_strategy_(StreamingStrategy::EXACTLY_SAME);

  std::vector<ray::ObjectID> queue_id_vec;
  for (uint32_t i = 0; i < queue_num; ++i) {
    queue_id_vec.push_back(ray::ObjectID::FromRandom());
  }

  std::vector<StreamingMessageBundlePtr> first_bundle_vec;
  config.SetStreaming_persistence_checkpoint_max_cnt(100);
  streaming_strategy_test(config, queue_id_vec, &writer_client, &reader_client,
                          first_bundle_vec);

  delete writer_client;
  delete reader_client;

  uint64_t checkpoint_id = GetParam();
  std::vector<StreamingMessageBundlePtr> second_bundle_vec;
  config.SetStreaming_rollback_checkpoint_id(checkpoint_id);
  STREAMING_LOG(INFO) << "Streaming Replay Start => EXACTLY_SAME Operator";

  streaming_strategy_test(config, queue_id_vec, &writer_client, &reader_client,
                          second_bundle_vec,
                          StreamingQueueCreationType::RECREATE_AND_CLEAR,
                          streaming::fbs::StreamingRole::Operator, true);

  // EXPECT_EQ(first_bundle_vec.size(), second_bundle_vec.size());
  uint32_t rollback_meta_vec_size = second_bundle_vec.size();
  uint32_t original_meta_vec_size = first_bundle_vec.size();
  uint64_t meta_ts = 0;
  for (uint32_t i = 0; i < rollback_meta_vec_size; ++i) {
    uint32_t index = original_meta_vec_size - rollback_meta_vec_size + i;
    if (!first_bundle_vec[index]->operator==(second_bundle_vec[i].get())) {
      STREAMING_LOG(INFO) << "i : " << i << " , index => " << index << ", "
                          << first_bundle_vec[index]->ToString() << "|"
                          << second_bundle_vec[i]->ToString();
    }
    EXPECT_TRUE(first_bundle_vec[index]->operator==(second_bundle_vec[i].get()));
    EXPECT_TRUE(meta_ts <= first_bundle_vec[index]->GetMessageBundleTs());
    meta_ts = first_bundle_vec[index]->GetMessageBundleTs();
  }

  delete writer_client;
  delete reader_client;
  writer_client = nullptr;
  reader_client = nullptr;
}

INSTANTIATE_TEST_CASE_P(TrueReturn, StreamingExactlySameTest,
                        testing::Values(0, 1, 5, 9));

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
