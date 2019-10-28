#include <cstdlib>
#include <iostream>
#include <list>
#include <vector>
#include "gtest/gtest.h"

#include "plasma/client.h"
#include "plasma/common.h"
#include "ray/util/util.h"
#include "test/streaming_ray_test_common.h"
#include "test/test_utils.h"

#define random(a, b) ((rand() % ((b) + 1 - a)) + (a))

using namespace ray;
using namespace ray::streaming;

class StreamingReaderTest : public StreamingRayPipeTest {};

uint8_t *get_test_data(int len) {
  auto *data = new uint8_t[len];
  for (int i = 0; i < len; ++i) {
    data[i] = static_cast<uint8_t>(i % 256);
  }
  return data;
}

bool check_test_data(uint8_t *data, int len) {
  for (int i = 0; i < len; ++i) {
    if (data[i] != i % 256) return false;
  }
  return true;
}

bool check_test_reader_msg(std::shared_ptr<StreamingReaderBundle> &msg) {
  StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
  std::list<StreamingMessagePtr> msg_list;
  bundle_ptr->GetMessageList(msg_list);
  for (auto &msg_ptr : msg_list) {
    if (!check_test_data(msg_ptr->RawData(), msg_ptr->GetDataSize())) {
      return false;
    }
  }
  return true;
}

TEST_F(StreamingReaderTest, reader_duplicated_item_test) {
  StreamingConfig config1;
  config1.SetStreaming_raylet_socket_path(raylet_socket_name_1_);

  int queue_num = 1;
  std::vector<ObjectID> queue_ids;
  std::vector<uint64_t> msg_offset_vec(queue_num, 0);
  std::vector<uint64_t> queue_seq_ids(queue_num, 0);
  for (int i = 0; i < queue_num; ++i) {
    ObjectID queue_id = ObjectID::FromRandom();
    queue_ids.emplace_back(queue_id);
  }

  int msg_first_round = 100;
  int data_len = 100;
  // write some message first
  STREAMING_LOG(INFO) << "First round";
  std::shared_ptr<StreamingWriter> writer(new StreamingWriter());
  writer->SetConfig(config1);
  std::vector<ObjectID> remain_id_vec;
  writer->Init(queue_ids, store_socket_name_1_, msg_offset_vec,
               std::vector<uint64_t>(queue_ids.size(), 1000000), remain_id_vec);
  STREAMING_CHECK(remain_id_vec.empty());
  writer->Run();

  for (int i = 0; i < msg_first_round; ++i) {
    for (auto &qid : queue_ids) {
      uint8_t *data = get_test_data(100);
      auto buffer = util::ToMessageBuffer(writer.get(), qid, data, data_len);
      writer->WriteMessageToBufferRing(qid, buffer.Data(), data_len,
                                       StreamingMessageType::Message);
      delete data;
    }
  }

  usleep(100 * 1000);
  writer = nullptr;

  int msg_second_round = 1000;
  // restart writer, resend some message from smaller msg_id
  STREAMING_LOG(INFO) << "Second Round";
  writer = std::make_shared<StreamingWriter>();
  writer->SetConfig(config1);
  std::vector<ObjectID> remain_id_vec2;

  writer->Init(queue_ids, store_socket_name_1_, msg_offset_vec,
               std::vector<uint64_t>(queue_ids.size(), 1000000), remain_id_vec2);

  STREAMING_CHECK(remain_id_vec.empty()) << remain_id_vec2.size();
  writer->Run();
  for (int i = 0; i < msg_second_round; ++i) {
    for (auto &qid : queue_ids) {
      uint8_t *data = get_test_data(100);
      writer->WriteMessageToBufferRing(qid, data, data_len,
                                       StreamingMessageType::Message);
      delete data;
    }
  }

  STREAMING_LOG(INFO) << "Checking data";
  // check data
  std::shared_ptr<StreamingReader> reader(new StreamingReader());
  reader->SetConfig(config1);
  reader->Init(store_socket_name_1_, queue_ids, -1);
  std::unordered_map<ObjectID, uint64_t> msg_cnt;
  int tot_msg = 0;
  for (int i = 0; i < msg_second_round * queue_num; ++i) {
    std::shared_ptr<StreamingReaderBundle> msg;
    reader->GetBundle(100, msg);
    if (msg->data) {
      StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
      std::list<StreamingMessagePtr> msg_list;
      bundle_ptr->GetMessageList(msg_list);
      for (auto &msg_ptr : msg_list) {
        ASSERT_EQ(check_test_data(msg_ptr->RawData(), msg_ptr->GetDataSize()), true);
        // important. checking if msg_id is continuous.
        STREAMING_LOG(DEBUG) << "msg_id=" << msg_ptr->GetMessageSeqId()
                             << ", qid=" << msg->from.Hex();
        ASSERT_EQ(msg_ptr->GetMessageSeqId(), ++msg_cnt[msg->from]);
      }
      STREAMING_LOG(DEBUG) << "OK, msg_cnt=" << msg_cnt[msg->from]
                           << ", qid=" << msg->from.Hex();
      tot_msg += bundle_ptr->GetMessageListSize();
    }
    if (tot_msg == queue_num * msg_second_round) {
      break;
    }
    STREAMING_LOG(DEBUG) << "total message number => " << tot_msg;
  }
}

std::string StreamingRayPipeTest::store_executable = "";
std::string StreamingRayPipeTest::raylet_executable = "";

int main(int argc, char **argv) {
  set_streaming_log_config("streaming_reader_test", StreamingLogLevel::INFO);
  ::testing::InitGoogleTest(&argc, argv);
  StreamingRayPipeTest::store_executable = std::string(argv[1]);
  StreamingRayPipeTest::raylet_executable = std::string(argv[2]);
  std::string redis_executable = std::string(argv[3]);
  std::string ray_redis_module_file = std::string(argv[4]);
  start_redis(redis_executable, ray_redis_module_file);
  return RUN_ALL_TESTS();
}
