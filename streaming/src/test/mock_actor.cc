#define BOOST_BIND_NO_PLACEHOLDERS
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "src/ray/util/test_util.h"

#include "data_reader.h"
#include "data_writer.h"
#include "message/message.h"
#include "message/message_bundle.h"
#include "queue/queue_client.h"
#include "ring_buffer.h"
#include "status.h"

#include "gtest/gtest.h"
using namespace std::placeholders;

const uint32_t MESSAGE_BOUND_SIZE = 10000;
const uint32_t DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE = 1000;

namespace ray {
namespace streaming {

class StreamingQueueTestSuite {
 public:
  StreamingQueueTestSuite(std::shared_ptr<CoreWorker> core_worker, ActorID &peer_actor_id,
                          std::vector<ObjectID> queue_ids,
                          std::vector<ObjectID> rescale_queue_ids)
      : core_worker_(core_worker),
        peer_actor_id_(peer_actor_id),
        queue_ids_(queue_ids),
        rescale_queue_ids_(rescale_queue_ids) {}

  virtual void ExecuteTest(std::string test_name) {
    auto it = test_func_map_.find(test_name);
    STREAMING_CHECK(it != test_func_map_.end());
    current_test_ = test_name;
    status_ = false;
    auto func = it->second;
    executor_thread_ = std::make_shared<std::thread>(func);
    executor_thread_->detach();
  }

  virtual std::shared_ptr<LocalMemoryBuffer> CheckCurTestStatus() {
    TestCheckStatusRspMsg msg(current_test_, status_);
    return msg.ToBytes();
  }

  virtual bool TestDone() { return status_; }

  virtual ~StreamingQueueTestSuite() {}

 protected:
  std::unordered_map<std::string, std::function<void()>> test_func_map_;
  std::string current_test_;
  bool status_;
  std::shared_ptr<std::thread> executor_thread_;
  std::shared_ptr<CoreWorker> core_worker_;
  ActorID peer_actor_id_;
  std::vector<ObjectID> queue_ids_;
  std::vector<ObjectID> rescale_queue_ids_;
};

class StreamingQueueWriterTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingQueueWriterTestSuite(std::shared_ptr<CoreWorker> core_worker,
                                ActorID &peer_actor_id, std::vector<ObjectID> queue_ids,
                                std::vector<ObjectID> rescale_queue_ids)
      : StreamingQueueTestSuite(core_worker, peer_actor_id, queue_ids,
                                rescale_queue_ids) {
    test_func_map_ = {
        {"streaming_writer_exactly_once_test",
         std::bind(&StreamingQueueWriterTestSuite::StreamingWriterExactlyOnceTest,
                   this)}};
  }

 private:
  void TestWriteMessageToBufferRing(std::shared_ptr<DataWriter> writer_client,
                                    std::vector<ray::ObjectID> &q_list) {
    // const uint8_t temp_data[] = {1, 2, 4, 5};

    uint32_t i = 1;
    while (i <= MESSAGE_BOUND_SIZE) {
      for (auto &q_id : q_list) {
        uint64_t buffer_len = (i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE);
        uint8_t *data = new uint8_t[buffer_len];
        for (uint32_t j = 0; j < buffer_len; ++j) {
          data[j] = j % 128;
        }

        writer_client->WriteMessageToBufferRing(q_id, data, buffer_len,
                                                StreamingMessageType::Message);
      }
      ++i;
    }

    // Wait a while
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  }

  void StreamingWriterStrategyTest(StreamingConfig &config) {
    for (auto &queue_id : queue_ids_) {
      STREAMING_LOG(INFO) << "queue_id: " << queue_id;
    }
    std::vector<ActorID> actor_ids(queue_ids_.size(), peer_actor_id_);
    STREAMING_LOG(INFO) << "writer actor_ids size: " << actor_ids.size()
                        << " actor_id: " << peer_actor_id_;

    std::shared_ptr<RuntimeContext> runtime_context(new RuntimeContext());
    runtime_context->SetConfig(config);

    std::shared_ptr<DataWriter> streaming_writer_client(new DataWriter(runtime_context));
    uint64_t queue_size = 10 * 1000 * 1000;
    std::vector<uint64_t> channel_seq_id_vec(queue_ids_.size(), 0);
    streaming_writer_client->Init(queue_ids_, actor_ids, channel_seq_id_vec,
                                  std::vector<uint64_t>(queue_ids_.size(), queue_size));
    STREAMING_LOG(INFO) << "streaming_writer_client Init done";

    streaming_writer_client->Run();
    std::thread test_loop_thread(
        &StreamingQueueWriterTestSuite::TestWriteMessageToBufferRing, this,
        streaming_writer_client, std::ref(queue_ids_));
    // test_loop_thread.detach();
    if (test_loop_thread.joinable()) {
      test_loop_thread.join();
    }
  }

  void StreamingWriterExactlyOnceTest() {
    StreamingConfig config;
    StreamingWriterStrategyTest(config);

    STREAMING_LOG(INFO)
        << "StreamingQueueWriterTestSuite::StreamingWriterExactlyOnceTest";
    status_ = true;
  }
};

class StreamingQueueReaderTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingQueueReaderTestSuite(std::shared_ptr<CoreWorker> core_worker,
                                ActorID peer_actor_id, std::vector<ObjectID> queue_ids,
                                std::vector<ObjectID> rescale_queue_ids)
      : StreamingQueueTestSuite(core_worker, peer_actor_id, queue_ids,
                                rescale_queue_ids) {
    test_func_map_ = {
        {"streaming_writer_exactly_once_test",
         std::bind(&StreamingQueueReaderTestSuite::StreamingWriterExactlyOnceTest,
                   this)}};
  }

 private:
  void ReaderLoopForward(std::shared_ptr<DataReader> reader_client,
                         std::shared_ptr<DataWriter> writer_client,
                         std::vector<ray::ObjectID> &queue_id_vec) {
    uint64_t recevied_message_cnt = 0;
    std::unordered_map<ray::ObjectID, uint64_t> queue_last_cp_id;

    for (auto &q_id : queue_id_vec) {
      queue_last_cp_id[q_id] = 0;
    }
    STREAMING_LOG(INFO) << "Start read message bundle";
    while (true) {
      std::shared_ptr<DataBundle> msg;
      StreamingStatus st = reader_client->GetBundle(100, msg);

      if (st != StreamingStatus::OK || !msg->data) {
        STREAMING_LOG(DEBUG) << "read bundle timeout, status = " << (int)st;
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
        // writer_client->ClearCheckpoint(msg->last_barrier_id);

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
      STREAMING_LOG(INFO) << "message size => " << message_list.size()
                          << " from queue id => " << msg->from.Hex()
                          << " last message id => " << msg->meta->GetLastMessageId();

      recevied_message_cnt += message_list.size();
      for (auto &item : message_list) {
        uint64_t i = item->GetMessageSeqId();

        uint32_t buff_len = i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE;
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

  void StreamingReaderStrategyTest(StreamingConfig &config) {
    std::vector<ActorID> actor_ids(queue_ids_.size(), peer_actor_id_);
    STREAMING_LOG(INFO) << "reader actor_ids size: " << actor_ids.size()
                        << " actor_id: " << peer_actor_id_;
    std::shared_ptr<RuntimeContext> runtime_context(new RuntimeContext());
    runtime_context->SetConfig(config);
    std::shared_ptr<DataReader> reader(new DataReader(runtime_context));

    reader->Init(queue_ids_, actor_ids, -1);
    ReaderLoopForward(reader, nullptr, queue_ids_);

    STREAMING_LOG(INFO) << "Reader exit";
  }

  void StreamingWriterExactlyOnceTest() {
    STREAMING_LOG(INFO)
        << "StreamingQueueReaderTestSuite::StreamingWriterExactlyOnceTest";
    StreamingConfig config;

    StreamingReaderStrategyTest(config);
    status_ = true;
  }
};

class TestSuiteFactory {
 public:
  static std::shared_ptr<StreamingQueueTestSuite> CreateTestSuite(
      std::shared_ptr<CoreWorker> worker, std::shared_ptr<TestInitMessage> message) {
    std::shared_ptr<StreamingQueueTestSuite> test_suite = nullptr;
    std::string suite_name = message->TestSuiteName();
    queue::protobuf::StreamingQueueTestRole role = message->Role();
    const std::vector<ObjectID> &queue_ids = message->QueueIds();
    const std::vector<ObjectID> &rescale_queue_ids = message->RescaleQueueIds();
    ActorID peer_actor_id = message->PeerActorId();

    if (role == queue::protobuf::StreamingQueueTestRole::WRITER) {
      if (suite_name == "StreamingWriterTest") {
        test_suite = std::make_shared<StreamingQueueWriterTestSuite>(
            worker, peer_actor_id, queue_ids, rescale_queue_ids);
      } else {
        STREAMING_CHECK(false) << "unsurported suite_name: " << suite_name;
      }
    } else {
      if (suite_name == "StreamingWriterTest") {
        test_suite = std::make_shared<StreamingQueueReaderTestSuite>(
            worker, peer_actor_id, queue_ids, rescale_queue_ids);
      } else {
        STREAMING_CHECK(false) << "unsupported suite_name: " << suite_name;
      }
    }

    return test_suite;
  }
};

class StreamingWorker {
 public:
  StreamingWorker(const std::string &store_socket, const std::string &raylet_socket,
                  int node_manager_port, const gcs::GcsClientOptions &gcs_options)
      : test_suite_(nullptr), peer_actor_handle_(nullptr) {
    worker_ = std::make_shared<CoreWorker>(
        WorkerType::WORKER, Language::PYTHON, store_socket, raylet_socket,
        JobID::FromInt(1), gcs_options, "", "127.0.0.1", node_manager_port,
        std::bind(&StreamingWorker::ExecuteTask, this, _1, _2, _3, _4, _5, _6, _7));

    RayFunction reader_async_call_func{ray::Language::PYTHON, {"reader_async_call_func"}};
    RayFunction reader_sync_call_func{ray::Language::PYTHON, {"reader_sync_call_func"}};
    RayFunction writer_async_call_func{ray::Language::PYTHON, {"writer_async_call_func"}};
    RayFunction writer_sync_call_func{ray::Language::PYTHON, {"writer_sync_call_func"}};

    reader_client_ = std::make_shared<ReaderClient>(worker_.get(), reader_async_call_func,
                                                    reader_sync_call_func);
    writer_client_ = std::make_shared<WriterClient>(worker_.get(), writer_async_call_func,
                                                    writer_sync_call_func);
    STREAMING_LOG(INFO) << "StreamingWorker constructor";
  }

  void StartExecutingTasks() {
    // Start executing tasks.
    worker_->StartExecutingTasks();
  }

 private:
  Status ExecuteTask(TaskType task_type, const RayFunction &ray_function,
                     const std::unordered_map<std::string, double> &required_resources,
                     const std::vector<std::shared_ptr<RayObject>> &args,
                     const std::vector<ObjectID> &arg_reference_ids,
                     const std::vector<ObjectID> &return_ids,
                     std::vector<std::shared_ptr<RayObject>> *results) {
    // Only one arg param used in streaming.
    STREAMING_CHECK(args.size() >= 1) << "args.size() = " << args.size();

    std::vector<std::string> function_descriptor = ray_function.GetFunctionDescriptor();
    STREAMING_LOG(INFO) << "StreamingWorker::ExecuteTask " << function_descriptor[0];

    std::string func_name = function_descriptor[0];
    if (func_name == "init") {
      std::shared_ptr<LocalMemoryBuffer> local_buffer =
          std::make_shared<LocalMemoryBuffer>(args[0]->GetData()->Data(),
                                              args[0]->GetData()->Size(), true);
      HandleInitTask(local_buffer);
    } else if (func_name == "execute_test") {
      STREAMING_LOG(INFO) << "Test name: " << function_descriptor[1];
      test_suite_->ExecuteTest(function_descriptor[1]);
    } else if (func_name == "check_current_test_status") {
      results->push_back(
          std::make_shared<RayObject>(test_suite_->CheckCurTestStatus(), nullptr));
    } else if (func_name == "reader_sync_call_func") {
      if (test_suite_->TestDone()) {
        STREAMING_LOG(WARNING) << "Test has done!!";
        return Status::OK();
      }
      std::shared_ptr<LocalMemoryBuffer> local_buffer =
          std::make_shared<LocalMemoryBuffer>(args[1]->GetData()->Data(),
                                              args[1]->GetData()->Size(), true);
      auto result_buffer = reader_client_->OnReaderMessageSync(local_buffer);
      results->push_back(std::make_shared<RayObject>(result_buffer, nullptr));
    } else if (func_name == "reader_async_call_func") {
      if (test_suite_->TestDone()) {
        STREAMING_LOG(WARNING) << "Test has done!!";
        return Status::OK();
      }
      std::shared_ptr<LocalMemoryBuffer> local_buffer =
          std::make_shared<LocalMemoryBuffer>(args[1]->GetData()->Data(),
                                              args[1]->GetData()->Size(), true);
      reader_client_->OnReaderMessage(local_buffer);
    } else if (func_name == "writer_sync_call_func") {
      if (test_suite_->TestDone()) {
        STREAMING_LOG(WARNING) << "Test has done!!";
        return Status::OK();
      }
      std::shared_ptr<LocalMemoryBuffer> local_buffer =
          std::make_shared<LocalMemoryBuffer>(args[1]->GetData()->Data(),
                                              args[1]->GetData()->Size(), true);
      auto result_buffer = writer_client_->OnWriterMessageSync(local_buffer);
      results->push_back(std::make_shared<RayObject>(result_buffer, nullptr));
    } else if (func_name == "writer_async_call_func") {
      if (test_suite_->TestDone()) {
        STREAMING_LOG(WARNING) << "Test has done!!";
        return Status::OK();
      }
      std::shared_ptr<LocalMemoryBuffer> local_buffer =
          std::make_shared<LocalMemoryBuffer>(args[1]->GetData()->Data(),
                                              args[1]->GetData()->Size(), true);
      writer_client_->OnWriterMessage(local_buffer);
    } else {
      STREAMING_LOG(WARNING) << "Invalid function name " << func_name;
    }

    return Status::OK();
  }

 private:
  void HandleInitTask(std::shared_ptr<LocalMemoryBuffer> buffer) {
    uint8_t *bytes = buffer->Data();
    uint8_t *p_cur = bytes;
    uint32_t *magic_num = (uint32_t *)p_cur;
    STREAMING_CHECK(*magic_num == Message::MagicNum);

    p_cur += sizeof(Message::MagicNum);
    queue::protobuf::StreamingQueueMessageType *type =
        (queue::protobuf::StreamingQueueMessageType *)p_cur;
    STREAMING_CHECK(
        *type ==
        queue::protobuf::StreamingQueueMessageType::StreamingQueueTestInitMsgType);
    std::shared_ptr<TestInitMessage> message = TestInitMessage::FromBytes(bytes);

    STREAMING_LOG(INFO) << "Init message: " << message->ToString();
    std::string actor_handle_serialized = message->ActorHandleSerialized();
    worker_->DeserializeAndRegisterActorHandle(actor_handle_serialized);
    std::shared_ptr<ActorHandle> actor_handle(new ActorHandle(actor_handle_serialized));
    STREAMING_CHECK(actor_handle != nullptr);
    STREAMING_LOG(INFO) << " actor id from handle: " << actor_handle->GetActorID();
    ;

    // STREAMING_LOG(INFO) << "actor_handle_serialized: " << actor_handle_serialized;
    // peer_actor_handle_ =
    //     std::make_shared<ActorHandle>(actor_handle_serialized);

    STREAMING_LOG(INFO) << "HandleInitTask queues:";
    for (auto qid : message->QueueIds()) {
      STREAMING_LOG(INFO) << "queue: " << qid;
    }
    for (auto qid : message->RescaleQueueIds()) {
      STREAMING_LOG(INFO) << "rescale queue: " << qid;
    }

    test_suite_ = TestSuiteFactory::CreateTestSuite(worker_, message);
    STREAMING_CHECK(test_suite_ != nullptr);
  }

 private:
  std::shared_ptr<CoreWorker> worker_;
  std::shared_ptr<ReaderClient> reader_client_;
  std::shared_ptr<WriterClient> writer_client_;
  std::shared_ptr<std::thread> test_thread_;
  std::shared_ptr<StreamingQueueTestSuite> test_suite_;
  std::shared_ptr<ActorHandle> peer_actor_handle_;
};

}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  RAY_CHECK(argc == 4);
  auto store_socket = std::string(argv[1]);
  auto raylet_socket = std::string(argv[2]);
  auto node_manager_port = std::stoi(std::string(argv[3]));

  ray::gcs::GcsClientOptions gcs_options("127.0.0.1", 6379, "");
  ray::streaming::StreamingWorker worker(store_socket, raylet_socket, node_manager_port,
                                         gcs_options);
  worker.StartExecutingTasks();
  return 0;
}
