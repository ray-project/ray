#define BOOST_BIND_NO_PLACEHOLDERS
#include "common/status.h"
#include "data_reader.h"
#include "data_writer.h"
#include "gtest/gtest.h"
#include "message/message.h"
#include "message/message_bundle.h"
#include "queue/queue_client.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ring_buffer/ring_buffer.h"
using namespace std::placeholders;

const uint32_t MESSAGE_BOUND_SIZE = 10000;
const uint32_t DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE = 1000;

namespace ray {
namespace streaming {

class StreamingQueueTestSuite {
 public:
  StreamingQueueTestSuite(ActorID &peer_actor_id, std::vector<ObjectID> queue_ids,
                          std::vector<ObjectID> rescale_queue_ids)
      : peer_actor_id_(peer_actor_id),
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
  ActorID peer_actor_id_;
  std::vector<ObjectID> queue_ids_;
  std::vector<ObjectID> rescale_queue_ids_;
};

class StreamingQueueWriterTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingQueueWriterTestSuite(ActorID &peer_actor_id, std::vector<ObjectID> queue_ids,
                                std::vector<ObjectID> rescale_queue_ids)
      : StreamingQueueTestSuite(peer_actor_id, queue_ids, rescale_queue_ids) {
    test_func_map_ = {
        {"streaming_writer_exactly_once_test",
         std::bind(&StreamingQueueWriterTestSuite::StreamingWriterExactlyOnceTest,
                   this)}};
  }

 private:
  void StreamingWriterExactlyOnceTest() {
    StreamingConfig config;
    StreamingWriterStrategyTest(config);

    STREAMING_LOG(INFO)
        << "StreamingQueueWriterTestSuite::StreamingWriterExactlyOnceTest";
    status_ = true;
  }

  void StreamingWriterStrategyTest(StreamingConfig &config) {
    for (auto &queue_id : queue_ids_) {
      STREAMING_LOG(INFO) << "queue_id: " << queue_id;
    }
    ChannelCreationParameter param{
        peer_actor_id_,
        std::make_shared<RayFunction>(
            ray::Language::PYTHON,
            ray::FunctionDescriptorBuilder::FromVector(
                ray::Language::PYTHON, {"", "", "reader_async_call_func", ""})),
        std::make_shared<RayFunction>(
            ray::Language::PYTHON,
            ray::FunctionDescriptorBuilder::FromVector(
                ray::Language::PYTHON, {"", "", "reader_sync_call_func", ""}))};
    std::vector<ChannelCreationParameter> params(queue_ids_.size(), param);
    STREAMING_LOG(INFO) << "writer actor_ids size: " << params.size()
                        << " actor_id: " << peer_actor_id_;

    std::shared_ptr<RuntimeContext> runtime_context(new RuntimeContext());
    runtime_context->SetConfig(config);

    // Create writer.
    std::shared_ptr<DataWriter> streaming_writer_client(new DataWriter(runtime_context));
    uint64_t queue_size = 10 * 1000 * 1000;
    std::vector<uint64_t> channel_seq_id_vec(queue_ids_.size(), 0);
    streaming_writer_client->Init(queue_ids_, params, channel_seq_id_vec,
                                  std::vector<uint64_t>(queue_ids_.size(), queue_size));
    STREAMING_LOG(INFO) << "streaming_writer_client Init done";

    streaming_writer_client->Run();

    // Write some data.
    std::thread test_loop_thread(
        &StreamingQueueWriterTestSuite::TestWriteMessageToBufferRing, this,
        streaming_writer_client, std::ref(queue_ids_));
    if (test_loop_thread.joinable()) {
      test_loop_thread.join();
    }
  }

  void TestWriteMessageToBufferRing(std::shared_ptr<DataWriter> writer_client,
                                    std::vector<ray::ObjectID> &q_list) {
    uint32_t i = 1;
    while (i <= MESSAGE_BOUND_SIZE) {
      for (auto &q_id : q_list) {
        uint64_t buffer_len = (i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE);
        uint8_t *data = new uint8_t[buffer_len];
        for (uint32_t j = 0; j < buffer_len; ++j) {
          data[j] = j % 128;
        }
        STREAMING_LOG(DEBUG) << "Write data to queue, count=" << i
                             << ", queue_id=" << q_id;
        writer_client->WriteMessageToBufferRing(q_id, data, buffer_len,
                                                StreamingMessageType::Message);
        if (i % 10 == 0) {
          writer_client->BroadcastBarrier(i / 10, nullptr, 0);
        }
      }
      ++i;
    }
    STREAMING_LOG(INFO) << "Write data done.";
    // Wait a while.
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  }
};

class StreamingQueueReaderTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingQueueReaderTestSuite(ActorID peer_actor_id, std::vector<ObjectID> queue_ids,
                                std::vector<ObjectID> rescale_queue_ids)
      : StreamingQueueTestSuite(peer_actor_id, queue_ids, rescale_queue_ids) {
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
    STREAMING_LOG(INFO) << "Start read message bundle, queue_id_size="
                        << queue_id_vec.size();
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
        StreamingBarrierHeader barrier_header;
        StreamingMessage::GetBarrierIdFromRawData(msg->data + kMessageHeaderSize,
                                                  &barrier_header);
        STREAMING_LOG(DEBUG) << "barrier message recevied, time="
                             << msg->meta->GetMessageBundleTs()
                             << ", barrier_id=" << barrier_header.barrier_id
                             << ", data=" << Util::Byte2hex(msg->data, msg->data_size);
        std::unordered_map<ray::ObjectID, ConsumerChannelInfo> *offset_map;
        reader_client->GetOffsetInfo(offset_map);

        for (auto &q_id : queue_id_vec) {
          reader_client->NotifyConsumedItem((*offset_map)[q_id],
                                            (*offset_map)[q_id].current_message_id);
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
        uint64_t i = item->GetMessageId();

        uint32_t buff_len = i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE;
        if (i > MESSAGE_BOUND_SIZE) break;

        EXPECT_EQ(buff_len, item->PayloadSize());
        uint8_t *compared_data = new uint8_t[buff_len];
        for (uint32_t j = 0; j < item->PayloadSize(); ++j) {
          compared_data[j] = j % 128;
        }
        EXPECT_EQ(std::memcmp(compared_data, item->Payload(), item->PayloadSize()), 0);
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
    ChannelCreationParameter param{
        peer_actor_id_,
        std::make_shared<RayFunction>(
            ray::Language::PYTHON,
            ray::FunctionDescriptorBuilder::FromVector(
                ray::Language::PYTHON, {"", "", "writer_async_call_func", ""})),
        std::make_shared<RayFunction>(
            ray::Language::PYTHON,
            ray::FunctionDescriptorBuilder::FromVector(
                ray::Language::PYTHON, {"", "", "writer_sync_call_func", ""}))};
    std::vector<ChannelCreationParameter> params(queue_ids_.size(), param);
    STREAMING_LOG(INFO) << "reader actor_ids size: " << params.size()
                        << " actor_id: " << peer_actor_id_;
    std::shared_ptr<RuntimeContext> runtime_context(new RuntimeContext());
    runtime_context->SetConfig(config);
    std::shared_ptr<DataReader> reader(new DataReader(runtime_context));

    reader->Init(queue_ids_, params, -1);
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

class StreamingQueueUpStreamTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingQueueUpStreamTestSuite(ActorID &peer_actor_id, std::vector<ObjectID> queue_ids,
                                  std::vector<ObjectID> rescale_queue_ids)
      : StreamingQueueTestSuite(peer_actor_id, queue_ids, rescale_queue_ids) {
    test_func_map_ = {
        {"pull_peer_async_test",
         std::bind(&StreamingQueueUpStreamTestSuite::PullPeerAsyncTest, this)},
        {"get_queue_test",
         std::bind(&StreamingQueueUpStreamTestSuite::GetQueueTest, this)}};
  }

  void GetQueueTest() {
    // Sleep 2s, queue shoulde not exist when reader pull.
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    auto upstream_handler = ray::streaming::UpstreamQueueMessageHandler::GetService();
    ObjectID &queue_id = queue_ids_[0];
    RayFunction async_call_func{
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(
            ray::Language::PYTHON, {"", "", "reader_async_call_func", ""})};
    RayFunction sync_call_func{
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(
            ray::Language::PYTHON, {"", "", "reader_sync_call_func", ""})};
    upstream_handler->SetPeerActorID(queue_id, peer_actor_id_, async_call_func,
                                     sync_call_func);
    upstream_handler->CreateUpstreamQueue(queue_id, peer_actor_id_, 10240);
    STREAMING_LOG(INFO) << "IsQueueExist: "
                        << upstream_handler->UpstreamQueueExists(queue_id);

    // Sleep 2s, No valid data when reader pull
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000));
    STREAMING_LOG(INFO) << "StreamingQueueUpStreamTestSuite::GetQueueTest done";
    status_ = true;
  }

  void PullPeerAsyncTest() {
    // Sleep 2s, queue should not exist when reader pull.
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    auto upstream_handler = ray::streaming::UpstreamQueueMessageHandler::GetService();
    ObjectID &queue_id = queue_ids_[0];
    RayFunction async_call_func{
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(
            ray::Language::PYTHON, {"", "", "reader_async_call_func", ""})};
    RayFunction sync_call_func{
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(
            ray::Language::PYTHON, {"", "", "reader_sync_call_func", ""})};
    upstream_handler->SetPeerActorID(queue_id, peer_actor_id_, async_call_func,
                                     sync_call_func);
    std::shared_ptr<WriterQueue> queue =
        upstream_handler->CreateUpstreamQueue(queue_id, peer_actor_id_, 10240);
    STREAMING_LOG(INFO) << "IsQueueExist: "
                        << upstream_handler->UpstreamQueueExists(queue_id);

    // Sleep 2s, No valid data when reader pull
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    // message id starts from 1
    for (int msg_id = 1; msg_id <= 80; msg_id++) {
      uint8_t data[100];
      memset(data, msg_id, 100);
      STREAMING_LOG(INFO) << "Writer User Push item msg_id: " << msg_id;
      ASSERT_TRUE(
          queue->Push(data, 100, current_sys_time_ms(), msg_id, msg_id, true).ok());
      queue->Send();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    STREAMING_LOG(INFO) << "StreamingQueueUpStreamTestSuite::PullPeerAsyncTest done";
    status_ = true;
  }
};

class StreamingQueueDownStreamTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingQueueDownStreamTestSuite(ActorID peer_actor_id,
                                    std::vector<ObjectID> queue_ids,
                                    std::vector<ObjectID> rescale_queue_ids)
      : StreamingQueueTestSuite(peer_actor_id, queue_ids, rescale_queue_ids) {
    test_func_map_ = {
        {"pull_peer_async_test",
         std::bind(&StreamingQueueDownStreamTestSuite::PullPeerAsyncTest, this)},
        {"get_queue_test",
         std::bind(&StreamingQueueDownStreamTestSuite::GetQueueTest, this)}};
  };

  void GetQueueTest() {
    auto downstream_handler = ray::streaming::DownstreamQueueMessageHandler::GetService();
    ObjectID &queue_id = queue_ids_[0];
    RayFunction async_call_func{
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(
            ray::Language::PYTHON, {"", "", "writer_async_call_func", ""})};
    RayFunction sync_call_func{
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(
            ray::Language::PYTHON, {"", "", "writer_sync_call_func", ""})};
    downstream_handler->SetPeerActorID(queue_id, peer_actor_id_, async_call_func,
                                       sync_call_func);
    downstream_handler->CreateDownstreamQueue(queue_id, peer_actor_id_);

    bool is_upstream_first_pull_ = false;
    downstream_handler->PullQueue(queue_id, 1, is_upstream_first_pull_, 10 * 1000);
    ASSERT_TRUE(is_upstream_first_pull_);
    downstream_handler->PullQueue(queue_id, 1, is_upstream_first_pull_, 10 * 1000);
    ASSERT_FALSE(is_upstream_first_pull_);
    STREAMING_LOG(INFO) << "StreamingQueueDownStreamTestSuite::GetQueueTest done";
    status_ = true;
  }

  void PullPeerAsyncTest() {
    auto downstream_handler = ray::streaming::DownstreamQueueMessageHandler::GetService();
    ObjectID &queue_id = queue_ids_[0];
    RayFunction async_call_func{
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(
            ray::Language::PYTHON, {"", "", "writer_async_call_func", ""})};
    RayFunction sync_call_func{
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(
            ray::Language::PYTHON, {"", "", "writer_sync_call_func", ""})};
    downstream_handler->SetPeerActorID(queue_id, peer_actor_id_, async_call_func,
                                       sync_call_func);
    std::shared_ptr<ReaderQueue> queue =
        downstream_handler->CreateDownstreamQueue(queue_id, peer_actor_id_);

    bool is_first_pull;
    downstream_handler->PullQueue(queue_id, 1, is_first_pull, 10 * 1000);
    uint64_t count = 0;
    uint8_t msg_id = 1;
    while (true) {
      uint8_t *data = nullptr;
      uint32_t data_size = 0;
      uint64_t timeout_ms = 1000;
      QueueItem item = queue->PopPendingBlockTimeout(timeout_ms * 1000);
      if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
        STREAMING_LOG(INFO) << "PopPendingBlockTimeout timeout.";
        data = nullptr;
        data_size = 0;
      } else {
        data = item.Buffer()->Data();
        data_size = item.Buffer()->Size();
      }

      STREAMING_LOG(INFO) << "[Reader] count: " << count;
      if (data == nullptr) {
        STREAMING_LOG(INFO) << "[Reader] data null";
        continue;
      }

      for (uint32_t i = 0; i < data_size; i++) {
        ASSERT_EQ(data[i], msg_id);
      }

      count++;
      if (count == 80) {
        bool is_upstream_first_pull;
        msg_id = 50;
        downstream_handler->PullPeerAsync(queue_id, 50, is_upstream_first_pull, 1000);
        continue;
      }

      msg_id++;
      STREAMING_LOG(INFO) << "[Reader] count: " << count;
      if (count == 110) {
        break;
      }
    }

    STREAMING_LOG(INFO) << "StreamingQueueDownStreamTestSuite::PullPeerAsyncTest done";
    status_ = true;
  }
};

class TestSuiteFactory {
 public:
  static std::shared_ptr<StreamingQueueTestSuite> CreateTestSuite(
      std::shared_ptr<TestInitMessage> message) {
    std::shared_ptr<StreamingQueueTestSuite> test_suite = nullptr;
    std::string suite_name = message->TestSuiteName();
    queue::protobuf::StreamingQueueTestRole role = message->Role();
    const std::vector<ObjectID> &queue_ids = message->QueueIds();
    const std::vector<ObjectID> &rescale_queue_ids = message->RescaleQueueIds();
    ActorID peer_actor_id = message->PeerActorId();

    if (role == queue::protobuf::StreamingQueueTestRole::WRITER) {
      if (suite_name == "StreamingWriterTest") {
        test_suite = std::make_shared<StreamingQueueWriterTestSuite>(
            peer_actor_id, queue_ids, rescale_queue_ids);
      } else if (suite_name == "StreamingQueueTest") {
        test_suite = std::make_shared<StreamingQueueUpStreamTestSuite>(
            peer_actor_id, queue_ids, rescale_queue_ids);
      } else {
        STREAMING_CHECK(false) << "unsurported suite_name: " << suite_name;
      }
    } else {
      if (suite_name == "StreamingWriterTest") {
        test_suite = std::make_shared<StreamingQueueReaderTestSuite>(
            peer_actor_id, queue_ids, rescale_queue_ids);
      } else if (suite_name == "StreamingQueueTest") {
        test_suite = std::make_shared<StreamingQueueDownStreamTestSuite>(
            peer_actor_id, queue_ids, rescale_queue_ids);
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
    // You must keep it same with `src/ray/core_worker/core_worker.h:CoreWorkerOptions`
    CoreWorkerOptions options;
    options.worker_type = WorkerType::WORKER;
    options.language = Language::PYTHON;
    options.store_socket = store_socket;
    options.raylet_socket = raylet_socket;
    options.gcs_options = gcs_options;
    options.enable_logging = true;
    options.install_failure_signal_handler = true;
    options.node_ip_address = "127.0.0.1";
    options.node_manager_port = node_manager_port;
    options.raylet_ip_address = "127.0.0.1";
    options.task_execution_callback = std::bind(&StreamingWorker::ExecuteTask, this, _1,
                                                _2, _3, _4, _5, _6, _7, _8, _9);
    options.num_workers = 1;
    options.metrics_agent_port = -1;
    CoreWorkerProcess::Initialize(options);
    STREAMING_LOG(INFO) << "StreamingWorker constructor";
  }

  void RunTaskExecutionLoop() {
    // Start executing tasks.
    CoreWorkerProcess::RunTaskExecutionLoop();
  }

 private:
  Status ExecuteTask(TaskType task_type, const std::string task_name,
                     const RayFunction &ray_function,
                     const std::unordered_map<std::string, double> &required_resources,
                     const std::vector<std::shared_ptr<RayObject>> &args,
                     const std::vector<rpc::ObjectReference> &arg_refs,
                     const std::vector<ObjectID> &return_ids,
                     const std::string &debugger_breakpoint,
                     std::vector<std::shared_ptr<RayObject>> *results) {
    // Only one arg param used in streaming.
    STREAMING_CHECK(args.size() >= 1) << "args.size() = " << args.size();

    ray::FunctionDescriptor function_descriptor = ray_function.GetFunctionDescriptor();
    RAY_CHECK(function_descriptor->Type() ==
              ray::FunctionDescriptorType::kPythonFunctionDescriptor);
    auto typed_descriptor = function_descriptor->As<ray::PythonFunctionDescriptor>();
    STREAMING_LOG(DEBUG) << "StreamingWorker::ExecuteTask "
                         << typed_descriptor->ToString();

    std::string func_name = typed_descriptor->FunctionName();
    if (func_name == "init") {
      std::shared_ptr<LocalMemoryBuffer> local_buffer =
          std::make_shared<LocalMemoryBuffer>(args[0]->GetData()->Data(),
                                              args[0]->GetData()->Size(), true);
      HandleInitTask(local_buffer);
    } else if (func_name == "execute_test") {
      STREAMING_LOG(INFO) << "Test name: " << typed_descriptor->ClassName();
      test_suite_->ExecuteTest(typed_descriptor->ClassName());
    } else if (func_name == "check_current_test_status") {
      results->push_back(
          std::make_shared<RayObject>(test_suite_->CheckCurTestStatus(), nullptr,
                                      std::vector<rpc::ObjectReference>()));
    } else if (func_name == "reader_sync_call_func") {
      if (test_suite_->TestDone()) {
        STREAMING_LOG(WARNING) << "Test has done!!";
        return Status::OK();
      }
      std::shared_ptr<LocalMemoryBuffer> local_buffer =
          std::make_shared<LocalMemoryBuffer>(args[1]->GetData()->Data(),
                                              args[1]->GetData()->Size(), true);
      auto result_buffer = reader_client_->OnReaderMessageSync(local_buffer);
      results->push_back(std::make_shared<RayObject>(
          result_buffer, nullptr, std::vector<rpc::ObjectReference>()));
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
      results->push_back(std::make_shared<RayObject>(
          result_buffer, nullptr, std::vector<rpc::ObjectReference>()));
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
    reader_client_ = std::make_shared<ReaderClient>();
    writer_client_ = std::make_shared<WriterClient>();
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

    std::string actor_handle_serialized = message->ActorHandleSerialized();
    CoreWorkerProcess::GetCoreWorker().DeserializeAndRegisterActorHandle(
        actor_handle_serialized, ObjectID::Nil());
    std::shared_ptr<ActorHandle> actor_handle(new ActorHandle(actor_handle_serialized));
    STREAMING_CHECK(actor_handle != nullptr);
    STREAMING_LOG(INFO) << "Actor id from handle: " << actor_handle->GetActorID();

    STREAMING_LOG(INFO) << "HandleInitTask queues:";
    for (auto qid : message->QueueIds()) {
      STREAMING_LOG(INFO) << "queue: " << qid;
    }
    for (auto qid : message->RescaleQueueIds()) {
      STREAMING_LOG(INFO) << "rescale queue: " << qid;
    }

    test_suite_ = TestSuiteFactory::CreateTestSuite(message);
    STREAMING_CHECK(test_suite_ != nullptr);
  }

 private:
  std::shared_ptr<ReaderClient> reader_client_;
  std::shared_ptr<WriterClient> writer_client_;
  std::shared_ptr<std::thread> test_thread_;
  std::shared_ptr<StreamingQueueTestSuite> test_suite_;
  std::shared_ptr<ActorHandle> peer_actor_handle_;
};

}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  RAY_CHECK(argc == 5);
  auto store_socket = std::string(argv[1]);
  auto raylet_socket = std::string(argv[2]);
  auto node_manager_port = std::stoi(std::string(argv[3]));
  // auto runtime_env_hash = std::string(argv[4]); // Unused in this test

  ray::gcs::GcsClientOptions gcs_options("127.0.0.1", 6379, "");
  ray::streaming::StreamingWorker worker(store_socket, raylet_socket, node_manager_port,
                                         gcs_options);
  worker.RunTaskExecutionLoop();
  return 0;
}
