#include "transport.h"
#include "utils.h"

namespace ray {
namespace streaming {

static constexpr int TASK_OPTION_RETURN_NUM_0 = 0;
static constexpr int TASK_OPTION_RETURN_NUM_1 = 1;

const uint32_t Message::MagicNum = 0xBABA0510;

std::unique_ptr<LocalMemoryBuffer> Message::ToBytes() {
  uint8_t *bytes = nullptr;

  std::string pboutput;
  ConstructProtoBuf(&pboutput);
  int64_t fbs_length = pboutput.length();

  ray::streaming::queue::protobuf::StreamingQueueMessageType type = Type();
  size_t total_len =
      sizeof(Message::MagicNum) + sizeof(type) + sizeof(fbs_length) + fbs_length;
  if (buffer_ != nullptr) {
    total_len += buffer_->Size();
  }
  bytes = new uint8_t[total_len];
  STREAMING_CHECK(bytes != nullptr) << "allocate bytes fail.";

  uint8_t *p_cur = bytes;
  memcpy(p_cur, &Message::MagicNum, sizeof(Message::MagicNum));

  p_cur += sizeof(Message::MagicNum);
  memcpy(p_cur, &type, sizeof(type));

  p_cur += sizeof(type);
  memcpy(p_cur, &fbs_length, sizeof(fbs_length));

  p_cur += sizeof(fbs_length);
  uint8_t *fbs_bytes = (uint8_t*)pboutput.data();
  memcpy(p_cur, fbs_bytes, fbs_length);
  p_cur += fbs_length;

  if (buffer_ != nullptr) {
    memcpy(p_cur, buffer_->Data(), buffer_->Size());
  }

  // COPY
  std::unique_ptr<LocalMemoryBuffer> buffer =
      std::unique_ptr<LocalMemoryBuffer>(new LocalMemoryBuffer(bytes, total_len, true));
  delete bytes;
  return buffer;
}

void DataMessage::ConstructProtoBuf(std::string *output) {
  ray::streaming::queue::protobuf::StreamingQueueDataMsg msg;
  msg.set_src_actor_id(actor_id_.Binary());
  msg.set_dst_actor_id(peer_actor_id_.Binary());
  msg.set_queue_id(queue_id_.Binary());
  msg.set_seq_id(seq_id_);
  msg.set_length(buffer_->Size());
  msg.set_raw(raw_);
  msg.SerializeToString(output);
}

std::shared_ptr<DataMessage> DataMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(ray::streaming::queue::protobuf::StreamingQueueMessageType);
  uint64_t *fbs_length = (uint64_t *)bytes;
  bytes += sizeof(uint64_t);

  std::string inputpb(reinterpret_cast<char const*>(bytes), *fbs_length);
  ray::streaming::queue::protobuf::StreamingQueueDataMsg message;
  message.ParseFromString(inputpb);
  ActorID src_actor_id = ActorID::FromBinary(message.src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.queue_id());
  uint64_t seq_id = message.seq_id();
  uint64_t length = message.length();
  bool raw = message.raw();
  bytes += *fbs_length;

  /// COPY
  std::shared_ptr<LocalMemoryBuffer> buffer =
      std::make_shared<LocalMemoryBuffer>(bytes, (size_t)length, true);
  std::shared_ptr<DataMessage> data_msg = std::make_shared<DataMessage>(
      src_actor_id, dst_actor_id, queue_id, seq_id, buffer, raw);

  return data_msg;
}

void NotificationMessage::ConstructProtoBuf(std::string *output) {
  ray::streaming::queue::protobuf::StreamingQueueNotificationMsg msg;
  msg.set_src_actor_id(actor_id_.Binary());
  msg.set_dst_actor_id(peer_actor_id_.Binary());
  msg.set_queue_id(queue_id_.Binary());
  msg.set_seq_id(seq_id_);
  msg.SerializeToString(output);
}

std::shared_ptr<NotificationMessage> NotificationMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(ray::streaming::queue::protobuf::StreamingQueueMessageType);
  uint64_t* length = (uint64_t*)bytes;
  bytes += sizeof(uint64_t);

  std::string inputpb(reinterpret_cast<char const*>(bytes), *length);
  ray::streaming::queue::protobuf::StreamingQueueNotificationMsg message;
  message.ParseFromString(inputpb);
  STREAMING_LOG(INFO) << "message.src_actor_id: " << message.src_actor_id();
  ActorID src_actor_id = ActorID::FromBinary(message.src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.queue_id());
  uint64_t seq_id = message.seq_id();

  std::shared_ptr<NotificationMessage> notify_msg =
      std::make_shared<NotificationMessage>(src_actor_id, dst_actor_id, queue_id, seq_id);

  return notify_msg;
}

void CheckMessage::ConstructProtoBuf(std::string *output) {
  ray::streaming::queue::protobuf::StreamingQueueCheckMsg msg;
  msg.set_src_actor_id(actor_id_.Binary());
  msg.set_dst_actor_id(peer_actor_id_.Binary());
  msg.set_queue_id(queue_id_.Binary());
  msg.SerializeToString(output);
}

std::shared_ptr<CheckMessage> CheckMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(ray::streaming::queue::protobuf::StreamingQueueMessageType);
  uint64_t* length = (uint64_t*)bytes;
  bytes += sizeof(uint64_t);

  std::string inputpb(reinterpret_cast<char const*>(bytes), *length);
  ray::streaming::queue::protobuf::StreamingQueueCheckMsg message;
  message.ParseFromString(inputpb);
  ActorID src_actor_id = ActorID::FromBinary(message.src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.queue_id());

  std::shared_ptr<CheckMessage> check_msg =
      std::make_shared<CheckMessage>(src_actor_id, dst_actor_id, queue_id);

  return check_msg;
}

void CheckRspMessage::ConstructProtoBuf(std::string *output) {
  ray::streaming::queue::protobuf::StreamingQueueCheckRspMsg msg;
  msg.set_src_actor_id(actor_id_.Binary());
  msg.set_dst_actor_id(peer_actor_id_.Binary());
  msg.set_queue_id(queue_id_.Binary());
  msg.set_err_code(err_code_);
  msg.SerializeToString(output);
}

std::shared_ptr<CheckRspMessage> CheckRspMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(ray::streaming::queue::protobuf::StreamingQueueMessageType);
  uint64_t* length = (uint64_t*)bytes;
  bytes += sizeof(uint64_t);

  std::string inputpb(reinterpret_cast<char const*>(bytes), *length);
  ray::streaming::queue::protobuf::StreamingQueueCheckRspMsg message;
  message.ParseFromString(inputpb);
  ActorID src_actor_id = ActorID::FromBinary(message.src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.queue_id());
  ray::streaming::queue::protobuf::StreamingQueueError err_code = message.err_code();

  std::shared_ptr<CheckRspMessage> check_rsp_msg =
      std::make_shared<CheckRspMessage>(src_actor_id, dst_actor_id, queue_id, err_code);

  return check_rsp_msg;
}

void TestInitMessage::ConstructProtoBuf(std::string *output) {
  ray::streaming::queue::protobuf::StreamingQueueTestInitMessage msg;
  msg.set_role(role_);
  msg.set_src_actor_id(actor_id_.Binary());
  msg.set_dst_actor_id(peer_actor_id_.Binary());
  msg.set_actor_handle(actor_handle_serialized_);
  for (auto &queue_id : queue_ids_) {
    msg.add_queue_ids(queue_id.Binary());
  }
  for (auto &queue_id : rescale_queue_ids_) {
    msg.add_rescale_queue_ids(queue_id.Binary());
  }
  msg.set_test_suite_name(test_suite_name_);
  msg.set_test_name(test_name_);
  msg.set_param(param_);
  msg.SerializeToString(output);
}

std::shared_ptr<TestInitMessage> TestInitMessage::FromBytes(
    uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(ray::streaming::queue::protobuf::StreamingQueueMessageType);
  uint64_t* length = (uint64_t*)bytes;
  bytes += sizeof(uint64_t);

  std::string inputpb(reinterpret_cast<char const*>(bytes), *length);
  ray::streaming::queue::protobuf::StreamingQueueTestInitMessage message;
  message.ParseFromString(inputpb);
  queue::protobuf::StreamingQueueTestRole role = message.role();
  ActorID src_actor_id = ActorID::FromBinary(message.src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.dst_actor_id());
  std::string actor_handle_serialized = message.actor_handle();
  std::vector<ObjectID> queue_ids;
  for (int i=0; i<message.queue_ids_size(); i++) {
    queue_ids.push_back(ObjectID::FromBinary(message.queue_ids(i)));
  }
  std::vector<ObjectID> rescale_queue_ids;
  for (int i=0; i<message.rescale_queue_ids_size(); i++) {
    rescale_queue_ids.push_back(ObjectID::FromBinary(message.rescale_queue_ids(i)));
  }
  std::string test_suite_name = message.test_suite_name();
  std::string test_name = message.test_name();
  uint64_t param = message.param();

  std::shared_ptr<TestInitMessage> test_init_msg =
      std::make_shared<TestInitMessage>(role, src_actor_id, dst_actor_id, actor_handle_serialized, queue_ids, rescale_queue_ids, test_suite_name, test_name, param);

  return test_init_msg;
}

void TestCheckStatusRspMsg::ConstructProtoBuf(std::string *output) {
  ray::streaming::queue::protobuf::StreamingQueueTestCheckStatusRspMsg msg;
  msg.set_test_name(test_name_);
  msg.set_status(status_);
  msg.SerializeToString(output);
}

std::shared_ptr<TestCheckStatusRspMsg> TestCheckStatusRspMsg::FromBytes(
    uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(ray::streaming::queue::protobuf::StreamingQueueMessageType);
  uint64_t* length = (uint64_t*)bytes;
  bytes += sizeof(uint64_t);

  std::string inputpb(reinterpret_cast<char const*>(bytes), *length);
  ray::streaming::queue::protobuf::StreamingQueueTestCheckStatusRspMsg message;
  message.ParseFromString(inputpb);
  std::string test_name = message.test_name();
  bool status = message.status();

  std::shared_ptr<TestCheckStatusRspMsg> test_check_msg =
      std::make_shared<TestCheckStatusRspMsg>(test_name, status);

  return test_check_msg;
}

void Transport::Send(std::unique_ptr<LocalMemoryBuffer> buffer) {
  STREAMING_LOG(INFO) << "Transport::Send buffer size: " << buffer->Size();
  std::unordered_map<std::string, double> resources;
  TaskOptions options{TASK_OPTION_RETURN_NUM_0, resources};

  char meta_data[3] = {'R', 'A', 'W'};
  std::shared_ptr<LocalMemoryBuffer> meta =
      std::make_shared<LocalMemoryBuffer>((uint8_t *)meta_data, 3, true);

  std::vector<TaskArg> args;
  if (async_func_.GetLanguage() == Language::PYTHON) {
    char dummy_meta[5] = {'D', 'U', 'M', 'M', 'Y'};
     std::shared_ptr<LocalMemoryBuffer> dummy_meta_buf =
      std::make_shared<LocalMemoryBuffer>((uint8_t *)dummy_meta, 5, true);
    char dummy[1] = {' '};
    std::shared_ptr<LocalMemoryBuffer> dummyBuffer =
        std::make_shared<LocalMemoryBuffer>((uint8_t *)(dummy), 1, true);
    args.emplace_back(TaskArg::PassByValue(
        std::make_shared<RayObject>(std::move(dummyBuffer), dummy_meta_buf, true)));
  }
  args.emplace_back(
      TaskArg::PassByValue(std::make_shared<RayObject>(std::move(buffer), meta, true)));
  
  STREAMING_CHECK(core_worker_ != nullptr);
  std::vector<ObjectID> return_ids;
  std::vector<std::shared_ptr<RayObject>> results;
  ray::Status st = core_worker_->SubmitActorTask(peer_actor_id_, async_func_, args,
                                                         options, &return_ids);
  if (!st.ok()) {
    STREAMING_LOG(ERROR) << "SubmitActorTask fail. " << st;
  }

  Status get_st = core_worker_->Get(return_ids, -1, &results);
  if (!get_st.ok()) {
    STREAMING_LOG(ERROR) << "Get fail.";
  }
}

std::shared_ptr<LocalMemoryBuffer> Transport::SendForResult(
    std::shared_ptr<LocalMemoryBuffer> buffer, int64_t timeout_ms) {
  std::unordered_map<std::string, double> resources;
  TaskOptions options{TASK_OPTION_RETURN_NUM_1, resources};

  char meta_data[3] = {'R', 'A', 'W'};
  std::shared_ptr<LocalMemoryBuffer> meta =
      std::make_shared<LocalMemoryBuffer>((uint8_t *)meta_data, 3, true);

  std::vector<TaskArg> args;
  if (async_func_.GetLanguage() == Language::PYTHON) {
    char dummy_meta[5] = {'D', 'U', 'M', 'M', 'Y'};
     std::shared_ptr<LocalMemoryBuffer> dummy_meta_buf =
      std::make_shared<LocalMemoryBuffer>((uint8_t *)dummy_meta, 5, true);
    char dummy[1] = {' '};
    std::shared_ptr<LocalMemoryBuffer> dummyBuffer =
        std::make_shared<LocalMemoryBuffer>(
            (uint8_t *)(dummy), 1, true);
    args.emplace_back(
        TaskArg::PassByValue(std::make_shared<RayObject>(dummyBuffer, dummy_meta_buf, true)));
  }
  args.emplace_back(
      TaskArg::PassByValue(std::make_shared<RayObject>(buffer, meta, true)));

  STREAMING_CHECK(core_worker_ != nullptr);
  std::vector<ObjectID> return_ids;
  ray::Status st = core_worker_->SubmitActorTask(peer_actor_id_, sync_func_, args,
                                                 options, &return_ids);
  if (!st.ok()) {
    STREAMING_LOG(ERROR) << "SubmitActorTask fail.";
  }

  std::vector<bool> wait_results;
  std::vector<std::shared_ptr<RayObject>> results;
  Status wait_st = core_worker_->Wait(return_ids, 1, timeout_ms, &wait_results);
  if (!wait_st.ok()) {
    STREAMING_LOG(ERROR) << "Wait fail.";
    return nullptr;
  }
  STREAMING_CHECK(wait_results.size() >= 1);
  if (!wait_results[0]) {
    STREAMING_LOG(ERROR) << "Wait direct call fail.";
    return nullptr;
  }

  Status get_st = core_worker_->Get(return_ids, -1, &results);
  if (!get_st.ok()) {
    STREAMING_LOG(ERROR) << "Get fail.";
    return nullptr;
  }
  STREAMING_CHECK(results.size() >= 1);
  if (results[0]->IsException()) {
    STREAMING_LOG(ERROR) << "peer actor may has exceptions, should retry.";
    return nullptr;
  }
  STREAMING_CHECK(results[0]->HasData());
  /// TODO: size 4 means byte[] array size 1, we will remove this by adding protobuf
  /// command.
  if (results[0]->GetSize() == 4) {
    STREAMING_LOG(WARNING) << "peer actor may not ready yet, should retry.";
    return nullptr;
  }

  std::shared_ptr<Buffer> result_buffer = results[0]->GetData();
  std::shared_ptr<LocalMemoryBuffer> return_buffer = std::make_shared<LocalMemoryBuffer>(
      result_buffer->Data(), result_buffer->Size(), true);
  return return_buffer;
}

std::shared_ptr<LocalMemoryBuffer> Transport::SendForResultWithRetry(
    std::unique_ptr<LocalMemoryBuffer> buffer, int retry_cnt, int64_t timeout_ms) {
  STREAMING_LOG(INFO) << "SendForResultWithRetry retry_cnt: " << retry_cnt
                      << " timeout_ms: " << timeout_ms;
  std::shared_ptr<LocalMemoryBuffer> buffer_shared = std::move(buffer);
  for (int cnt = 0; cnt < retry_cnt; cnt++) {
    auto result = SendForResult(buffer_shared, timeout_ms);
    if (result != nullptr) {
      return result;
    }
  }

  STREAMING_LOG(WARNING) << "SendForResultWithRetry fail after retry.";
  return nullptr;
}

std::shared_ptr<LocalMemoryBuffer> Transport::Recv() {
  STREAMING_CHECK(false) << "Should not be called.";
  return nullptr;
}
}  // namespace streaming
}  // namespace ray
