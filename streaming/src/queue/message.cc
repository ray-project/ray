#include "queue/message.h"

namespace ray {
namespace streaming {
const uint32_t Message::MagicNum = 0xBABA0510;

std::unique_ptr<LocalMemoryBuffer> Message::ToBytes() {
  uint8_t *bytes = nullptr;

  std::string pboutput;
  ToProtobuf(&pboutput);
  int64_t fbs_length = pboutput.length();

  queue::protobuf::StreamingQueueMessageType type = Type();
  size_t total_len = kItemHeaderSize + fbs_length;
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
  uint8_t *fbs_bytes = (uint8_t *)pboutput.data();
  memcpy(p_cur, fbs_bytes, fbs_length);
  p_cur += fbs_length;

  if (buffer_ != nullptr) {
    memcpy(p_cur, buffer_->Data(), buffer_->Size());
  }

  // COPY
  std::unique_ptr<LocalMemoryBuffer> buffer =
      std::make_unique<LocalMemoryBuffer>(bytes, total_len, true);
  delete bytes;
  return buffer;
}

void Message::FillMessageCommon(queue::protobuf::MessageCommon *common) {
  common->set_src_actor_id(actor_id_.Binary());
  common->set_dst_actor_id(peer_actor_id_.Binary());
  common->set_queue_id(queue_id_.Binary());
}

void DataMessage::ToProtobuf(std::string *output) {
  queue::protobuf::StreamingQueueDataMsg msg;
  FillMessageCommon(msg.mutable_common());
  msg.set_seq_id(seq_id_);
  msg.set_msg_id_start(msg_id_start_);
  msg.set_msg_id_end(msg_id_end_);
  msg.set_length(buffer_->Size());
  msg.set_raw(raw_);
  msg.SerializeToString(output);
}

std::shared_ptr<DataMessage> DataMessage::FromBytes(uint8_t *bytes) {
  uint64_t *fbs_length = (uint64_t *)(bytes + kItemMetaHeaderSize);
  bytes += kItemHeaderSize;
  std::string inputpb(reinterpret_cast<char const *>(bytes), *fbs_length);
  queue::protobuf::StreamingQueueDataMsg message;
  message.ParseFromString(inputpb);
  ActorID src_actor_id = ActorID::FromBinary(message.common().src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.common().dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.common().queue_id());
  uint64_t msg_id_start = message.msg_id_start();
  uint64_t msg_id_end = message.msg_id_end();
  uint64_t seq_id = message.seq_id();
  uint64_t length = message.length();
  bool raw = message.raw();
  bytes += *fbs_length;

  /// Copy data and create a new buffer for streaming queue.
  std::shared_ptr<LocalMemoryBuffer> buffer =
      std::make_shared<LocalMemoryBuffer>(bytes, (size_t)length, true);
  std::shared_ptr<DataMessage> data_msg =
      std::make_shared<DataMessage>(src_actor_id, dst_actor_id, queue_id, seq_id,
                                    msg_id_start, msg_id_end, buffer, raw);

  return data_msg;
}

void NotificationMessage::ToProtobuf(std::string *output) {
  queue::protobuf::StreamingQueueNotificationMsg msg;
  FillMessageCommon(msg.mutable_common());
  msg.set_seq_id(msg_id_);
  msg.SerializeToString(output);
}

std::shared_ptr<NotificationMessage> NotificationMessage::FromBytes(uint8_t *bytes) {
  uint64_t *length = (uint64_t *)(bytes + kItemMetaHeaderSize);
  bytes += kItemHeaderSize;
  std::string inputpb(reinterpret_cast<char const *>(bytes), *length);
  queue::protobuf::StreamingQueueNotificationMsg message;
  message.ParseFromString(inputpb);
  ActorID src_actor_id = ActorID::FromBinary(message.common().src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.common().dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.common().queue_id());
  uint64_t seq_id = message.seq_id();

  std::shared_ptr<NotificationMessage> notify_msg =
      std::make_shared<NotificationMessage>(src_actor_id, dst_actor_id, queue_id, seq_id);

  return notify_msg;
}

void CheckMessage::ToProtobuf(std::string *output) {
  queue::protobuf::StreamingQueueCheckMsg msg;
  FillMessageCommon(msg.mutable_common());
  msg.SerializeToString(output);
}

std::shared_ptr<CheckMessage> CheckMessage::FromBytes(uint8_t *bytes) {
  uint64_t *length = (uint64_t *)(bytes + kItemMetaHeaderSize);
  bytes += kItemHeaderSize;
  std::string inputpb(reinterpret_cast<char const *>(bytes), *length);
  queue::protobuf::StreamingQueueCheckMsg message;
  message.ParseFromString(inputpb);
  ActorID src_actor_id = ActorID::FromBinary(message.common().src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.common().dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.common().queue_id());

  std::shared_ptr<CheckMessage> check_msg =
      std::make_shared<CheckMessage>(src_actor_id, dst_actor_id, queue_id);

  return check_msg;
}

void CheckRspMessage::ToProtobuf(std::string *output) {
  queue::protobuf::StreamingQueueCheckRspMsg msg;
  FillMessageCommon(msg.mutable_common());
  msg.set_err_code(err_code_);
  msg.SerializeToString(output);
}

std::shared_ptr<CheckRspMessage> CheckRspMessage::FromBytes(uint8_t *bytes) {
  uint64_t *length = (uint64_t *)(bytes + kItemMetaHeaderSize);
  bytes += kItemHeaderSize;
  std::string inputpb(reinterpret_cast<char const *>(bytes), *length);
  queue::protobuf::StreamingQueueCheckRspMsg message;
  message.ParseFromString(inputpb);
  ActorID src_actor_id = ActorID::FromBinary(message.common().src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.common().dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.common().queue_id());
  queue::protobuf::StreamingQueueError err_code = message.err_code();

  std::shared_ptr<CheckRspMessage> check_rsp_msg =
      std::make_shared<CheckRspMessage>(src_actor_id, dst_actor_id, queue_id, err_code);

  return check_rsp_msg;
}

void PullRequestMessage::ToProtobuf(std::string *output) {
  queue::protobuf::StreamingQueuePullRequestMsg msg;
  FillMessageCommon(msg.mutable_common());
  msg.set_msg_id(msg_id_);
  msg.SerializeToString(output);
}

std::shared_ptr<PullRequestMessage> PullRequestMessage::FromBytes(uint8_t *bytes) {
  uint64_t *length = (uint64_t *)(bytes + kItemMetaHeaderSize);
  bytes += kItemHeaderSize;
  std::string inputpb(reinterpret_cast<char const *>(bytes), *length);
  queue::protobuf::StreamingQueuePullRequestMsg message;
  message.ParseFromString(inputpb);
  ActorID src_actor_id = ActorID::FromBinary(message.common().src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.common().dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.common().queue_id());
  uint64_t msg_id = message.msg_id();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id
                       << " msg_id:" << msg_id;

  std::shared_ptr<PullRequestMessage> pull_msg =
      std::make_shared<PullRequestMessage>(src_actor_id, dst_actor_id, queue_id, msg_id);
  return pull_msg;
}

void PullResponseMessage::ToProtobuf(std::string *output) {
  queue::protobuf::StreamingQueuePullResponseMsg msg;
  FillMessageCommon(msg.mutable_common());
  msg.set_seq_id(seq_id_);
  msg.set_msg_id(msg_id_);
  msg.set_err_code(err_code_);
  msg.set_is_upstream_first_pull(is_upstream_first_pull_);
  msg.SerializeToString(output);
}

std::shared_ptr<PullResponseMessage> PullResponseMessage::FromBytes(uint8_t *bytes) {
  uint64_t *length = (uint64_t *)(bytes + kItemMetaHeaderSize);
  bytes += kItemHeaderSize;
  std::string inputpb(reinterpret_cast<char const *>(bytes), *length);
  queue::protobuf::StreamingQueuePullResponseMsg message;
  message.ParseFromString(inputpb);
  ActorID src_actor_id = ActorID::FromBinary(message.common().src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.common().dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.common().queue_id());
  uint64_t seq_id = message.seq_id();
  uint64_t msg_id = message.msg_id();
  queue::protobuf::StreamingQueueError err_code = message.err_code();
  bool is_upstream_first_pull = message.is_upstream_first_pull();

  STREAMING_LOG(INFO) << "src_actor_id:" << src_actor_id
                      << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id
                      << " seq_id: " << seq_id << " msg_id: " << msg_id << " err_code:"
                      << queue::protobuf::StreamingQueueError_Name(err_code)
                      << " is_upstream_first_pull: " << is_upstream_first_pull;

  std::shared_ptr<PullResponseMessage> pull_rsp_msg =
      std::make_shared<PullResponseMessage>(src_actor_id, dst_actor_id, queue_id, seq_id,
                                            msg_id, err_code, is_upstream_first_pull);

  return pull_rsp_msg;
}

void ResendDataMessage::ToProtobuf(std::string *output) {
  queue::protobuf::StreamingQueueResendDataMsg msg;
  FillMessageCommon(msg.mutable_common());
  msg.set_first_seq_id(first_seq_id_);
  msg.set_last_seq_id(last_seq_id_);
  msg.set_seq_id(seq_id_);
  msg.set_msg_id_start(msg_id_start_);
  msg.set_msg_id_end(msg_id_end_);
  msg.set_length(buffer_->Size());
  msg.set_raw(raw_);
  msg.SerializeToString(output);
}

std::shared_ptr<ResendDataMessage> ResendDataMessage::FromBytes(uint8_t *bytes) {
  uint64_t *fbs_length = (uint64_t *)(bytes + kItemMetaHeaderSize);
  bytes += kItemHeaderSize;
  std::string inputpb(reinterpret_cast<char const *>(bytes), *fbs_length);
  queue::protobuf::StreamingQueueResendDataMsg message;
  message.ParseFromString(inputpb);
  ActorID src_actor_id = ActorID::FromBinary(message.common().src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.common().dst_actor_id());
  ObjectID queue_id = ObjectID::FromBinary(message.common().queue_id());
  uint64_t first_seq_id = message.first_seq_id();
  uint64_t last_seq_id = message.last_seq_id();
  uint64_t seq_id = message.seq_id();
  uint64_t msg_id_start = message.msg_id_start();
  uint64_t msg_id_end = message.msg_id_end();
  uint64_t length = message.length();
  bool raw = message.raw();

  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id
                       << " first_seq_id:" << first_seq_id << " seq_id:" << seq_id
                       << " msg_id_start: " << msg_id_start
                       << " msg_id_end: " << msg_id_end << " last_seq_id:" << last_seq_id
                       << " queue_id:" << queue_id << " length:" << length;

  bytes += *fbs_length;
  /// COPY
  std::shared_ptr<LocalMemoryBuffer> buffer =
      std::make_shared<LocalMemoryBuffer>(bytes, (size_t)length, true);
  std::shared_ptr<ResendDataMessage> pull_data_msg = std::make_shared<ResendDataMessage>(
      src_actor_id, dst_actor_id, queue_id, first_seq_id, seq_id, msg_id_start,
      msg_id_end, last_seq_id, buffer, raw);

  return pull_data_msg;
}

void TestInitMessage::ToProtobuf(std::string *output) {
  queue::protobuf::StreamingQueueTestInitMsg msg;
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

std::shared_ptr<TestInitMessage> TestInitMessage::FromBytes(uint8_t *bytes) {
  uint64_t *length = (uint64_t *)(bytes + kItemMetaHeaderSize);
  bytes += kItemHeaderSize;
  std::string inputpb(reinterpret_cast<char const *>(bytes), *length);
  queue::protobuf::StreamingQueueTestInitMsg message;
  message.ParseFromString(inputpb);
  queue::protobuf::StreamingQueueTestRole role = message.role();
  ActorID src_actor_id = ActorID::FromBinary(message.src_actor_id());
  ActorID dst_actor_id = ActorID::FromBinary(message.dst_actor_id());
  std::string actor_handle_serialized = message.actor_handle();
  std::vector<ObjectID> queue_ids;
  for (int i = 0; i < message.queue_ids_size(); i++) {
    queue_ids.push_back(ObjectID::FromBinary(message.queue_ids(i)));
  }
  std::vector<ObjectID> rescale_queue_ids;
  for (int i = 0; i < message.rescale_queue_ids_size(); i++) {
    rescale_queue_ids.push_back(ObjectID::FromBinary(message.rescale_queue_ids(i)));
  }
  std::string test_suite_name = message.test_suite_name();
  std::string test_name = message.test_name();
  uint64_t param = message.param();

  std::shared_ptr<TestInitMessage> test_init_msg = std::make_shared<TestInitMessage>(
      role, src_actor_id, dst_actor_id, actor_handle_serialized, queue_ids,
      rescale_queue_ids, test_suite_name, test_name, param);

  return test_init_msg;
}

void TestCheckStatusRspMsg::ToProtobuf(std::string *output) {
  queue::protobuf::StreamingQueueTestCheckStatusRspMsg msg;
  msg.set_test_name(test_name_);
  msg.set_status(status_);
  msg.SerializeToString(output);
}

std::shared_ptr<TestCheckStatusRspMsg> TestCheckStatusRspMsg::FromBytes(uint8_t *bytes) {
  uint64_t *length = (uint64_t *)(bytes + kItemMetaHeaderSize);
  bytes += kItemHeaderSize;
  std::string inputpb(reinterpret_cast<char const *>(bytes), *length);
  queue::protobuf::StreamingQueueTestCheckStatusRspMsg message;
  message.ParseFromString(inputpb);
  std::string test_name = message.test_name();
  bool status = message.status();

  std::shared_ptr<TestCheckStatusRspMsg> test_check_msg =
      std::make_shared<TestCheckStatusRspMsg>(test_name, status);

  return test_check_msg;
}
}  // namespace streaming
}  // namespace ray
