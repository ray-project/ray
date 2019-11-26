#ifndef _STREAMING_QUEUE_TRANSPORT_H_
#define _STREAMING_QUEUE_TRANSPORT_H_

#include <condition_variable>
#include <mutex>
#include <queue>

#include "protobuf/streaming_queue.pb.h"
#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

/// base class of all command messages
class Message {
 public:
  Message(const ActorID &actor_id, const ActorID &peer_actor_id, const ObjectID &queue_id,
          std::shared_ptr<LocalMemoryBuffer> buffer = nullptr)
      : actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        queue_id_(queue_id),
        buffer_(buffer) {}
  Message() {}
  virtual ~Message() {}
  virtual ActorID ActorId() { return actor_id_; }
  virtual ActorID PeerActorId() { return peer_actor_id_; }
  virtual ObjectID QueueId() { return queue_id_; }
  virtual queue::protobuf::StreamingQueueMessageType Type() = 0;
  std::shared_ptr<LocalMemoryBuffer> Buffer() { return buffer_; }

  virtual std::unique_ptr<LocalMemoryBuffer> ToBytes();
  virtual void ToProtobuf(std::string *output) = 0;

 protected:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  ObjectID queue_id_;
  std::shared_ptr<LocalMemoryBuffer> buffer_;

 public:
  static const uint32_t MagicNum;
};

class DataMessage : public Message {
 public:
  DataMessage(const ActorID &actor_id, const ActorID &peer_actor_id, ObjectID queue_id,
              uint64_t seq_id, std::shared_ptr<LocalMemoryBuffer> buffer, bool raw)
      : Message(actor_id, peer_actor_id, queue_id, buffer), seq_id_(seq_id), raw_(raw) {}
  virtual ~DataMessage() {}

  static std::shared_ptr<DataMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);

  uint64_t SeqId() { return seq_id_; }
  bool IsRaw() { return raw_; }
  queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  uint64_t seq_id_;
  bool raw_;

  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueDataMsgType;
};

class NotificationMessage : public Message {
 public:
  NotificationMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id, uint64_t seq_id)
      : Message(actor_id, peer_actor_id, queue_id), seq_id_(seq_id) {}

  virtual ~NotificationMessage() {}

  static std::shared_ptr<NotificationMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);

  uint64_t SeqId() { return seq_id_; }
  queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  uint64_t seq_id_;
  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueNotificationMsgType;
};

class CheckMessage : public Message {
 public:
  CheckMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
               const ObjectID &queue_id)
      : Message(actor_id, peer_actor_id, queue_id) {}
  virtual ~CheckMessage() {}

  static std::shared_ptr<CheckMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);

  queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckMsgType;
};

class CheckRspMessage : public Message {
 public:
  CheckRspMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                  const ObjectID &queue_id, queue::protobuf::StreamingQueueError err_code)
      : Message(actor_id, peer_actor_id, queue_id), err_code_(err_code) {}
  virtual ~CheckRspMessage() {}

  static std::shared_ptr<CheckRspMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  queue::protobuf::StreamingQueueMessageType Type() { return type_; }
  queue::protobuf::StreamingQueueError Error() { return err_code_; }

 private:
  queue::protobuf::StreamingQueueError err_code_;
  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckRspMsgType;
};

class TestInitMessage : public Message {
 public:
  TestInitMessage(const queue::protobuf::StreamingQueueTestRole role,
              const ActorID &actor_id, const ActorID &peer_actor_id,
              const std::string actor_handle_serialized, 
              const std::vector<ObjectID> &queue_ids, const std::vector<ObjectID> &rescale_queue_ids,
              std::string test_suite_name, std::string test_name,
              uint64_t param)
      : Message(actor_id, peer_actor_id, queue_ids[0]),
  actor_handle_serialized_(actor_handle_serialized), queue_ids_(queue_ids), rescale_queue_ids_(rescale_queue_ids),
  role_(role), test_suite_name_(test_suite_name), test_name_(test_name), param_(param) {}
  virtual ~TestInitMessage() {}

  static std::shared_ptr<TestInitMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  queue::protobuf::StreamingQueueMessageType Type() { return type_; }
  std::string ActorHandleSerialized() { return actor_handle_serialized_; }
  queue::protobuf::StreamingQueueTestRole Role() { return role_; }
  std::vector<ObjectID> QueueIds() { return queue_ids_; }
  std::vector<ObjectID> RescaleQueueIds() { return rescale_queue_ids_; }
  std::string TestSuiteName() { return test_suite_name_; }
  std::string TestName() { return test_name_;}
  uint64_t Param() { return param_; }

  std::string ToString() {
    std::ostringstream os;
    os << "actor_handle_serialized: " << actor_handle_serialized_;
    os << " actor_id: " << ActorId();
    os << " peer_actor_id: " << PeerActorId();
    os << " queue_ids:[";
    for (auto &qid : queue_ids_) {
      os << qid << ",";
    }
    os << "], rescale_queue_ids:[";
    for (auto &qid : rescale_queue_ids_) {
      os << qid << ",";
    }
    os << "],";
    os << " role:" << queue::protobuf::StreamingQueueTestRole_Name(role_);
    os << " suite_name: " << test_suite_name_;
    os << " test_name: " << test_name_;
    os << " param: " << param_;
    return os.str();
  }
 private:
  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueTestInitMessageType;
  std::string actor_handle_serialized_;
  std::vector<ObjectID> queue_ids_;
  std::vector<ObjectID> rescale_queue_ids_;
  queue::protobuf::StreamingQueueTestRole role_;
  std::string test_suite_name_;
  std::string test_name_;
  uint64_t param_;
};

class TestCheckStatusRspMsg : public Message {
 public:
  TestCheckStatusRspMsg(const std::string test_name, bool status)
      : test_name_(test_name), status_(status) {}
  virtual ~TestCheckStatusRspMsg() {}

  static std::shared_ptr<TestCheckStatusRspMsg> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  queue::protobuf::StreamingQueueMessageType Type() { return type_; }
  std::string TestName() { return test_name_; }
  bool Status() { return status_; }

 private:
  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueTestCheckStatusRspMsgType;
  std::string test_name_;
  bool status_;
};

class Transport {
 public:
  Transport(CoreWorker* worker, const ActorID &actor_id)
      : core_worker_(worker),
        peer_actor_id_(actor_id) {
  }
  virtual ~Transport() = default;

  virtual void Destroy() {}

  virtual void Send(std::unique_ptr<LocalMemoryBuffer> buffer, RayFunction &function);
  virtual std::shared_ptr<LocalMemoryBuffer> SendForResult(
      std::shared_ptr<LocalMemoryBuffer> buffer, RayFunction &function, int64_t timeout_ms);
  std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::unique_ptr<LocalMemoryBuffer> buffer, RayFunction &function, int retry_cnt, int64_t timeout_ms);
  virtual std::shared_ptr<LocalMemoryBuffer> Recv();

 private:
  CoreWorker* core_worker_;
  ActorID peer_actor_id_;
};
}  // namespace streaming
}  // namespace ray
#endif
