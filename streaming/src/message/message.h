#pragma once

#include <cstring>
#include <memory>

namespace ray {
namespace streaming {

class StreamingMessage;

typedef std::shared_ptr<StreamingMessage> StreamingMessagePtr;

enum class StreamingMessageType : uint32_t {
  Barrier = 1,
  Message = 2,
  MIN = Barrier,
  MAX = Message
};

enum class StreamingBarrierType : uint32_t { GlobalBarrier = 0 };

struct StreamingBarrierHeader {
  StreamingBarrierType barrier_type;
  uint64_t barrier_id;
  StreamingBarrierHeader() = default;
  StreamingBarrierHeader(StreamingBarrierType barrier_type, uint64_t barrier_id) {
    this->barrier_type = barrier_type;
    this->barrier_id = barrier_id;
  }
  inline bool IsGlobalBarrier() {
    return StreamingBarrierType::GlobalBarrier == barrier_type;
  }
};

constexpr uint32_t kMessageHeaderSize =
    sizeof(uint32_t) + sizeof(uint64_t) + sizeof(StreamingMessageType);

constexpr uint32_t kBarrierHeaderSize = sizeof(StreamingBarrierType) + sizeof(uint64_t);

/// All messages should be wrapped by this protocol.
//  DataSize means length of raw data, message id is increasing from [1, +INF].
//  MessageType will be used for barrier transporting and checkpoint.
///      +----------------+
///      | PayloadSize=U32|
///      +----------------+
///      | MessageId=U64  |
///      +----------------+
///      | MessageType=U32|
///      +----------------+
///      | Payload=var    |
///      +----------------+
/// Payload field contains barrier header and carried buffer if message type is
/// global/partial barrier.
///
/// Barrier's Payload field:
///      +----------------------------+
///      | StreamingBarrierType=U32   |
///      +----------------------------+
///      | barrier_id=U64             |
///      +----------------------------+
///      | carried_buffer=var         |
///      +----------------------------+

class StreamingMessage {
 private:
  std::shared_ptr<uint8_t> payload_;
  uint32_t payload_size_;
  StreamingMessageType message_type_;
  uint64_t message_id_;

 public:
  /// Copy raw data from outside shared buffer.
  /// \param payload_ raw data from user buffer
  /// \param payload_size_ raw data size
  /// \param msg_id message id
  /// \param message_type
  StreamingMessage(std::shared_ptr<uint8_t> &payload_data, uint32_t payload_size,
                   uint64_t msg_id, StreamingMessageType message_type);

  /// Move outsite raw data to message data.
  /// \param payload_ raw data from user buffer
  /// \param payload_size_ raw data size
  /// \param msg_id message id
  /// \param message_type
  StreamingMessage(std::shared_ptr<uint8_t> &&payload_data, uint32_t payload_size,
                   uint64_t msg_id, StreamingMessageType message_type);

  /// Copy raw data from outside buffer.
  /// \param payload_ raw data from user buffer
  /// \param payload_size_ raw data size
  /// \param msg_id message id
  /// \param message_type
  StreamingMessage(const uint8_t *payload_data, uint32_t payload_size, uint64_t msg_id,
                   StreamingMessageType message_type);

  StreamingMessage(const StreamingMessage &);

  StreamingMessage operator=(const StreamingMessage &) = delete;

  virtual ~StreamingMessage() = default;

  inline StreamingMessageType GetMessageType() const { return message_type_; }
  inline uint64_t GetMessageId() const { return message_id_; }

  inline uint8_t *Payload() const { return payload_.get(); }

  inline uint32_t PayloadSize() const { return payload_size_; }

  inline bool IsMessage() { return StreamingMessageType::Message == message_type_; }
  inline bool IsBarrier() { return StreamingMessageType::Barrier == message_type_; }

  bool operator==(const StreamingMessage &) const;

  static inline std::shared_ptr<uint8_t> MakeBarrierPayload(
      StreamingBarrierHeader &barrier_header, const uint8_t *data, uint32_t data_size) {
    std::shared_ptr<uint8_t> ptr(new uint8_t[data_size + kBarrierHeaderSize],
                                 std::default_delete<uint8_t[]>());
    std::memcpy(ptr.get(), &barrier_header.barrier_type, sizeof(StreamingBarrierType));
    std::memcpy(ptr.get() + sizeof(StreamingBarrierType), &barrier_header.barrier_id,
                sizeof(uint64_t));
    if (data && data_size > 0) {
      std::memcpy(ptr.get() + kBarrierHeaderSize, data, data_size);
    }
    return ptr;
  }

  virtual void ToBytes(uint8_t *data);
  static StreamingMessagePtr FromBytes(const uint8_t *data, bool verifer_check = true);

  inline virtual uint32_t ClassBytesSize() { return kMessageHeaderSize + payload_size_; }

  static inline void GetBarrierIdFromRawData(const uint8_t *data,
                                             StreamingBarrierHeader *barrier_header) {
    barrier_header->barrier_type = *reinterpret_cast<const StreamingBarrierType *>(data);
    barrier_header->barrier_id =
        *reinterpret_cast<const uint64_t *>(data + sizeof(StreamingBarrierType));
  }

  friend std::ostream &operator<<(std::ostream &os, const StreamingMessage &message);
};

}  // namespace streaming
}  // namespace ray
