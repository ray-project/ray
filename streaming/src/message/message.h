#pragma once

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

constexpr uint32_t kMessageHeaderSize =
    sizeof(uint32_t) + sizeof(uint64_t) + sizeof(StreamingMessageType);

/// All messages should be wrapped by this protocol.
//  DataSize means length of raw data, message id is increasing from [1, +INF].
//  MessageType will be used for barrier transporting and checkpoint.
///      +----------------+
///      | DataSize=U32   |
///      +----------------+
///      | MessageId=U64  |
///      +----------------+
///      | MessageType=U32|
///      +----------------+
///      | Data=var       |
///      +----------------+

class StreamingMessage {
 private:
  std::shared_ptr<uint8_t> message_data_;
  uint32_t data_size_;
  StreamingMessageType message_type_;
  uint64_t message_id_;

 public:
  /// Copy raw data from outside shared buffer.
  /// \param data raw data from user buffer
  /// \param data_size raw data size
  /// \param seq_id message id
  /// \param message_type
  StreamingMessage(std::shared_ptr<uint8_t> &data, uint32_t data_size, uint64_t seq_id,
                   StreamingMessageType message_type);

  /// Move outsite raw data to message data.
  /// \param data raw data from user buffer
  /// \param data_size raw data size
  /// \param seq_id message id
  /// \param message_type
  StreamingMessage(std::shared_ptr<uint8_t> &&data, uint32_t data_size, uint64_t seq_id,
                   StreamingMessageType message_type);

  /// Copy raw data from outside buffer.
  /// \param data raw data from user buffer
  /// \param data_size raw data size
  /// \param seq_id message id
  /// \param message_type
  StreamingMessage(const uint8_t *data, uint32_t data_size, uint64_t seq_id,
                   StreamingMessageType message_type);

  StreamingMessage(const StreamingMessage &);

  StreamingMessage operator=(const StreamingMessage &) = delete;

  virtual ~StreamingMessage() = default;

  inline uint8_t *RawData() const { return message_data_.get(); }

  inline uint32_t GetDataSize() const { return data_size_; }
  inline StreamingMessageType GetMessageType() const { return message_type_; }
  inline uint64_t GetMessageSeqId() const { return message_id_; }
  inline bool IsMessage() { return StreamingMessageType::Message == message_type_; }
  inline bool IsBarrier() { return StreamingMessageType::Barrier == message_type_; }

  bool operator==(const StreamingMessage &) const;

  virtual void ToBytes(uint8_t *data);
  static StreamingMessagePtr FromBytes(const uint8_t *data, bool verifer_check = true);

  inline virtual uint32_t ClassBytesSize() { return kMessageHeaderSize + data_size_; }
};

}  // namespace streaming
}  // namespace ray
