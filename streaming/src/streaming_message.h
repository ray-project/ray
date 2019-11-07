#ifndef RAY_STREAMING_MESSAGE_H
#define RAY_STREAMING_MESSAGE_H

#include <memory>

#include "streaming_serializable.h"

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

/*
        +----------------+
        | DataSize=U32   |
        +----------------+
        | MessageId=U64  |
        +----------------+
        | MessageType=U32|
        +----------------+
        | Data=var       |
        +----------------+
  Data contains barrier header and carried buffer if message type is
  global/partial barrier.
*/

class StreamingMessage : public StreamingSerializable {
 private:
  std::shared_ptr<uint8_t> message_data_;
  uint32_t data_size_;
  StreamingMessageType message_type_;
  uint64_t message_id_;

 public:
  /*!
   * @brief
   * @param data raw data from user buffer
   * @param data_size raw data size
   * @param seq_id message id
   * @param message_type
   */
  StreamingMessage(std::shared_ptr<uint8_t> &data, uint32_t data_size, uint64_t seq_id,
                   StreamingMessageType message_type);

  StreamingMessage(std::shared_ptr<uint8_t> &&data, uint32_t data_size, uint64_t seq_id,
                   StreamingMessageType message_type);

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

  STREAMING_SERIALIZATION
  STREAMING_DESERIALIZATION(StreamingMessagePtr)

  STREAMING_SERIALIZATION_LENGTH { return kMessageHeaderSize + data_size_; };
};

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_MESSAGE_H
