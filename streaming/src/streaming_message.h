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

enum class StreamingBarrierType : uint32_t { GlobalBarrier = 0, PartialBarrier = 1 };

struct StreamingBarrierHeader {
  StreamingBarrierType barrier_type;
  uint64_t barrier_id;
  // It's -1 if it's global barrier;
  uint64_t partial_barrier_id;
  inline bool IsGlobalBarrier() {
    return StreamingBarrierType::GlobalBarrier == barrier_type;
  }
  inline bool IsPartialBarrier() {
    return StreamingBarrierType::PartialBarrier == barrier_type;
  }
};

constexpr uint32_t kMessageHeaderSize =
    sizeof(uint32_t) + sizeof(uint64_t) + sizeof(StreamingMessageType);

// constexpr uint32_t kBarrierHeaderSize = sizeof(StreamingBarrierHeader);
constexpr uint32_t kBarrierHeaderSize =
    sizeof(StreamingBarrierType) + sizeof(uint64_t) * 2;

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
  StreamingMessage(std::shared_ptr<uint8_t> data, uint32_t data_size, uint64_t seq_id,
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

  static inline std::shared_ptr<uint8_t> MakeBarrierMessage(
      StreamingBarrierHeader &barrier_header, const uint8_t *data, uint32_t data_size) {
    std::shared_ptr<uint8_t> ptr(new uint8_t[data_size + kBarrierHeaderSize],
                                 std::default_delete<uint8_t[]>());
    std::memcpy(ptr.get(), &barrier_header.barrier_type, sizeof(StreamingBarrierType));
    std::memcpy(ptr.get() + sizeof(StreamingBarrierType), &barrier_header.barrier_id,
                sizeof(uint64_t));
    if (barrier_header.IsGlobalBarrier()) {
      barrier_header.partial_barrier_id = -1;
    }
    std::memcpy(ptr.get() + sizeof(StreamingBarrierType) + sizeof(uint64_t),
                &barrier_header.partial_barrier_id, sizeof(uint64_t));
    std::memcpy(ptr.get() + kBarrierHeaderSize, data, data_size);
    return ptr;
  }

  static inline void GetBarrierIdFromRawData(const uint8_t *data,
                                             StreamingBarrierHeader *barrier_header) {
    barrier_header->barrier_type = *reinterpret_cast<const StreamingBarrierType *>(data);
    barrier_header->barrier_id =
        *reinterpret_cast<const uint64_t *>(data + sizeof(StreamingBarrierType));
    barrier_header->partial_barrier_id = *reinterpret_cast<const uint64_t *>(
        data + sizeof(StreamingBarrierType) + sizeof(uint64_t));
  }
};

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_MESSAGE_H
