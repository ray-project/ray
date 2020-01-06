#ifndef RAY_MESSAGE_BUNDLE_H
#define RAY_MESSAGE_BUNDLE_H

#include <ctime>
#include <list>
#include <numeric>

#include "message.h"

namespace ray {
namespace streaming {

enum class StreamingMessageBundleType : uint32_t {
  Empty = 1,
  Barrier = 2,
  Bundle = 3,
  MIN = Empty,
  MAX = Bundle
};

class StreamingMessageBundleMeta;
class StreamingMessageBundle;

typedef std::shared_ptr<StreamingMessageBundle> StreamingMessageBundlePtr;
typedef std::shared_ptr<StreamingMessageBundleMeta> StreamingMessageBundleMetaPtr;

constexpr uint32_t kMessageBundleMetaHeaderSize = sizeof(uint32_t) + sizeof(uint32_t) +
                                                  sizeof(uint64_t) + sizeof(uint64_t) +
                                                  sizeof(StreamingMessageBundleType);

constexpr uint32_t kMessageBundleHeaderSize =
    kMessageBundleMetaHeaderSize + sizeof(uint32_t);

class StreamingMessageBundleMeta {
 public:
  static const uint32_t StreamingMessageBundleMagicNum = 0xCAFEBABA;

 protected:
  uint64_t message_bundle_ts_;

  uint64_t last_message_id_;

  uint32_t message_list_size_;

  StreamingMessageBundleType bundle_type_;

 private:
  /// To speed up memory copy and serilization, we use memory layout of compiler related
  /// member variables. It's must be modified if any field is going to be inserted before
  /// first member property.
  /// Reference
  /// :/http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p1113r0.html#2254).
  inline uint8_t *GetFirstMemberAddress() {
    return reinterpret_cast<uint8_t *>(&message_bundle_ts_);
  }

 public:
  explicit StreamingMessageBundleMeta(const uint8_t *bytes);

  explicit StreamingMessageBundleMeta(const uint64_t message_bunddle_tes,
                                      const uint64_t last_offset_seq_id,
                                      const uint32_t message_list_size,
                                      const StreamingMessageBundleType bundle_type);

  explicit StreamingMessageBundleMeta(StreamingMessageBundleMeta *);

  explicit StreamingMessageBundleMeta();

  virtual ~StreamingMessageBundleMeta() = default;

  bool operator==(StreamingMessageBundleMeta &) const;

  bool operator==(StreamingMessageBundleMeta *) const;

  inline uint64_t GetMessageBundleTs() const { return message_bundle_ts_; }

  inline uint64_t GetLastMessageId() const { return last_message_id_; }

  inline uint32_t GetMessageListSize() const { return message_list_size_; }

  inline StreamingMessageBundleType GetBundleType() const { return bundle_type_; }

  inline bool IsBarrier() { return StreamingMessageBundleType::Barrier == bundle_type_; }
  inline bool IsBundle() { return StreamingMessageBundleType::Bundle == bundle_type_; }

  virtual void ToBytes(uint8_t *data);
  static StreamingMessageBundleMetaPtr FromBytes(const uint8_t *data,
                                                 bool verifer_check = true);
  inline virtual uint32_t ClassBytesSize() { return kMessageBundleMetaHeaderSize; }

  inline static bool CheckBundleMagicNum(const uint8_t *bytes) {
    const uint32_t *magic_num = reinterpret_cast<const uint32_t *>(bytes);
    return *magic_num == StreamingMessageBundleMagicNum;
  }

  std::string ToString() {
    return std::to_string(last_message_id_) + "," + std::to_string(message_list_size_) +
           "," + std::to_string(message_bundle_ts_) + "," +
           std::to_string(static_cast<uint32_t>(bundle_type_));
  }
};

/// StreamingMessageBundle inherits from metadata class (StreamingMessageBundleMeta)
/// with the following protocol: MagicNum = 0xcafebaba Timestamp 64bits timestamp
/// (milliseconds from 1970) LastMessageId( the last id of bundle) (0,INF]
/// MessageListSize(bundle len of message)
/// BundleType(a. bundle = 3 , b. barrier =2, c. empty = 1)
/// RawBundleSize（binary length of data)
/// RawData ( binary data)
///
///  +--------------------+
///  | MagicNum=U32       |
///  +--------------------+
///  | BundleTs=U64       |
///  +--------------------+
///  | LastMessageId=U64  |
///  +--------------------+
///  | MessageListSize=U32|
///  +--------------------+
///  | BundleType=U32     |
///  +--------------------+
///  | RawBundleSize=U32  |
///  +--------------------+
///  | RawData=var(N*Msg) |
///  +--------------------+
/// It should be noted that StreamingMessageBundle and StreamingMessageBundleMeta share
/// almost same protocol but the last two fields (RawBundleSize and RawData).
class StreamingMessageBundle : public StreamingMessageBundleMeta {
 private:
  uint32_t raw_bundle_size_;

  // Lazy serlization/deserlization.
  std::list<StreamingMessagePtr> message_list_;

 public:
  explicit StreamingMessageBundle(std::list<StreamingMessagePtr> &&message_list,
                                  uint64_t bundle_ts, uint64_t offset,
                                  StreamingMessageBundleType bundle_type,
                                  uint32_t raw_data_size = 0);

  // Duplicated copy if left reference in constructor.
  explicit StreamingMessageBundle(std::list<StreamingMessagePtr> &message_list,
                                  uint64_t bundle_ts, uint64_t offset,
                                  StreamingMessageBundleType bundle_type,
                                  uint32_t raw_data_size = 0);

  // New a empty bundle by passing last message id and timestamp.
  explicit StreamingMessageBundle(uint64_t, uint64_t);

  explicit StreamingMessageBundle(StreamingMessageBundle &bundle);

  virtual ~StreamingMessageBundle() = default;

  inline uint32_t GetRawBundleSize() const { return raw_bundle_size_; }

  bool operator==(StreamingMessageBundle &bundle) const;

  bool operator==(StreamingMessageBundle *bundle_ptr) const;

  void GetMessageList(std::list<StreamingMessagePtr> &message_list);

  const std::list<StreamingMessagePtr> &GetMessageList() const { return message_list_; }

  virtual void ToBytes(uint8_t *data);
  static StreamingMessageBundlePtr FromBytes(const uint8_t *data,
                                             bool verifer_check = true);
  inline virtual uint32_t ClassBytesSize() {
    return kMessageBundleHeaderSize + raw_bundle_size_;
  };

  static void GetMessageListFromRawData(const uint8_t *bytes, uint32_t bytes_size,
                                        uint32_t message_list_size,
                                        std::list<StreamingMessagePtr> &message_list);

  static void ConvertMessageListToRawData(
      const std::list<StreamingMessagePtr> &message_list, uint32_t raw_data_size,
      uint8_t *raw_data);
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_MESSAGE_BUNDLE_H
