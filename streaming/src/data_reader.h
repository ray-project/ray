#pragma once

#include <cstdlib>
#include <functional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "channel.h"
#include "message/message_bundle.h"
#include "message/priority_queue.h"
#include "runtime_context.h"

namespace ray {
namespace streaming {

/// Databundle is super-bundle that contains channel information (upstream
/// channel id & bundle meta data) and raw buffer pointer.
struct DataBundle {
  uint8_t *data = nullptr;
  uint32_t data_size;
  ObjectID from;
  uint64_t seq_id;
  StreamingMessageBundleMetaPtr meta;
};

/// This is implementation of merger policy in StreamingReaderMsgPtrComparator.
struct StreamingReaderMsgPtrComparator {
  StreamingReaderMsgPtrComparator() = default;
  bool operator()(const std::shared_ptr<DataBundle> &a,
                  const std::shared_ptr<DataBundle> &b);
};

/// DataReader will fetch data bundles from channels of upstream workers, once
/// invoked by user thread. Firstly put them into a priority queue ordered by bundle
/// comparator that's related meta-data, then pop out the top bunlde to user
/// thread every time, so that the order of the message can be guranteed, which
/// will also facilitate our future implementation of fault tolerance. Finally
/// user thread can extract messages from the bundle and process one by one.
class DataReader {
 private:
  std::vector<ObjectID> input_queue_ids_;

  std::vector<ObjectID> unready_queue_ids_;

  std::unique_ptr<
      PriorityQueue<std::shared_ptr<DataBundle>, StreamingReaderMsgPtrComparator>>
      reader_merger_;

  std::shared_ptr<DataBundle> last_fetched_queue_item_;

  int64_t timer_interval_;
  int64_t last_bundle_ts_;
  int64_t last_message_ts_;
  int64_t last_message_latency_;
  int64_t last_bundle_unit_;

  ObjectID last_read_q_id_;

  static const uint32_t kReadItemTimeout;

 protected:
  std::unordered_map<ObjectID, ConsumerChannelInfo> channel_info_map_;
  std::unordered_map<ObjectID, std::shared_ptr<ConsumerChannel>> channel_map_;
  std::shared_ptr<Config> transfer_config_;
  std::shared_ptr<RuntimeContext> runtime_context_;

 public:
  explicit DataReader(std::shared_ptr<RuntimeContext> &runtime_context);
  virtual ~DataReader();

  /// During initialization, only the channel parameters and necessary member properties
  /// are assigned. All channels will be connected in the first reading operation.
  ///  \param input_ids
  ///  \param actor_ids
  ///  \param channel_seq_ids
  ///  \param msg_ids
  ///  \param timer_interval
  void Init(const std::vector<ObjectID> &input_ids,
            const std::vector<ChannelCreationParameter> &init_params,
            const std::vector<uint64_t> &channel_seq_ids,
            const std::vector<uint64_t> &msg_ids, int64_t timer_interval);

  void Init(const std::vector<ObjectID> &input_ids,
            const std::vector<ChannelCreationParameter> &init_params,
            int64_t timer_interval);

  /// Get latest message from input queues.
  ///  \param timeout_ms
  ///  \param message, return the latest message
  StreamingStatus GetBundle(uint32_t timeout_ms, std::shared_ptr<DataBundle> &message);

  /// Get offset information about channels for checkpoint.
  ///  \param offset_map (return value)
  void GetOffsetInfo(std::unordered_map<ObjectID, ConsumerChannelInfo> *&offset_map);

  void Stop();

  /// Notify input queues to clear data whose seq id is equal or less than offset.
  /// It's used when checkpoint is done.
  ///  \param channel_info consumer's channel info
  ///  \param offset consumed channel offset
  void NotifyConsumedItem(ConsumerChannelInfo &channel_info, uint64_t offset);

  //// Notify message related channel to clear data.
  void NotifyConsumed(std::shared_ptr<DataBundle> &message);

 private:
  /// Create channels and connect to all upstream.
  StreamingStatus InitChannel();

  /// One item from every channel will be popped out, then collecting
  /// them to a merged queue. High prioprity items will be fetched one by one.
  /// When item pop from one channel where must produce new item for placeholder
  /// in merged queue.
  StreamingStatus InitChannelMerger();

  StreamingStatus StashNextMessage(std::shared_ptr<DataBundle> &message);

  StreamingStatus GetMessageFromChannel(ConsumerChannelInfo &channel_info,
                                        std::shared_ptr<DataBundle> &message);

  /// Get top item from prioprity queue.
  StreamingStatus GetMergedMessageBundle(std::shared_ptr<DataBundle> &message,
                                         bool &is_valid_break);
};
}  // namespace streaming
}  // namespace ray
