#pragma once

#include <cstdlib>
#include <functional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "channel/channel.h"
#include "message/message_bundle.h"
#include "message/priority_queue.h"
#include "reliability/barrier_helper.h"
#include "reliability_helper.h"
#include "runtime_context.h"

namespace ray {
namespace streaming {

class ReliabilityHelper;
class AtLeastOnceHelper;

enum class BundleCheckStatus : uint32_t {
  OkBundle = 0,
  BundleToBeThrown = 1,
  BundleToBeSplit = 2
};

static inline std::ostream &operator<<(std::ostream &os,
                                       const BundleCheckStatus &status) {
  os << static_cast<std::underlying_type<BundleCheckStatus>::type>(status);
  return os;
}

/// This is implementation of merger policy in StreamingReaderMsgPtrComparator.
struct StreamingReaderMsgPtrComparator {
  explicit StreamingReaderMsgPtrComparator(ReliabilityLevel strategy)
      : comp_strategy(strategy){};
  StreamingReaderMsgPtrComparator(){};
  ReliabilityLevel comp_strategy = ReliabilityLevel::EXACTLY_ONCE;

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

  std::unordered_map<uint64_t, uint32_t> global_barrier_cnt_;

  int64_t timer_interval_;
  int64_t last_bundle_ts_;
  int64_t last_message_ts_;
  int64_t last_message_latency_;
  int64_t last_bundle_unit_;

  ObjectID last_read_q_id_;

  static const uint32_t kReadItemTimeout;
  StreamingBarrierHelper barrier_helper_;
  std::shared_ptr<ReliabilityHelper> reliability_helper_;
  std::unordered_map<ObjectID, uint64_t> last_message_id_;

  friend class ReliabilityHelper;
  friend class AtLeastOnceHelper;

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
  ///  \param init_params
  ///  \param msg_ids
  ///  \param[out] creation_status
  ///  \param timer_interval
  void Init(const std::vector<ObjectID> &input_ids,
            const std::vector<ChannelCreationParameter> &init_params,
            const std::vector<uint64_t> &msg_ids,
            std::vector<TransferCreationStatus> &creation_status, int64_t timer_interval);

  /// Create reader use msg_id=0, this method is public only for test, and users
  /// usuallly don't need it.
  ///  \param input_ids
  ///  \param init_params
  ///  \param timer_interval
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
  StreamingStatus InitChannel(std::vector<TransferCreationStatus> &creation_status);

  /// One item from every channel will be popped out, then collecting
  /// them to a merged queue. High prioprity items will be fetched one by one.
  /// When item pop from one channel where must produce new item for placeholder
  /// in merged queue.
  StreamingStatus InitChannelMerger(uint32_t timeout_ms);

  StreamingStatus StashNextMessageAndPop(std::shared_ptr<DataBundle> &message,
                                         uint32_t timeout_ms);

  StreamingStatus GetMessageFromChannel(ConsumerChannelInfo &channel_info,
                                        std::shared_ptr<DataBundle> &message,
                                        uint32_t timeout_ms, uint32_t wait_time_ms);

  /// Get top item from prioprity queue.
  StreamingStatus GetMergedMessageBundle(std::shared_ptr<DataBundle> &message,
                                         bool &is_valid_break, uint32_t timeout_ms);

  bool BarrierAlign(std::shared_ptr<DataBundle> &message);

  BundleCheckStatus CheckBundle(const std::shared_ptr<DataBundle> &message);

  static void SplitBundle(std::shared_ptr<DataBundle> &message, uint64_t last_msg_id);
};
}  // namespace streaming
}  // namespace ray
