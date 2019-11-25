#ifndef RAY_DATA_READER_H
#define RAY_DATA_READER_H

#include <cstdlib>
#include <functional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "message/message_bundle.h"
#include "message/priority_queue.h"
#include "runtime_context.h"
#include "transfer.h"

namespace ray {
namespace streaming {

struct DataBundle {
  // it's immutable data point
  uint8_t *data = nullptr;
  uint32_t data_size;
  ObjectID from;
  uint64_t seq_id;
  StreamingMessageBundleMetaPtr meta;
};

struct StreamingReaderMsgPtrComparator {
  StreamingReaderMsgPtrComparator()= default;;
  bool operator()(const std::shared_ptr<DataBundle> &a,
                  const std::shared_ptr<DataBundle> &b);
};

class DataReader {
 private:
  std::vector<ObjectID> input_queue_ids_;

  std::vector<ObjectID> unready_queue_ids_;

  std::unique_ptr<PriorityQueue<std::shared_ptr<DataBundle>,
                                StreamingReaderMsgPtrComparator>>
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
  ///  Init Streaming reader. For exception status throwing, we do not init
  ///  in constructor. Actually, There are only reigster somethings in this
  ///  function.
  ///  \param store_path
  ///  \param input_ids
  ///  \param queue_seq_ids
  ///  \param raylet_client
  ///
  void Init(const std::vector<ObjectID> &input_ids,
            const std::vector<uint64_t> &queue_seq_ids,
            const std::vector<uint64_t> &streaming_msg_ids, int64_t timer_interval);

  void Init(const std::vector<ObjectID> &input_ids, int64_t timer_interval);

  ///  Get latest message from input queues
  ///  \param timeout_ms
  ///  \param message, return the latest message
  StreamingStatus GetBundle(uint32_t timeout_ms,
                            std::shared_ptr<DataBundle> &message);

  ///  Get offset information about channels for checkpoint.
  ///  \param offset_map (return value)
  void GetOffsetInfo(std::unordered_map<ObjectID, ConsumerChannelInfo> *&offset_map);

  void Stop();

  explicit DataReader(std::shared_ptr<RuntimeContext> &runtime_context);
  virtual ~DataReader();

  ///  Notify input queues to clear data its seq id is equal or less than offset.
  ///  It's used when checkpoint is done.
  ///  \param channel_info
  ///  \param offset
  ///
  void NotifyConsumedItem(ConsumerChannelInfo &channel_info, uint64_t offset);

 private:
  /// One item from every channel will be popped out, then collecting
  /// them to a merged queue. High prioprity items will be fetched one by one.
  ///  When item pop from one channel where must produce new item for placeholder
  ///  in merged queue.
  StreamingStatus InitChannel();

  StreamingStatus InitChannelMerger();

  StreamingStatus StashNextMessage(std::shared_ptr<DataBundle> &message);

  StreamingStatus GetMessageFromChannel(ConsumerChannelInfo &channel_info,
                                        std::shared_ptr<DataBundle> &message);

  StreamingStatus GetMergedMessageBundle(std::shared_ptr<DataBundle> &message,
                                         bool &is_valid_break);
};

class DirectCallDataReader : public DataReader {
 public:
  DirectCallDataReader(std::shared_ptr<RuntimeContext> &runtime_context,
                       CoreWorker *core_worker, const std::vector<ObjectID> &queue_ids,
                       const std::vector<ActorID> &actor_ids, RayFunction async_func,
                       RayFunction sync_func)
      : DataReader(runtime_context), core_worker_(core_worker) {
    transfer_config_->Set(ConfigEnum::CORE_WORKER,
                          reinterpret_cast<uint64_t>(core_worker_));
    transfer_config_->Set(ConfigEnum::ASYNC_FUNCTION, async_func);
    transfer_config_->Set(ConfigEnum::SYNC_FUNCTION, sync_func);
    for (size_t i = 0; i < queue_ids.size(); ++i) {
      auto &q_id = queue_ids[i];
      channel_info_map_[q_id].actor_id = actor_ids[i];
    }
  }

  ~DirectCallDataReader() override { core_worker_ = nullptr; }

 private:
  CoreWorker *core_worker_;
};

}  // namespace streaming
}  // namespace ray
#endif  // RAY_DATA_READER_H
