#ifndef RAY_STREAMING_READER_H
#define RAY_STREAMING_READER_H

#include <cstdlib>
#include <functional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "streaming.h"
#include "streaming_message_bundle.h"
#include "streaming_message_merger.h"
#include "streaming_transfer.h"

namespace ray {
namespace streaming {

struct StreamingReaderBundle {
  // it's immutable data point
  uint8_t *data = nullptr;
  uint32_t data_size;
  ObjectID from;
  uint64_t seq_id;
  uint64_t last_barrier_id = 0;
  uint64_t last_partial_barrier_id = 0;
  StreamingMessageBundleMetaPtr meta;
};

struct StreamingReaderMsgPtrComparator {
  StreamingReaderMsgPtrComparator(StreamingStrategy strategy) : comp_strategy(strategy){};
  StreamingReaderMsgPtrComparator(){};
  StreamingStrategy comp_strategy = StreamingStrategy::EXACTLY_ONCE;

  bool operator()(const std::shared_ptr<StreamingReaderBundle> &a,
                  const std::shared_ptr<StreamingReaderBundle> &b);
};

class StreamingReader : public StreamingCommon {
 private:
  std::shared_ptr<ConsumerTransfer> transfer_;
  std::vector<ObjectID> input_queue_ids_;

  std::vector<ObjectID> unready_queue_ids_;

  std::unique_ptr<StreamingMessageMerger<std::shared_ptr<StreamingReaderBundle>,
                                         StreamingReaderMsgPtrComparator>>
      reader_merger_;

  std::unordered_map<uint64_t, uint32_t> barrier_cnt_;
  std::unordered_map<uint64_t, uint32_t> partial_barrier_cnt_;
  std::shared_ptr<StreamingReaderBundle> last_fetched_queue_item_;

  bool is_recreate_;
  int64_t timer_interval_;
  int64_t last_bundle_ts_;
  int64_t last_message_ts_;
  int64_t last_message_latency_;
  int64_t last_bundle_unit_;
  std::unordered_map<ObjectID, ConsumerChannelInfo> channel_info_map_;

  ObjectID last_read_q_id_;

  static const uint32_t kReadItemTimeout;

 public:
  /*!
   * init Streaming reader
   * @param store_path
   * @param input_ids
   * @param plasma_queue_seq_ids
   * @param raylet_client
   */
  void Init(const std::string &store_path, const std::vector<ObjectID> &input_ids,
            const std::vector<uint64_t> &plasma_queue_seq_ids,
            const std::vector<uint64_t> &streaming_msg_ids, int64_t timer_interval,
            bool is_recreate, std::vector<ObjectID> &abnormal_queues);

  void Init(const std::string &store_path, const std::vector<ObjectID> &input_ids,
            int64_t timer_interval, bool is_rescale = false);
  /*!
   * get latest message from input queues
   * @param timeout_ms
   * @param offset_seq_id, return offsets in plasma seq_id of each input queue, in
   * unordered_map
   * @param offset_msg_id, return offsets in streaming msg_id of each input queue, in
   * unordered_map
   * @param msg, return the latest message
   */
  StreamingStatus GetBundle(const uint32_t timeout_ms,
                            std::shared_ptr<StreamingReaderBundle> &message);

  void Stop();

  virtual ~StreamingReader();

 private:
  StreamingStatus InitChannel(std::vector<ObjectID> &abnormal_Channel);

  StreamingStatus InitChannelMerger();

  StreamingStatus StashNextMessage(std::shared_ptr<StreamingReaderBundle> &message);

  StreamingStatus GetMessageFromChannel(ConsumerChannelInfo &channel_info,
                                        std::shared_ptr<StreamingReaderBundle> &message);

  StreamingStatus GetMergedMessageBundle(std::shared_ptr<StreamingReaderBundle> &message,
                                         bool &is_valid_break);

  bool BarrierAlign(std::shared_ptr<StreamingReaderBundle> &message);
};

}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_READER_H
