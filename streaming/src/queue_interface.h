
#ifndef QUEUE_INTERFACE_H
#define QUEUE_INTERFACE_H

#include "plasma/client.h"
#include "plasma/common.h"
#include "ray/common/status.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/util.h"

#include <iterator>
#include <limits>

namespace ray {

const uint64_t QUEUE_INTERFACE_INVALID = std::numeric_limits<uint64_t>::max();

// convert plasma status to ray status
ray::Status ConvertStatus(const arrow::Status &status);

inline void ConvertToValidQueueId(const ObjectID &queue_id) {
  auto addr = const_cast<ObjectID *>(&queue_id);
  *(reinterpret_cast<uint64_t *>(addr)) = 0;
}

// Ray queue should implement this interface
class QueueWriterInterface {
 public:
  QueueWriterInterface() {}
  virtual ~QueueWriterInterface() {}
  virtual Status CreateQueue(const ObjectID &queue_id, int64_t data_size,
                             bool reconstruct_queue = false, bool clear = false) = 0;
  virtual Status PushQueueItem(const ObjectID &queue_id, uint64_t seq_id, uint8_t *data,
                               uint32_t data_size, uint64_t timestamp) = 0;
  virtual bool IsQueueFoundInLocal(const ObjectID &queue_id,
                                   const int64_t timeout_ms = 0) = 0;
  virtual Status SetQueueEvictionLimit(const ObjectID &queue_id,
                                       uint64_t eviction_limit) = 0;
  virtual void PullQueueToLocal(const ObjectID &queue_id) = 0;
  virtual void WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids,
                                   int64_t timeout_ms,
                                   std::vector<ObjectID> &failed_queues) = 0;
  virtual void GetMinConsumedSeqID(const ObjectID &queue_id,
                                   uint64_t &min_consumed_id) = 0;
  virtual void GetUnconsumedBytes(const ObjectID &queue_id,
                                  uint32_t &unconsumed_bytes) = 0;
  virtual bool NotifyResubscribe(const ObjectID &queue_id) = 0;
  virtual void CleanupSubscription(const ObjectID &queue_id) = 0;
  virtual void GetLastQueueItem(const ObjectID &queue_id, std::shared_ptr<uint8_t> &data,
                                uint32_t &data_size, uint64_t &sequence_id) = 0;
  virtual Status DeleteQueue(const ObjectID &queue_id) = 0;
  virtual bool UsePull() = 0;
};

class QueueReaderInterface {
 public:
  QueueReaderInterface() {}
  virtual ~QueueReaderInterface() {}
  virtual bool GetQueue(const ObjectID &queue_id, int64_t timeout_ms,
                        uint64_t start_seq_id) = 0;
  virtual Status GetQueueItem(const ObjectID &object_id, uint8_t *&data,
                              uint32_t &data_size, uint64_t &seq_id,
                              uint64_t timeout_ms = -1) = 0;
  virtual void NotifyConsumedItem(const ObjectID &object_id, uint64_t seq_id) = 0;
  virtual void GetLastSeqID(const ObjectID &object_id, uint64_t &last_seq_id) = 0;

  virtual void WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids,
                                   int64_t timeout_ms,
                                   std::vector<ObjectID> &failed_queues) = 0;
  virtual Status DeleteQueue(const ObjectID &queue_id) = 0;
};

}  // namespace ray

#endif
