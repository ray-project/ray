#ifndef QUEUE_INTERFACE_H
#define QUEUE_INTERFACE_H

#include "ray/common/status.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/util.h"
#include "queue/queue_manager.h"

#include <iterator>
#include <limits>

namespace ray {
namespace streaming {

class StreamingQueueWriter;
class StreamingQueueReader;

std::shared_ptr<StreamingQueueWriter> CreateQueueWriter(
    const JobID &job_id,
    const std::vector<ObjectID> &queue_ids, CoreWorker *core_worker,
    ray::RayFunction &async_func, ray::RayFunction &sync_func);

std::shared_ptr<StreamingQueueReader> CreateQueueReader(
    const JobID &job_id,
    const std::vector<ObjectID> &queue_ids, CoreWorker *core_worker,
    ray::RayFunction &async_func, ray::RayFunction &sync_func);

inline void ConvertToValidQueueId(const ObjectID &queue_id) {
  auto addr = const_cast<ObjectID *>(&queue_id);
  *(reinterpret_cast<uint64_t *>(addr)) = 0;
}

/// code below is interface implementation of streaming queue
class StreamingQueueWriter {
 public:
  StreamingQueueWriter(CoreWorker *core_worker, RayFunction &async_func,
                       RayFunction &sync_func);
  ~StreamingQueueWriter() {}

  void CreateQueue(const ObjectID &queue_id, int64_t data_size,
                     ActorID &actor_handle);
  Status SetQueueEvictionLimit(const ObjectID &queue_id, uint64_t eviction_limit);

  void WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                           std::vector<ObjectID> &failed_queues);
  void GetMinConsumedSeqID(const ObjectID &queue_id, uint64_t &min_consumed_id);
  Status PushQueueItem(const ObjectID &queue_id, uint64_t seq_id, uint8_t *data,
                       uint32_t data_size, uint64_t timestamp);
  Status DeleteQueue(const ObjectID &queue_id);

 private:
  std::shared_ptr<ray::streaming::QueueManager> queue_manager_;
  ActorID actor_id_;

  CoreWorker *core_worker_;
  RayFunction async_func_;
  RayFunction sync_func_;
};

class StreamingQueueReader {
 public:
  StreamingQueueReader(CoreWorker *core_worker, RayFunction &async_func,
                       RayFunction &sync_func);
  ~StreamingQueueReader() {}

  bool GetQueue(const ObjectID &queue_id, int64_t timeout_ms, uint64_t start_seq_id,
                ActorID &actor_handle);
  Status GetQueueItem(const ObjectID &queue_id, uint8_t *&data, uint32_t &data_size,
                      uint64_t &seq_id, uint64_t timeout_ms = -1);
  void NotifyConsumedItem(const ObjectID &queue_id, uint64_t seq_id);
  void WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                           std::vector<ObjectID> &failed_queues);
  Status DeleteQueue(const ObjectID &queue_id);

 private:
  std::shared_ptr<ray::streaming::QueueManager> queue_manager_;
  ActorID actor_id_;
  CoreWorker *core_worker_;
  RayFunction async_func_;
  RayFunction sync_func_;
};

}  // namespace streaming
}  // namespace ray

#endif
