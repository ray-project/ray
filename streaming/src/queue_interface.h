#ifndef QUEUE_INTERFACE_H
#define QUEUE_INTERFACE_H

#include "ray/common/status.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/util.h"
#include "queue/queue_manager.h"

#include <iterator>
#include <limits>

namespace ray {

const uint64_t QUEUE_INTERFACE_INVALID = std::numeric_limits<uint64_t>::max();

class QueueWriterInterface;
class QueueReaderInterface;

std::shared_ptr<QueueWriterInterface> CreateQueueWriter(
    const JobID &job_id,
    const std::vector<ObjectID> &queue_ids, CoreWorker *core_worker,
    ray::RayFunction &async_func, ray::RayFunction &sync_func);

std::shared_ptr<QueueReaderInterface> CreateQueueReader(
    const JobID &job_id,
    const std::vector<ObjectID> &queue_ids, CoreWorker *core_worker,
    ray::RayFunction &async_func, ray::RayFunction &sync_func);

inline void ConvertToValidQueueId(const ObjectID &queue_id) {
  auto addr = const_cast<ObjectID *>(&queue_id);
  *(reinterpret_cast<uint64_t *>(addr)) = 0;
}

// Ray queue should implement this interface
class QueueWriterInterface {
 public:
  QueueWriterInterface() {}
  virtual ~QueueWriterInterface() {}
  virtual Status CreateQueue(const ObjectID &queue_id, int64_t data_size, ActorID &actor_handle) = 0;
  virtual Status PushQueueItem(const ObjectID &queue_id, uint64_t seq_id, uint8_t *data,
                               uint32_t data_size, uint64_t timestamp) = 0;
  virtual bool IsQueueFoundInLocal(const ObjectID &queue_id,
                                   const int64_t timeout_ms = 0) = 0;
  virtual Status SetQueueEvictionLimit(const ObjectID &queue_id,
                                       uint64_t eviction_limit) = 0;
  virtual void WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids,
                                   int64_t timeout_ms,
                                   std::vector<ObjectID> &failed_queues) = 0;
  virtual void GetMinConsumedSeqID(const ObjectID &queue_id,
                                   uint64_t &min_consumed_id) = 0;
  virtual void CleanupSubscription(const ObjectID &queue_id) = 0;
  virtual void GetLastQueueItem(const ObjectID &queue_id, std::shared_ptr<uint8_t> &data,
                                uint32_t &data_size, uint64_t &sequence_id) = 0;
  virtual Status DeleteQueue(const ObjectID &queue_id) = 0;
};

class QueueReaderInterface {
 public:
  QueueReaderInterface() {}
  virtual ~QueueReaderInterface() {}
  virtual bool GetQueue(const ObjectID &queue_id, int64_t timeout_ms,
                        uint64_t start_seq_id, ActorID &actor_id) = 0;
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

/***
 * code below is interface implementation of streaming queue
 ***/
class StreamingQueueWriter : public QueueWriterInterface {
 public:
  StreamingQueueWriter(CoreWorker *core_worker, RayFunction &async_func,
                       RayFunction &sync_func);
  ~StreamingQueueWriter() {}

  Status CreateQueue(const ObjectID &queue_id, int64_t data_size,
                     ActorID &actor_handle) override;
  bool IsQueueFoundInLocal(const ObjectID &queue_id, const int64_t timeout_ms) override;
  Status SetQueueEvictionLimit(const ObjectID &queue_id, uint64_t eviction_limit) override;

  void WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                           std::vector<ObjectID> &failed_queues) override;
  void GetMinConsumedSeqID(const ObjectID &queue_id, uint64_t &min_consumed_id) override;
  Status PushQueueItem(const ObjectID &queue_id, uint64_t seq_id, uint8_t *data,
                       uint32_t data_size, uint64_t timestamp) override;
  void CleanupSubscription(const ObjectID &queue_id) override;
  void GetLastQueueItem(const ObjectID &queue_id, std::shared_ptr<uint8_t> &data,
                        uint32_t &data_size, uint64_t &sequence_id) override;
  Status DeleteQueue(const ObjectID &queue_id) override;

 private:
  std::shared_ptr<ray::streaming::QueueManager> queue_manager_;
  std::shared_ptr<ray::streaming::QueueWriter> queue_writer_;
  ActorID actor_id_;

  CoreWorker *core_worker_;
  RayFunction async_func_;
  RayFunction sync_func_;
};

class StreamingQueueReader : public QueueReaderInterface {
 public:
  StreamingQueueReader(CoreWorker *core_worker, RayFunction &async_func,
                       RayFunction &sync_func);
  ~StreamingQueueReader() {}

  bool GetQueue(const ObjectID &queue_id, int64_t timeout_ms, uint64_t start_seq_id,
                ActorID &actor_handle) override;
  Status GetQueueItem(const ObjectID &object_id, uint8_t *&data, uint32_t &data_size,
                      uint64_t &seq_id, uint64_t timeout_ms = -1) override;
  void NotifyConsumedItem(const ObjectID &object_id, uint64_t seq_id) override;
  void GetLastSeqID(const ObjectID &object_id, uint64_t &last_seq_id) override;

  void WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                           std::vector<ObjectID> &failed_queues) override;
  Status DeleteQueue(const ObjectID &queue_id) override;

 private:
  std::shared_ptr<ray::streaming::QueueManager> queue_manager_;
  std::shared_ptr<ray::streaming::QueueReader> queue_reader_;
  ActorID actor_id_;
  CoreWorker *core_worker_;
  RayFunction async_func_;
  RayFunction sync_func_;
};

}  // namespace ray

#endif
