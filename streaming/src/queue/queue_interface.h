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

class StreamingQueueWriter {
 public:
  /// \param[in] core_worker The C++ pointer point to CoreWorker object of current Actor, obtained in Python/Java
  /// \param[in] async_func peer's asynchronous entry point function descriptor for direct call in Python/Java
  /// \param[in] sync_func peer's synchronous entry point function descriptor for direct call in Python/Java
  StreamingQueueWriter(CoreWorker *core_worker, RayFunction &async_func,
                       RayFunction &sync_func);
  ~StreamingQueueWriter() {}

  /// Create a upstream queue, using \param queue_id to identify this queue
  /// \param[in] queue_id
  /// \param[in] max_size max storage size of the queue in bytes, PushQueueItem will get OutOfMemory if max_size reached.
  /// \param[in] peer_actor_id the actor id of peer actor
  void CreateQueue(const ObjectID &queue_id, int64_t max_size,
                     ActorID &peer_actor_id);

  /// Set max evict limit offset, this queue can evict data less than the offset
  /// \param[in] queue_id
  /// \param[in] eviction_limit max evict limit offset
  Status SetQueueEvictionLimit(const ObjectID &queue_id, uint64_t eviction_limit);

  /// Get consumed offset of corresponded downstream queue
  /// \param[in] queue_id queue id of upstream queue
  /// \param[out] min_consumed_id minimum consumed offset of downstream queue
  void GetMinConsumedSeqID(const ObjectID &queue_id, uint64_t &min_consumed_id);

  /// Write a queue item into queue, this item will be sent to corresponded downstream queue
  /// \param[in] queue_id
  /// \param[in] seq_id sequential id of this queue item
  /// \param[in] data data buffer pointer, should be freed by the caller
  /// \param[in] data_size length of the data buffer
  /// \param[in] timestamp timestamp in milliseconds of the time when the queu item was pushed
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

/// The interfaces streaming queue exposed to DataReader. 
class StreamingQueueReader {
 public:
  /// \param[in] core_worker The C++ pointer point to CoreWorker object of current Actor, obtained in Python/Java
  /// \param[in] async_func peer's asynchronous entry point function descriptor for direct call in Python/Java
  /// \param[in] sync_func peer's synchronous entry point function descriptor for direct call in Python/Java
  StreamingQueueReader(CoreWorker *core_worker, RayFunction &async_func,
                       RayFunction &sync_func);
  ~StreamingQueueReader() {}

  /// Create a downstream queue, using \param queue_id to identify this queue
  /// \param[in] queue_id
  /// \param[in] start_seq_id last consumed offset before failover.
  /// \param[in] peer_actor_id the actor id of peer actor
  bool GetQueue(const ObjectID &queue_id, uint64_t start_seq_id,
                ActorID &peer_actor_id);

  /// Read the latest queue item from the queue identified by \param queue_id
  /// \param[in] queue_id
  /// \param[in] data the start address of a data buffer to hold item
  /// \param[out] data_size return data buffer size
  /// \param[out] seq_id return the sequential id of the latest queue item
  /// \param[in] timeout_ms timeout in milliseconds, data will be nullptr if we can not read a item when timeout.
  Status GetQueueItem(const ObjectID &queue_id, uint8_t *&data, uint32_t &data_size,
                      uint64_t &seq_id, uint64_t timeout_ms = -1);
  
  /// Notify downstream's consumed offset to corresponded upstream queue.
  /// \param[in] queue_id queue id of the upstream queue
  /// \param[in] seq_id consumed offset of the downstream queue
  void NotifyConsumedItem(const ObjectID &queue_id, uint64_t seq_id);
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
