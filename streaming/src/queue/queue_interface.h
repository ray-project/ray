#ifndef QUEUE_INTERFACE_H
#define QUEUE_INTERFACE_H

#include "ray/common/status.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/util.h"
#include "queue/queue_service.h"

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
  StreamingQueueWriter(const ObjectID &queue_id, const ActorID& actor_id);
  ~StreamingQueueWriter() {}

  /// Create a upstream queue, using \param queue_id to identify this queue
  /// \param[in] queue_id
  /// \param[in] max_size max storage size of the queue in bytes, PushQueueItem will get OutOfMemory if max_size reached.
  /// \param[in] peer_actor_id the actor id of peer actor
  void CreateQueue(int64_t max_size, ActorID &peer_actor_id);

  /// Set max evict limit offset, this queue can evict data less than the offset
  /// \param[in] queue_id
  /// \param[in] eviction_limit max evict limit offset
  Status SetQueueEvictionLimit(uint64_t eviction_limit);

  /// Get consumed offset of corresponded downstream queue
  /// \param[in] queue_id queue id of upstream queue
  /// \param[out] min_consumed_id minimum consumed offset of downstream queue
  void GetMinConsumedSeqID(uint64_t &min_consumed_id);

  /// Write a queue item into queue, this item will be sent to corresponded downstream queue
  /// \param[in] queue_id
  /// \param[in] seq_id sequential id of this queue item
  /// \param[in] data data buffer pointer, should be freed by the caller
  /// \param[in] data_size length of the data buffer
  /// \param[in] timestamp timestamp in milliseconds of the time when the queu item was pushed
  Status PushQueueItem(uint64_t seq_id, uint8_t *data,
                       uint32_t data_size, uint64_t timestamp);
  Status DeleteQueue();

 private:
  std::shared_ptr<ray::streaming::UpstreamService> upstream_service_;
  ObjectID queue_id_;
  ActorID actor_id_;
  std::shared_ptr<WriterQueue> queue_;
};

/// The interfaces streaming queue exposed to DataReader. 
class StreamingQueueReader {
 public:
  /// \param[in] core_worker The C++ pointer point to CoreWorker object of current Actor, obtained in Python/Java
  /// \param[in] async_func peer's asynchronous entry point function descriptor for direct call in Python/Java
  /// \param[in] sync_func peer's synchronous entry point function descriptor for direct call in Python/Java
  StreamingQueueReader(const ObjectID &queue_id, const ActorID& actor_id);
  ~StreamingQueueReader() {}

  /// Create a downstream queue, using \param queue_id to identify this queue
  /// \param[in] queue_id
  /// \param[in] start_seq_id last consumed offset before failover.
  /// \param[in] peer_actor_id the actor id of peer actor
  bool GetQueue(uint64_t start_seq_id, ActorID &peer_actor_id);

  /// Read the latest queue item from the queue identified by \param queue_id
  /// \param[in] queue_id
  /// \param[in] data the start address of a data buffer to hold item
  /// \param[out] data_size return data buffer size
  /// \param[out] seq_id return the sequential id of the latest queue item
  /// \param[in] timeout_ms timeout in milliseconds, data will be nullptr if we can not read a item when timeout.
  Status GetQueueItem(uint8_t *&data, uint32_t &data_size,
                      uint64_t &seq_id, uint64_t timeout_ms = -1);
  
  /// Notify downstream's consumed offset to corresponded upstream queue.
  /// \param[in] queue_id queue id of the upstream queue
  /// \param[in] seq_id consumed offset of the downstream queue
  void NotifyConsumedItem(uint64_t seq_id);
  Status DeleteQueue();

 private:
  std::shared_ptr<ray::streaming::DownstreamService> downstream_service_;
  ObjectID queue_id_;
  ActorID actor_id_;
  std::shared_ptr<ReaderQueue> queue_;
};

}  // namespace streaming
}  // namespace ray

#endif
