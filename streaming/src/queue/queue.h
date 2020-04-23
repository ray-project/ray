#ifndef _STREAMING_QUEUE_H_
#define _STREAMING_QUEUE_H_
#include <iterator>
#include <list>
#include <vector>

#include "ray/common/id.h"
#include "ray/util/util.h"

#include "queue_item.h"
#include "transport.h"
#include "util/streaming_logging.h"
#include "utils.h"

namespace ray {
namespace streaming {

using ray::ObjectID;

enum QueueType { UPSTREAM = 0, DOWNSTREAM };

/// A queue-like data structure, which does not delete its items after poped.
/// The lifecycle of each item is:
/// - Pending, an item is pushed into a queue, but has not been processed (sent out or
/// consumed),
/// - Processed, has been handled by the user, but should not be deleted.
/// - Evicted, useless to the user, should be poped and destroyed.
/// At present, this data structure is implemented with one std::list,
/// using a watershed iterator to divided.
class Queue {
 public:
  /// \param[in] queue_id the unique identification of a pair of queues (upstream and
  /// downstream). \param[in] size max size of the queue in bytes. \param[in] transport
  /// transport to send items to peer.
  Queue(const ActorID &actor_id, const ActorID &peer_actor_id, ObjectID queue_id, uint64_t size, std::shared_ptr<Transport> transport)
      : actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        queue_id_(queue_id), max_data_size_(size), data_size_(0), data_size_sent_(0) {
    buffer_queue_.push_back(InvalidQueueItem());
    watershed_iter_ = buffer_queue_.begin();
  }

  virtual ~Queue() {}

  /// Push an item into the queue.
  /// \param[in] item the QueueItem object to be send to peer.
  /// \return false if the queue is full.
  bool Push(QueueItem item);

  /// Get the front of item which in processed state.
  QueueItem FrontProcessed();

  /// Pop the front of item which in processed state.
  QueueItem PopProcessed();

  /// Pop the front of item which in pending state, the item
  /// will not be evicted at this moment, its state turn to
  /// processed.
  QueueItem PopPending();

  /// PopPending with timeout in microseconds.
  QueueItem PopPendingBlockTimeout(uint64_t timeout_us);

  /// Return the last item in pending state.
  QueueItem BackPending();

  inline bool IsPendingEmpty();
  inline bool IsPendingFull(uint64_t data_size = 0);

  /// Return the size in bytes of all items in queue.
  inline uint64_t QueueSize() { return data_size_; }

  /// Return the size in bytes of all items in pending state.
  inline uint64_t PendingDataSize() { return data_size_ - data_size_sent_; }

  /// Return the size in bytes of all items in processed state.
  inline uint64_t ProcessedDataSize() { return data_size_sent_; }

  /// Return item count of the queue.
  inline size_t Count() { return buffer_queue_.size(); }

  /// Return item count in pending state.
  inline size_t PendingCount();

  /// Return item count in processed state.
  inline size_t ProcessedCount();

  inline ActorID GetActorID() { return actor_id_; }
  inline ActorID GetPeerActorID() { return peer_actor_id_; }
  inline ObjectID GetQueueID() { return queue_id_; }
 protected:
  std::list<QueueItem> buffer_queue_;
  std::list<QueueItem>::iterator watershed_iter_;

  ActorID actor_id_;
  ActorID peer_actor_id_;
  ObjectID queue_id_;
  /// max data size in bytes
  uint64_t max_data_size_;
  uint64_t data_size_;
  uint64_t data_size_sent_;

  std::mutex mutex_;
  std::condition_variable readable_cv_;
};

const uint64_t QUEUE_INITIAL_SEQ_ID = 1;

/// Queue in upstream.
class WriterQueue : public Queue {
 public:
  /// \param queue_id, the unique ObjectID to identify a queue
  /// \param actor_id, the actor id of upstream worker
  /// \param peer_actor_id, the actor id of downstream worker
  /// \param size, max data size in bytes
  /// \param transport, transport
  WriterQueue(const ObjectID &queue_id, const ActorID &actor_id,
              const ActorID &peer_actor_id, uint64_t size,
              std::shared_ptr<Transport> transport)
      : Queue(actor_id, peer_actor_id, queue_id, size, transport),
        actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        seq_id_(QUEUE_INITIAL_SEQ_ID),
        eviction_limit_(QUEUE_INVALID_SEQ_ID),
        min_consumed_id_(QUEUE_INVALID_SEQ_ID),
        peer_last_msg_id_(0),
        peer_last_seq_id_(QUEUE_INVALID_SEQ_ID),
        transport_(transport),
        is_resending_(false) {}

  /// Push a continuous buffer into queue.
  /// NOTE: the buffer should be copied.
  Status Push(uint64_t seq_id, uint8_t *data, uint32_t data_size, uint64_t timestamp,
              uint64_t msg_id_start, uint64_t msg_id_end, bool raw = false);

  /// Callback function, will be called when downstream queue notifies
  /// it has consumed some items.
  /// NOTE: this callback function is called in queue thread.
  void OnNotify(std::shared_ptr<NotificationMessage> notify_msg);

  /// Callback function, will be called when downstream queue wants to
  /// pull some items form our upstream queue.
  /// NOTE: this callback function is called in queue thread.
  void OnPull(std::shared_ptr<PullRequestMessage> pull_msg,
              boost::asio::io_service &service,
              std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback);

  /// Send items through direct call.
  void Send();

  /// Called when user pushs item into queue. The count of items
  /// can be evicted, determined by eviction_limit_ and min_consumed_id_.
  Status TryEvictItems();

  void SetQueueEvictionLimit(uint64_t eviction_limit) {
    eviction_limit_ = eviction_limit;
  }

  uint64_t EvictionLimit() { return eviction_limit_; }

  uint64_t GetMinConsumedSeqID() { return min_consumed_id_; }

  void SetPeerLastIds(uint64_t msg_id, uint64_t seq_id) {
    peer_last_msg_id_ = msg_id;
    peer_last_seq_id_ = seq_id;
  }

  uint64_t GetPeerLastMsgId() { return peer_last_msg_id_; }

  uint64_t GetPeerLastSeqId() { return peer_last_seq_id_; }

 private:
  void ResendItem(QueueItem &item, uint64_t first_seq_id, uint64_t last_seq_id);
  int ResendItems(std::list<QueueItem>::iterator start_iter, uint64_t first_seq_id,
                  uint64_t last_seq_id);
  void FindItem(
      uint64_t target, std::function<void()> large, std::function<void()> small,
      std::function<void(std::list<QueueItem>::iterator, uint64_t, uint64_t)> ok);
 private:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  uint64_t seq_id_;
  uint64_t eviction_limit_;
  uint64_t min_consumed_id_;
  uint64_t peer_last_msg_id_;
  uint64_t peer_last_seq_id_;
  std::shared_ptr<Transport> transport_;

  std::atomic<bool> is_resending_;
  bool is_upstream_first_pull_;
};

/// Queue in downstream.
class ReaderQueue : public Queue {
 public:
  /// \param queue_id, the unique ObjectID to identify a queue
  /// \param actor_id, the actor id of upstream worker
  /// \param peer_actor_id, the actor id of downstream worker
  /// \param transport, transport
  /// NOTE: we do not restrict queue size of ReaderQueue
  ReaderQueue(const ObjectID &queue_id, const ActorID &actor_id,
              const ActorID &peer_actor_id, std::shared_ptr<Transport> transport)
      : Queue(actor_id, peer_actor_id, queue_id, std::numeric_limits<uint64_t>::max(), transport),
        actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        min_consumed_id_(QUEUE_INVALID_SEQ_ID),
        last_recv_seq_id_(QUEUE_INVALID_SEQ_ID),
        expect_seq_id_(1),
        transport_(transport) {}

  /// Delete processed items whose seq id <= seq_id,
  /// then notify upstream queue.
  void OnConsumed(uint64_t seq_id);

  void OnData(QueueItem &item);

  uint64_t GetMinConsumedSeqID() { return min_consumed_id_; }

  uint64_t GetLastRecvSeqId() { return last_recv_seq_id_; }

  void SetExpectSeqId(uint64_t expect) { expect_seq_id_ = expect; }

 private:
  void Notify(uint64_t seq_id);
  void CreateNotifyTask(uint64_t seq_id, std::vector<TaskArg> &task_args);

 private:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  uint64_t min_consumed_id_;
  uint64_t last_recv_seq_id_;
  uint64_t expect_seq_id_;
  std::shared_ptr<PromiseWrapper> promise_for_pull_;
  std::shared_ptr<Transport> transport_;
};

}  // namespace streaming
}  // namespace ray
#endif
