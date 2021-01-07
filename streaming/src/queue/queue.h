#pragma once

#include <iterator>
#include <list>
#include <vector>

#include "queue/queue_item.h"
#include "queue/transport.h"
#include "queue/utils.h"
#include "ray/common/id.h"
#include "ray/util/util.h"
#include "util/streaming_logging.h"

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
  /// \param actor_id, the actor id of upstream worker
  /// \param peer_actor_id, the actor id of downstream worker
  /// \param queue_id the unique identification of a pair of queues (upstream and
  /// downstream).
  /// \param size max size of the queue in bytes.
  /// \param transport
  /// transport to send items to peer.
  Queue(const ActorID &actor_id, const ActorID &peer_actor_id, ObjectID queue_id,
        uint64_t size, std::shared_ptr<Transport> transport)
      : actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        queue_id_(queue_id),
        max_data_size_(size),
        data_size_(0),
        data_size_sent_(0) {
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

  inline bool IsPendingEmpty() {
    std::unique_lock<std::mutex> lock(mutex_);
    return std::next(watershed_iter_) == buffer_queue_.end();
  };

  inline bool IsPendingFull(uint64_t data_size = 0) {
    std::unique_lock<std::mutex> lock(mutex_);
    return max_data_size_ < data_size + data_size_;
  }

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
        min_consumed_msg_id_(QUEUE_INVALID_SEQ_ID),
        peer_last_msg_id_(0),
        peer_last_seq_id_(QUEUE_INVALID_SEQ_ID),
        transport_(transport),
        is_resending_(false),
        is_upstream_first_pull_(true) {}

  /// Push a continuous buffer into queue, the buffer consists of some messages packed by
  /// DataWriter.
  /// \param data, the buffer address
  /// \param data_size, buffer size
  /// \param timestamp, the timestamp when the buffer pushed in
  /// \param msg_id_start, the message id of the first message in the buffer
  /// \param msg_id_end, the message id of the last message in the buffer
  /// \param raw, whether this buffer is raw data, be True only in test
  Status Push(uint8_t *buffer, uint32_t buffer_size, uint64_t timestamp,
              uint64_t msg_id_start, uint64_t msg_id_end, bool raw = false);

  /// Callback function, will be called when downstream queue notifies
  /// it has consumed some items.
  /// NOTE: this callback function is called in queue thread.
  void OnNotify(std::shared_ptr<NotificationMessage> notify_msg);

  /// Callback function, will be called when downstream queue receives
  /// resend items form upstream queue.
  /// NOTE: this callback function is called in queue thread.
  void OnPull(std::shared_ptr<PullRequestMessage> pull_msg,
              boost::asio::io_service &service,
              std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback);

  /// Send items through direct call.
  void Send();

  /// Called when user pushs item into queue. The count of items
  /// can be evicted, determined by eviction_limit_ and min_consumed_msg_id_.
  Status TryEvictItems();

  void SetQueueEvictionLimit(uint64_t msg_id) { eviction_limit_ = msg_id; }

  uint64_t EvictionLimit() { return eviction_limit_; }

  uint64_t GetMinConsumedMsgID() { return min_consumed_msg_id_; }

  void SetPeerLastIds(uint64_t msg_id, uint64_t seq_id) {
    peer_last_msg_id_ = msg_id;
    peer_last_seq_id_ = seq_id;
  }

  uint64_t GetPeerLastMsgId() { return peer_last_msg_id_; }

  uint64_t GetPeerLastSeqId() { return peer_last_seq_id_; }

 private:
  /// Resend an item to peer.
  /// \param item, the item object reference to ben resend.
  /// \param first_seq_id, the seq id of the first item in this resend sequence.
  /// \param last_seq_id, the seq id of the last item in this resend sequence.
  void ResendItem(QueueItem &item, uint64_t first_seq_id, uint64_t last_seq_id);
  /// Resend items to peer from start_iter iterator to watershed_iter_.
  /// \param start_iter, the starting list iterator.
  /// \param first_seq_id, the seq id of the first item in this resend sequence.
  /// \param last_seq_id, the seq id of the last item in this resend sequence.
  int ResendItems(std::list<QueueItem>::iterator start_iter, uint64_t first_seq_id,
                  uint64_t last_seq_id);
  /// Find the item which the message with `target_msg_id` in. If the `target_msg_id`
  /// is larger than the largest message id in the queue, the `greater_callback` callback
  /// will be called; If the `target_message_id` is smaller than the smallest message id
  /// in the queue, the `less_callback` callback will be called; If the `target_msg_id` is
  /// found in the queue, the `found_callback` callback willbe called.
  /// \param target_msg_id, the target message id to be found.
  void FindItem(uint64_t target_msg_id, std::function<void()> greater_callback,
                std::function<void()> less_callback,
                std::function<void(std::list<QueueItem>::iterator, uint64_t, uint64_t)>
                    equal_callback);

 private:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  uint64_t seq_id_;
  uint64_t eviction_limit_;
  uint64_t min_consumed_msg_id_;
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
      : Queue(actor_id, peer_actor_id, queue_id, std::numeric_limits<uint64_t>::max(),
              transport),
        actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        last_recv_seq_id_(QUEUE_INVALID_SEQ_ID),
        last_recv_msg_id_(QUEUE_INVALID_SEQ_ID),
        transport_(transport) {}

  /// Delete processed items whose seq id <= seq_id,
  /// then notify upstream queue.
  void OnConsumed(uint64_t seq_id);

  void OnData(QueueItem &item);
  /// Callback function, will be called when PullPeer DATA comes.
  /// TODO: can be combined with OnData
  /// NOTE: this callback function is called in queue thread.
  void OnResendData(std::shared_ptr<ResendDataMessage> msg);

  inline uint64_t GetLastRecvSeqId() { return last_recv_seq_id_; }
  inline uint64_t GetLastRecvMsgId() { return last_recv_msg_id_; }

 private:
  void Notify(uint64_t seq_id);
  void CreateNotifyTask(uint64_t seq_id, std::vector<TaskArg> &task_args);

 private:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  uint64_t last_recv_seq_id_;
  uint64_t last_recv_msg_id_;
  std::shared_ptr<PromiseWrapper> promise_for_pull_;
  std::shared_ptr<Transport> transport_;
};

}  // namespace streaming
}  // namespace ray
