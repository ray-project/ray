#ifndef _QUEUE_MANAGER_H_
#define _QUEUE_MANAGER_H_

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <thread>

#include "streaming_logging.h"
#include "streaming_queue.h"

namespace ray {
namespace streaming {

class QueueManager;

/// Interface for Streaming Writer
class QueueWriter {
 public:
  QueueWriter(std::shared_ptr<QueueManager> manager) : manager_(manager) {}

  virtual ~QueueWriter();

  void CreateQueue(const ObjectID &queue_id, const ActorID &actor_id,
                   const ActorID &peer_actor_id, uint64_t size);
  bool IsQueueExist(const ObjectID &queue_id);
  void SetQueueEvictionLimit(const ObjectID &queue_id, uint64_t limit);
  void WaitQueues(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                  std::vector<ObjectID> &failed_queues);
  uint64_t GetMinConsumedSeqID(const ObjectID &queue_id);
  Status PushSync(const ObjectID &queue_id, uint64_t seq_id, uint8_t *data,
                  uint32_t data_size, uint64_t timestamp,
                  bool raw = false /*only for test*/);
  uint64_t GetLastQueueItem(const ObjectID &queue_id);

 private:
  std::shared_ptr<QueueManager> manager_;
};

/// Interface for Streaming Reader
class QueueReader {
 public:
  QueueReader(std::shared_ptr<QueueManager> manager) : manager_(manager) {}

  virtual ~QueueReader();

  bool CreateQueue(const ObjectID &queue_id, const ActorID &actor_id,
                   const ActorID &peer_actor_id, uint64_t start_seq_id);
  void GetQueueItem(const ObjectID &queue_id, uint8_t *&data, uint32_t &data_size,
                    uint64_t &seq_id, uint64_t timeout_ms = -1);
  void NotifyConsumedItem(const ObjectID &queue_id, uint64_t seq_id);
  uint64_t GetLastSeqID(const ObjectID &queue_id);
  void WaitQueues(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                  std::vector<ObjectID> &failed_queues);

 private:
  std::shared_ptr<QueueManager> manager_;
};

enum class StreamingQueueState : uint8_t {
  Init = 0,
  Running = 1,
  Destroyed = 2
};

/// QueueManager manages upstream and downstream queues for worker. Threre should be
/// only one QueueManager instance for a worker. Singleton.
/// QueueManager holds a boost.asio io_service (queue thread). For upstream, queue
/// thread handle events from main thread, for example, notify/pull. For downstream,
/// queue thread receive and handle all events from main thread, to make main thread
/// return quickly.
/// Interface for internal threads.
class QueueManager {
 public:
  QueueManager(const ActorID &actor_id)
      : actor_id_(actor_id),
      upstream_state_(StreamingQueueState::Init), downstream_state_(StreamingQueueState::Init),
      in_transport_(nullptr), queue_dummy_work_(queue_service_) {
    Init();
  }

  virtual ~QueueManager() {
    Stop();
  }

  /// TODO: Remove singleton if we have a proper initialization sequence.
  static std::shared_ptr<QueueManager> GetInstance(const ActorID &actor_id) {
    if (nullptr == queue_manager_) {
      queue_manager_ = std::make_shared<QueueManager>(actor_id);
    }

    return queue_manager_;
  }

  /// Queue thread callback function.
  void QueueThreadCallback() { queue_service_.run(); }

  std::shared_ptr<WriterQueue> CreateUpQueue(const ObjectID &queue_id,
                                             const ActorID &actor_id,
                                             const ActorID &peer_actor_id, uint64_t size);
  std::shared_ptr<ReaderQueue> CreateDownQueue(const ObjectID &queue_id,
                                               const ActorID &actor_id,
                                               const ActorID &peer_actor_id);
  bool IsUpQueueExist(const ObjectID &queue_id);
  bool IsDownQueueExist(const ObjectID &queue_id);
  std::shared_ptr<streaming::WriterQueue> GetUpQueue(const ObjectID &queue_id);
  std::shared_ptr<streaming::ReaderQueue> GetDownQueue(const ObjectID &queue_id);

  /// Wait all queues in queue_ids vector ready, until timeout.
  void WaitQueues(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                  std::vector<ObjectID> &failed_queues, QueueType type);

  void SetMinConsumedSeqId(const ObjectID &queue_id, uint64_t seq_id);

  void UpdateUpActor(const ObjectID &queue_id, const ActorID &actor_id);
  void UpdateDownActor(const ObjectID &queue_id, const ActorID &actor_id);

  void AddOutTransport(const ObjectID &actor_id, std::shared_ptr<Transport> transport);
  void SetInTransport(std::shared_ptr<Transport> transport);
  std::shared_ptr<Transport> GetOutTransport(const ObjectID &actor_id);
  std::shared_ptr<Transport> GetInTransport();

  void Stop();

  std::shared_ptr<LocalMemoryBuffer> OnCheckQueue(
      std::shared_ptr<CheckMessage> check_msg);
  void OnCheckQueueRsp(std::shared_ptr<CheckRspMessage> check_rsp_msg);

  void DispatchMessage(std::shared_ptr<LocalMemoryBuffer> buffer);
  std::shared_ptr<LocalMemoryBuffer> DispatchMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer);

  void ReleaseAllUpQueues();

  void ReleaseAllDownQueues();
 public:
  /// Used to support synchronize check queue request
  using SendMsgCallback = std::function<void(bool success)>;
  struct CheckQueueRequest {
    CheckQueueRequest() {}
    CheckQueueRequest(const ActorID &actor_id, const ObjectID &queue_id,
                      SendMsgCallback callback = nullptr)
        : actor_id_(actor_id), queue_id_(queue_id), callback_(callback) {}
    ActorID actor_id_;
    ObjectID queue_id_;
    SendMsgCallback callback_;
  };

 private:
  void Init();
  std::shared_ptr<Message> ParseMessage(std::shared_ptr<LocalMemoryBuffer> buffer);
  void DispatchMessageInternal(
      std::shared_ptr<LocalMemoryBuffer> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback);
  bool CheckQueue(const ObjectID &queue_ids, QueueType type);
  bool CheckQueueSync(const ObjectID &queue_ids, QueueType type);

 private:
  ActorID actor_id_;
  std::unordered_map<ObjectID, std::shared_ptr<streaming::WriterQueue>> upstream_queues_;
  std::unordered_map<ObjectID, std::shared_ptr<streaming::ReaderQueue>> downstream_queues_;
  StreamingQueueState upstream_state_;
  StreamingQueueState downstream_state_;

  std::unordered_map<ObjectID, std::pair<ActorID, ActorID>> actors_;

  std::unordered_map<ObjectID, std::shared_ptr<Transport>> out_transports_;

  /// Only used when execute test cases.
  std::shared_ptr<Transport> in_transport_;

  std::unordered_map<ObjectID, CheckQueueRequest> check_queue_requests_;

  std::thread queue_thread_;
  boost::asio::io_service queue_service_;
  // keep queue_service_ alive
  boost::asio::io_service::work queue_dummy_work_;
  // std::shared_ptr<Transport> transport_;

  static std::shared_ptr<QueueManager> queue_manager_;
};

}  // namespace streaming
}  // namespace ray
#endif
