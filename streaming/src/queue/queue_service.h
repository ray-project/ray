#ifndef _QUEUE_SERVICE_H_
#define _QUEUE_SERVICE_H_

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <thread>

#include "util/streaming_logging.h"
#include "queue.h"

namespace ray {
namespace streaming {

/// QueueManager manages upstream and downstream queues for worker. Threre should be
/// only one QueueManager instance for a worker. Singleton.
/// QueueManager holds a boost.asio io_service (queue thread). For upstream, queue
/// thread handle events from main thread, for example, notify/pull. For downstream,
/// queue thread receive and handle all events from main thread, to make main thread
/// return quickly.
/// Interface for internal threads.
class QueueService {
 public:
  QueueService(CoreWorker* core_worker, const ActorID &actor_id)
      : core_worker_(core_worker), actor_id_(actor_id), queue_dummy_work_(queue_service_) {
    Init();
  }

  virtual ~QueueService() {
    Stop();
  }

  void DispatchMessage(std::shared_ptr<LocalMemoryBuffer> buffer);
  std::shared_ptr<LocalMemoryBuffer> DispatchMessageSync(std::shared_ptr<LocalMemoryBuffer> buffer);

  std::shared_ptr<Transport> GetOutTransport(const ObjectID &actor_id);

  virtual void DispatchMessageInternal(
      std::shared_ptr<LocalMemoryBuffer> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) {
        STREAMING_CHECK(false) << "QueueService::DispatchMessageInternal should not be called";
  }

  void AddPeerActor(const ObjectID &queue_id, const ActorID &actor_id);
  ActorID GetPeerActor(const ObjectID &queue_id);

  void Release();
 private:
  void Init();
  void Stop();
  /// Queue thread callback function.
  void QueueThreadCallback() { queue_service_.run(); }

 protected:
  CoreWorker* core_worker_;
  ActorID actor_id_;
  std::shared_ptr<Message> ParseMessage(std::shared_ptr<LocalMemoryBuffer> buffer);
 private:
  std::unordered_map<ObjectID, ActorID> actors_;
  std::unordered_map<ObjectID, std::shared_ptr<Transport>> out_transports_;
  std::thread queue_thread_;
  boost::asio::io_service queue_service_;
  // keep queue_service_ alive
  boost::asio::io_service::work queue_dummy_work_;
};

class UpstreamService : public QueueService {
  public:
  UpstreamService(CoreWorker* core_worker, const ActorID &actor_id) : QueueService(core_worker, actor_id) {}
  std::shared_ptr<WriterQueue> CreateUpstreamQueue(const ObjectID &queue_id,
                                             const ActorID &actor_id,
                                             const ActorID &peer_actor_id, uint64_t size);
  bool UpstreamQueueExists(const ObjectID &queue_id);
  /// Wait all queues in queue_ids vector ready, until timeout.
  void WaitQueues(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                  std::vector<ObjectID> &failed_queues, QueueType type);
  
  void UpdateUpActor(const ObjectID &queue_id, const ActorID &actor_id);
  
  void OnNotify(std::shared_ptr<NotificationMessage> notify_msg);

  std::shared_ptr<streaming::WriterQueue> GetUpQueue(const ObjectID &queue_id);

  void ReleaseAllUpQueues();

  virtual void DispatchMessageInternal(
      std::shared_ptr<LocalMemoryBuffer> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback);

  static std::shared_ptr<UpstreamService> GetService(CoreWorker* core_worker, const ActorID &actor_id);

  static RayFunction PEER_SYNC_FUNCTION;
  static RayFunction PEER_ASYNC_FUNCTION;

  private:
  bool CheckQueueSync(const ObjectID &queue_ids, QueueType type);

  private:
  std::unordered_map<ObjectID, std::shared_ptr<streaming::WriterQueue>> upstream_queues_;
  static std::shared_ptr<UpstreamService> upstream_service_;
};

class DownstreamService : public QueueService {
  public:
  DownstreamService(CoreWorker* core_worker, const ActorID &actor_id) : QueueService(core_worker, actor_id) {}
  std::shared_ptr<ReaderQueue> CreateDownstreamQueue(const ObjectID &queue_id,
                                               const ActorID &actor_id,
                                               const ActorID &peer_actor_id);
  bool DownstreamQueueExists(const ObjectID &queue_id);

  void UpdateDownActor(const ObjectID &queue_id, const ActorID &actor_id);

  std::shared_ptr<LocalMemoryBuffer> OnCheckQueue(std::shared_ptr<CheckMessage> check_msg);

  std::shared_ptr<streaming::ReaderQueue> GetDownQueue(const ObjectID &queue_id);

  void ReleaseAllDownQueues();

  void OnData(std::shared_ptr<DataMessage> msg);
  virtual void DispatchMessageInternal(
      std::shared_ptr<LocalMemoryBuffer> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback);
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

  static std::shared_ptr<DownstreamService> GetService(CoreWorker* core_worker, const ActorID &actor_id);
  static RayFunction PEER_SYNC_FUNCTION;
  static RayFunction PEER_ASYNC_FUNCTION;

  private:
  std::unordered_map<ObjectID, std::shared_ptr<streaming::ReaderQueue>> downstream_queues_;
  std::unordered_map<ObjectID, CheckQueueRequest> check_queue_requests_;
  static std::shared_ptr<DownstreamService> downstream_service_;
};

}  // namespace streaming
}  // namespace ray
#endif
