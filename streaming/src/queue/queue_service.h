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

/// Base class of UpstreamService and DownstreamService.
/// A queue service manages a group of queues, upstream queues or downstream queues of current actor.
/// Each queue service holds a boost.asio io_service, to handle message asynchronously.
/// When a message received by Writer/Reader in ray call thread, the message was delivered
/// to UpstreamService/DownstreamService, then the ray call thread returns immediately.
/// The queue service parses meta infomations from message, including queue_id actor_id, etc,
/// and dispatchs message to queue according to queue_id.
class QueueService {
 public:
  /// Construct a QueueService instance.
  /// \param[in] core_worker CoreWorker C++ pointer of current actor, used to call Core Worker's api.
  ///            For Python worker, the pointer can be obtained from ray.worker.global_worker.core_worker;
  ///            For Java worker, obtained from RayNativeRuntime object through java reflection.
  /// \param[in] actor_id actor id of current actor.
  QueueService(CoreWorker* core_worker, const ActorID &actor_id)
      : core_worker_(core_worker), actor_id_(actor_id), queue_dummy_work_(queue_service_) {
    Start();
  }

  virtual ~QueueService() {
    Stop();
  }

  /// Dispatch message buffer to asio service.
  /// \param[in] buffer serialized message received from peer actor.
  void DispatchMessageAsync(std::shared_ptr<LocalMemoryBuffer> buffer);

  /// Dispatch message buffer to asio service synchronously, and wait for handle result.
  /// \param[in] buffer serialized message received from peer actor.
  std::shared_ptr<LocalMemoryBuffer> DispatchMessageSync(std::shared_ptr<LocalMemoryBuffer> buffer);

  /// Get transport to a peer actor specified by actor_id.
  /// \param[in] actor_id actor id of peer actor
  std::shared_ptr<Transport> GetOutTransport(const ObjectID &actor_id);

  /// The actual function where message being dispatched, called by DispatchMessageAsync and DispatchMessageSync.
  /// \param[in] buffer serialized message received from peer actor.
  /// \param[in] callback the callback function used by DispatchMessageSync, called after message processed complete. 
  ///            The std::shared_ptr<LocalMemoryBuffer> parameter is the return value.
  virtual void DispatchMessageInternal(
      std::shared_ptr<LocalMemoryBuffer> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) = 0;

  /// Save actor_id of the peer actor specified by queue_id. For a upstream queue, the peer actor refer specifically
  /// to the actor in current ray cluster who has a downstream queue with same queue_id, and vice versa. 
  /// \param[in] queue_id queue id of current queue
  /// \param[in] actor_id actor_id actor id of corresponded peer actor
  void SetPeerActorID(const ObjectID &queue_id, const ActorID &actor_id);

  /// Obtain the actor id of the peer actor specified by queue_id.
  ActorID GetPeerActorID(const ObjectID &queue_id);

  /// Release all queues in current queue service.
  void Release();

 private:
  /// Start asio service
  void Start();
  /// Stop asio service
  void Stop();
  /// The callback function of internal thread.
  void QueueThreadCallback() { queue_service_.run(); }

 protected:
  /// CoreWorker C++ pointer of current actor
  CoreWorker* core_worker_;
  /// actor_id actor id of current actor
  ActorID actor_id_;
  /// Helper function, parse message buffer to Message object.
  std::shared_ptr<Message> ParseMessage(std::shared_ptr<LocalMemoryBuffer> buffer);

 private:
  /// Map from queue id to a actor id of the queue's peer actor.
  std::unordered_map<ObjectID, ActorID> actors_;
  /// Map from queue id to a transport of the queue's peer actor.
  std::unordered_map<ObjectID, std::shared_ptr<Transport>> out_transports_;
  /// The internal thread which asio service run with.
  std::thread queue_thread_;
  /// The internal asio service.
  boost::asio::io_service queue_service_;
  /// The asio work which keeps queue_service_ alive.
  boost::asio::io_service::work queue_dummy_work_;
};

/// UpstreamService holds and manages all upstream queues of current actor.
class UpstreamService : public QueueService {
  public:
  /// Construct a UpstreamService instance.
  UpstreamService(CoreWorker* core_worker, const ActorID &actor_id) : QueueService(core_worker, actor_id) {}
  /// Create a upstream queue.
  /// \param[in] queue_id queue id of the queue to be created.
  /// \param[in] peer_actor_id actor id of peer actor.
  /// \param[in] size the max memory size of the queue.
  std::shared_ptr<WriterQueue> CreateUpstreamQueue(const ObjectID &queue_id,
                                             const ActorID &peer_actor_id, uint64_t size);
  /// Check whether the upstream queue specified by queue_id exists or not.
  bool UpstreamQueueExists(const ObjectID &queue_id);
  /// Wait all queues in queue_ids vector ready, until timeout.
  /// \param[in] queue_ids a group of queues
  /// \param[in] timeout_ms max timeout time interval for wait all queues
  /// \param[out] failed_queues a group of queues which are not ready when timeout
  /// \param[type]
  void WaitQueues(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                  std::vector<ObjectID> &failed_queues);
  /// Handle notify message from corresponded downstream queue.
  void OnNotify(std::shared_ptr<NotificationMessage> notify_msg);
  /// Obtain upstream queue specified by queue_id.
  std::shared_ptr<streaming::WriterQueue> GetUpQueue(const ObjectID &queue_id);
  /// Release all upstream queues
  void ReleaseAllUpQueues();

  virtual void DispatchMessageInternal(
      std::shared_ptr<LocalMemoryBuffer> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) override;

  static std::shared_ptr<UpstreamService> CreateService(CoreWorker* core_worker, const ActorID &actor_id);
  static std::shared_ptr<UpstreamService> GetService();

  static RayFunction PEER_SYNC_FUNCTION;
  static RayFunction PEER_ASYNC_FUNCTION;

  private:
  bool CheckQueueSync(const ObjectID &queue_ids);

  private:
  std::unordered_map<ObjectID, std::shared_ptr<streaming::WriterQueue>> upstream_queues_;
  static std::shared_ptr<UpstreamService> upstream_service_;
};

/// UpstreamService holds and manages all downstream queues of current actor.
class DownstreamService : public QueueService {
  public:
  DownstreamService(CoreWorker* core_worker, const ActorID &actor_id) : QueueService(core_worker, actor_id) {}
  std::shared_ptr<ReaderQueue> CreateDownstreamQueue(const ObjectID &queue_id,
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

  static std::shared_ptr<DownstreamService> CreateService(CoreWorker* core_worker, const ActorID &actor_id);
  static std::shared_ptr<DownstreamService> GetService();
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
