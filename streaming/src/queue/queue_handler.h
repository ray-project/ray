#pragma once

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <thread>

#include "queue/queue.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

using namespace ray::core;

enum class StreamingQueueStatus : uint32_t {
  OK = 0,
  Timeout = 1,
  DataLost = 2,  // The data in upstream has been evicted when downstream try to pull data
                 // from upstream.
  NoValidData = 3,  // There is no data written into queue, or start_msg_id is bigger than
                    // all items in queue now.
};

static inline std::ostream &operator<<(std::ostream &os,
                                       const StreamingQueueStatus &status) {
  os << static_cast<std::underlying_type<StreamingQueueStatus>::type>(status);
  return os;
}

/// Base class of UpstreamQueueMessageHandler and DownstreamQueueMessageHandler.
/// A queue service manages a group of queues, upstream queues or downstream queues of
/// the current actor. Each queue service holds a boost.asio io_service, to handle
/// messages asynchronously. When a message received by Writer/Reader in ray call thread,
/// the message was delivered to
/// UpstreamQueueMessageHandler/DownstreamQueueMessageHandler, then the ray call thread
/// returns immediately. The queue service parses meta infomation from the message,
/// including queue_id actor_id, etc, and dispatchs message to queue according to
/// queue_id.
class QueueMessageHandler {
 public:
  /// Construct a QueueMessageHandler instance.
  /// \param[in] actor_id actor id of current actor.
  QueueMessageHandler(const ActorID &actor_id)
      : actor_id_(actor_id), queue_dummy_work_(queue_service_) {}

  virtual ~QueueMessageHandler() { Stop(); }

  /// Dispatch message buffer to asio service.
  /// \param[in] buffer serialized message received from peer actor.
  void DispatchMessageAsync(std::shared_ptr<LocalMemoryBuffer> buffer);

  /// Dispatch message buffer to asio service synchronously, and wait for handle result.
  /// \param[in] buffer serialized message received from peer actor.
  /// \return handle result.
  std::shared_ptr<LocalMemoryBuffer> DispatchMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer);

  /// Get transport to a peer actor specified by actor_id.
  /// \param[in] actor_id actor id of peer actor
  /// \return transport
  std::shared_ptr<Transport> GetOutTransport(const ObjectID &actor_id);

  /// The actual function where message being dispatched, called by DispatchMessageAsync
  /// and DispatchMessageSync.
  /// \param[in] buffer serialized message received from peer actor.
  /// \param[in] callback the callback function used by DispatchMessageSync, called
  ///            after message processed complete. The std::shared_ptr<LocalMemoryBuffer>
  ///            parameter is the return value.
  virtual void DispatchMessageInternal(
      std::shared_ptr<LocalMemoryBuffer> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) = 0;

  /// Save actor_id of the peer actor specified by queue_id. For a upstream queue, the
  /// peer actor refer specifically to the actor in current ray cluster who has a
  /// downstream queue with same queue_id, and vice versa.
  /// \param[in] queue_id queue id of current queue.
  /// \param[in] actor_id actor_id actor id of corresponded peer actor.
  void SetPeerActorID(const ObjectID &queue_id, const ActorID &actor_id,
                      RayFunction &async_func, RayFunction &sync_func);

  /// Obtain the actor id of the peer actor specified by queue_id.
  /// \return actor id
  ActorID GetPeerActorID(const ObjectID &queue_id);

  /// Release all queues in current queue service.
  void Release();

 protected:
  /// Start asio service
  virtual void Start();
  /// Stop asio service
  virtual void Stop();
  /// The callback function of internal thread.
  void QueueThreadCallback() { queue_service_.run(); }

 protected:
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

/// UpstreamQueueMessageHandler holds and manages all upstream queues of current actor.
class UpstreamQueueMessageHandler : public QueueMessageHandler {
 public:
  /// Construct a UpstreamQueueMessageHandler instance.
  UpstreamQueueMessageHandler(const ActorID &actor_id)
      : QueueMessageHandler(actor_id), handler_service_dummy_worker_(handler_service_) {
    Start();
  }
  /// Create a upstream queue.
  /// \param[in] queue_id queue id of the queue to be created.
  /// \param[in] peer_actor_id actor id of peer actor.
  /// \param[in] size the max memory size of the queue.
  std::shared_ptr<WriterQueue> CreateUpstreamQueue(const ObjectID &queue_id,
                                                   const ActorID &peer_actor_id,
                                                   uint64_t size);
  /// Check whether the upstream queue specified by queue_id exists or not.
  bool UpstreamQueueExists(const ObjectID &queue_id);
  /// Wait all queues in queue_ids vector ready, until timeout.
  /// \param[in] queue_ids a group of queues.
  /// \param[in] timeout_ms max timeout time interval for wait all queues.
  /// \param[out] failed_queues a group of queues which are not ready when timeout.
  void WaitQueues(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                  std::vector<ObjectID> &failed_queues);
  /// Handle notify message from corresponded downstream queue.
  void OnNotify(std::shared_ptr<NotificationMessage> notify_msg);
  /// Handle pull request message from corresponded downstream queue.
  void OnPullRequest(std::shared_ptr<PullRequestMessage> pull_msg,
                     std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback);
  /// Obtain upstream queue specified by queue_id.
  std::shared_ptr<streaming::WriterQueue> GetUpQueue(const ObjectID &queue_id);
  /// Release all upstream queues
  void ReleaseAllUpQueues();

  virtual void DispatchMessageInternal(
      std::shared_ptr<LocalMemoryBuffer> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) override;

  static std::shared_ptr<UpstreamQueueMessageHandler> CreateService(
      const ActorID &actor_id);
  static std::shared_ptr<UpstreamQueueMessageHandler> GetService();
  virtual void Start() override;

 private:
  bool CheckQueueSync(const ObjectID &queue_ids);
  virtual void Stop() override;

 private:
  std::unordered_map<ObjectID, std::shared_ptr<streaming::WriterQueue>> upstream_queues_;
  static std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler_;
  boost::asio::io_service handler_service_;
  boost::asio::io_service::work handler_service_dummy_worker_;
  std::thread handle_service_thread_;
};

/// DownstreamQueueMessageHandler holds and manages all downstream queues of current
/// actor.
class DownstreamQueueMessageHandler : public QueueMessageHandler {
 public:
  DownstreamQueueMessageHandler(const ActorID &actor_id) : QueueMessageHandler(actor_id) {
    Start();
  }
  /// Create a downstream queue.
  /// \param queue_id, queue id of the queue to be created.
  /// \param peer_actor_id, actor id of peer actor.
  std::shared_ptr<ReaderQueue> CreateDownstreamQueue(const ObjectID &queue_id,
                                                     const ActorID &peer_actor_id);
  /// Request to pull messages from corresponded upstream queue, whose message id
  /// is larger than `start_msg_id`. Multiple attempts to pull until timeout.
  /// \param queue_id, queue id of the queue to be pulled.
  /// \param start_msg_id, the starting message id reqeust by downstream queue.
  /// \param is_upstream_first_pull
  /// \param timeout_ms, the maxmium timeout.
  StreamingQueueStatus PullQueue(const ObjectID &queue_id, uint64_t start_msg_id,
                                 bool &is_upstream_first_pull,
                                 uint64_t timeout_ms = 2000);
  /// Check whether the downstream queue specified by queue_id exists or not.
  bool DownstreamQueueExists(const ObjectID &queue_id);
  std::shared_ptr<LocalMemoryBuffer> OnCheckQueue(
      std::shared_ptr<CheckMessage> check_msg);
  /// Obtain downstream queue specified by queue_id.
  std::shared_ptr<streaming::ReaderQueue> GetDownQueue(const ObjectID &queue_id);
  /// Release all downstream queues
  void ReleaseAllDownQueues();
  /// The callback function called when downstream queue receives a queue item.
  void OnData(std::shared_ptr<DataMessage> msg);
  virtual void DispatchMessageInternal(
      std::shared_ptr<LocalMemoryBuffer> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback);
  static std::shared_ptr<DownstreamQueueMessageHandler> CreateService(
      const ActorID &actor_id);
  static std::shared_ptr<DownstreamQueueMessageHandler> GetService();
  StreamingQueueStatus PullPeerAsync(const ObjectID &queue_id, uint64_t start_msg_id,
                                     bool &is_upstream_first_pull, uint64_t timeout_ms);

 private:
  std::unordered_map<ObjectID, std::shared_ptr<streaming::ReaderQueue>>
      downstream_queues_;
  static std::shared_ptr<DownstreamQueueMessageHandler> downstream_handler_;
};

}  // namespace streaming
}  // namespace ray
