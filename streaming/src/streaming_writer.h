#ifndef RAY_STREAMING_WRITER_H
#define RAY_STREAMING_WRITER_H

#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "buffer_pool.h"
#include "streaming.h"
#include "streaming_barrier_helper.h"
#include "streaming_channel.h"
#include "streaming_config.h"
#include "streaming_flow_control.h"
#include "streaming_message_bundle.h"
#include "streaming_reliability_helper.h"
#include "streaming_transfer.h"

namespace ray {
namespace streaming {
class ReliabilityHelper;
class ExactlySameHelper;
class StreamingFlowControl;

enum class EventType : uint8_t {
  UserEvent = 0,
  FlowEvent = 1,
  EmptyEvent = 2,
  FullChannel = 3,
  Reload = 4,
};

struct EnumTypeHash {
  template <typename T>
  std::size_t operator()(const T &t) const {
    return static_cast<std::size_t>(t);
  }
};

struct Event {
  ProducerChannelInfo *channel_info_;
  EventType type_;
  bool urgent_;
};

class EventServer {
 private:
  typedef std::function<bool(ProducerChannelInfo *info)> Handle;
  std::unordered_map<EventType, Handle, EnumTypeHash> event_handle_map_;
  std::shared_ptr<EventQueue<Event> > event_queue_;
  std::shared_ptr<std::thread> loop_thread_;

  bool stop_flag_ = false;

 public:
  EventServer() { event_queue_ = std::make_shared<EventQueue<Event> >(1000); }

  ~EventServer() {
    stop_flag_ = true;
    if (loop_thread_->joinable()) {
      STREAMING_LOG(WARNING) << "Loop Thread Stopped";
      loop_thread_->join();
    }
  }

  void Run() {
    stop_flag_ = false;
    event_queue_->Run();
    loop_thread_ = std::make_shared<std::thread>(&EventServer::LoopThd, this);
    STREAMING_LOG(WARNING) << "event_server run";
  }

  void Stop() {
    stop_flag_ = true;
    event_queue_->Stop();
    if (loop_thread_->joinable()) {
      loop_thread_->join();
    }
    STREAMING_LOG(WARNING) << "event_server stop";
  }

  bool Register(const EventType &type, const Handle &handle) {
    if (event_handle_map_.find(type) != event_handle_map_.end()) {
      STREAMING_LOG(WARNING) << "EventType had been registered!";
    }
    event_handle_map_[type] = handle;
    return true;
  }

  void Push(Event &&event) {
    if (event.urgent_) {
      event_queue_->PushToUrgent(event);
    } else {
      event_queue_->Push(event);
    }
  }

  void Push(const Event &event) {
    if (event.urgent_) {
      event_queue_->PushToUrgent(event);
    } else {
      event_queue_->Push(event);
    }
  }

  void Execute(Event &event) {
    if (event_handle_map_.find(event.type_) == event_handle_map_.end()) {
      STREAMING_LOG(WARNING) << "Handle is't Registered";
      return;
    }
    Handle &handle = event_handle_map_[event.type_];
    if (handle(event.channel_info_)) {
      event_queue_->Pop();
    }
  }

  void LoopThd() {
    while (true) {
      if (stop_flag_) {
        break;
      }
      Event event;
      if (event_queue_->Get(event)) {
        Execute(event);
      }
    }
  }

  size_t EventNums() { return event_queue_->Size(); }
};

class StreamingWriter : public StreamingCommon {
 private:
  struct StreamingRescaleRAII {
    StreamingWriter *p_;
    StreamingRescaleRAII(StreamingWriter *p) : p_(p) {
      STREAMING_LOG(INFO) << "runner suspened";
      p_->channel_state_ = StreamingChannelState::Rescaling;
      p_->ShutdownTimer();
      if (p_->config_.GetStreaming_transfer_event_driven()) {
        p_->event_server_->Stop();
        if (p_->empty_message_thread_->joinable()) {
          p_->empty_message_thread_->join();
        }
        if (p_->flow_control_thread_->joinable()) {
          p_->flow_control_thread_->join();
        }
      } else {
        p_->loop_thread_->join();
      }
    }

    ~StreamingRescaleRAII() {
      STREAMING_LOG(INFO) << "runner restart";
      p_->channel_state_ = StreamingChannelState::Running;
      if (p_->config_.GetStreaming_transfer_event_driven()) {
        p_->event_server_->Run();
        p_->empty_message_thread_ =
            std::make_shared<std::thread>(&StreamingWriter::EmptyMessageTimer, p_);
        p_->flow_control_thread_ =
            std::make_shared<std::thread>(&StreamingWriter::FlowControlThread, p_);
      } else {
        p_->loop_thread_ =
            std::make_shared<std::thread>(&StreamingWriter::WriterLoopForward, p_);
      }
      p_->EnableTimer();
    }
  };

  std::shared_ptr<EventServer> event_server_;

  std::shared_ptr<std::thread> loop_thread_;
  std::shared_ptr<std::thread> flow_control_thread_;
  std::shared_ptr<std::thread> empty_message_thread_;
  // ProducerTransfer is middle broker for data transporting.
  std::shared_ptr<ProducerTransfer> transfer_;

  // One channel have unique identity.
  std::vector<ObjectID> output_queue_ids_;
  std::unordered_map<ObjectID, ProducerChannelInfo> channel_info_map_;

  StreamingBarrierHelper barrier_helper_;

  std::shared_ptr<StreamingFlowControl> flow_controller_;
  std::shared_ptr<ReliabilityHelper> reliability_helper_;

  // This property is for debug if transfering blocked.
  // The verbose log in timer will report and show which channel is active,
  // which helps us to find some useful channel informations.
  ObjectID last_write_q_id_;

  // Make thread-safe between loop thread and user thread.
  // High-level runtime send notification about clear checkpoint if global
  // checkpoint is finished and low-level will auto flush & evict item memory
  // when no more space is available.
  std::atomic_flag notify_flag_ = ATOMIC_FLAG_INIT;

  // In daemon loop thread, the first phase is reloading meta data from global
  // storage outside if it's exactly same reliability. But it should not be
  // reopen while rescaling.
  bool reload_once = false;

  static constexpr uint32_t kQueueItemMaxBlocks = 10;

  // ExactlySameHelper need some writer private properties or methods
  // to put bundle meta info into storage outside.
  friend class ExactlySameHelper;

 private:
  void EmptyMessageTimer();
  void FlowControlThread();
  bool WriteAllToChannel(ProducerChannelInfo *channel_info);
  bool SendEmptyToChannel(ProducerChannelInfo *channel_info);
  bool Reload(ProducerChannelInfo *channel_info);

  bool IsMessageAvailableInBuffer(ProducerChannelInfo &channel_info);

  StreamingStatus WriteBufferToChannel(ProducerChannelInfo &channel_info,
                                       uint64_t &buffer_remain);

  void WriterLoopForward();

  /*!
   * @brief push empty message when no valid message or bundle was produced each time
   * interval
   * @param q_id, queue id
   * @param meta_ptr, it's resend empty bundle if meta pointer is non-null.
   */
  StreamingStatus WriteEmptyMessage(
      ProducerChannelInfo &channel_info,
      const StreamingMessageBundleMetaPtr &meta_ptr = nullptr);

  void ClearCheckpointId(ProducerChannelInfo &channel_info, uint64_t seq_id);

  StreamingStatus WriteTransientBufferToChannel(ProducerChannelInfo &channel_info);

  void NotifyConsumed(ProducerChannelInfo &channel_info);

  bool CollectFromRingBuffer(ProducerChannelInfo &channel_info, uint64_t &buffer_remain,
                             StreamingMessageList *message_list_ptr = nullptr,
                             const StreamingMessageBundleMetaPtr &meta_ptr = nullptr);

  StreamingStatus WriteChannelProcess(ProducerChannelInfo &channel_info,
                                      bool *is_empty_message);

  /*!
   * @brief send barrier to all channel
   * Note: there are user define data in barrier bundle
   * @param barrier_id
   * @param data
   * @param data_size
   */
  void BroadcastBarrier(uint64_t barrier_id, const uint8_t *data, uint32_t data_size);

  StreamingStatus InitChannel(const ObjectID &q_id, uint64_t channel_message_id,
                              const std::string &plasma_store_path, uint64_t queue_size);

  StreamingStatus DestroyChannel(const std::vector<ObjectID> &id_vec);

 protected:
  void ReportTimer() override;
  void ScaleUp(const std::vector<ray::ObjectID> &id_vec) override;
  void ScaleDown(const std::vector<ray::ObjectID> &id_vec) override;
  void RefreshChannelInfo(ProducerChannelInfo &channel_info);

 public:
  virtual ~StreamingWriter();

  /*!
   * @brief streaming writer client initialization without raylet/local sheduler
   * @param queue_id_vec queue id vector
   * @param plasma_store_path_vec plasma store path vector, it's bitwise mapping with
   * queueid
   * @param channel_message_id_vec channel seq id is related with message checkpoint
   * @param queue_size queue size (memory size not length)
   * @param abnormal_queues reamaining queue id vector
   */
  StreamingStatus Init(const std::vector<ObjectID> &queue_id_vec,
                       const std::string &plasma_store_path,
                       const std::vector<uint64_t> &channel_message_id_vec,
                       const std::vector<uint64_t> &queue_size_vec,
                       std::vector<ObjectID> &abnormal_queues);

  /*!
   * @brief new writer client in raylet
   * @param queue_id_vec
   * @param plasma_store_path_vec
   * @param channel_seq_id_vec
   * @param raylet_client
   * @param queue_size
   * @param abnormal_queues reamaining queue id vector
   * @param create_types_vec channel creation type.
   */

  StreamingStatus Init(const std::vector<ObjectID> &queue_id_vec,
                       const std::string &plasma_store_path,
                       const std::vector<uint64_t> &channel_seq_id_vec,
                       const std::vector<uint64_t> &queue_size_vec,
                       std::vector<ObjectID> &abnormal_queues,
                       const std::vector<StreamingQueueCreationType> &create_types_vec);
  /*!
   * @brief To increase throughout, we employed an output buffer for message
   * transformation,
   * which means we merge a lot of message to a message bundle and no message will be
   * pushed
   * into queue directly util daemon thread does this action. Additionally, writing will
   * block
   * when buffer ring is full.
   * @param q_id
   * @param data
   * @param data_size
   * @param message_type
   * @return message seq iq
   */
  uint64_t WriteMessageToBufferRing(
      const ObjectID &q_id, uint8_t *data, uint32_t data_size,
      StreamingMessageType message_type = StreamingMessageType::Message);

  /*!
   * @brief replay all queue from checkpoint, it's useful under FO
   * @param q_id_vec queue id vector
   * @param result offset vector
   * @return
   */
  void GetChannelOffset(const std::vector<ObjectID> &q_id_vec,
                        std::vector<uint64_t> &result);

  /*!
   * @brief fetch all channels back pressure ratio
   * @param channel_id_vec channel id vector
   * @param result backpressure ratio vector
   * @return
   */
  void GetChannelSetBackPressureRatio(const std::vector<ObjectID> &channel_id_vec,
                                      std::vector<double> &result);

  /*!
   * @brief send barrier to all channel
   * Note: there are user define data in barrier bundle
   * @param checkpoint_id : Its value is up to checkpoint operation. Actually it's useless
   * but logging.
   * @param barrier_id
   * @param data
   * @param data_size
   */
  void BroadcastBarrier(uint64_t checkpoint_id, uint64_t barrier_id, const uint8_t *data,
                        uint32_t data_size);

  /*
   * @brief broadcast partial barrier to all marked downstream.
   * @param global_barrier_id
   * @param partial_barrier_id
   * @param data
   * @param data_size
   */
  void BroadcastPartialBarrier(uint64_t global_barrier_id, uint64_t partial_barrier_id,
                               const uint8_t *data, uint32_t data_size);
  /*!
   * @brief To relieve stress from large source/input data, we define a new function
   * clear_check_point
   * in producer/writer class. Worker can invoke this function if and only if
   * notify_consumed each item
   * flag is passed in reader/consumer, which means writer's producing became more
   * rhythmical and reader
   * can't walk on old way anymore.
   * @param barrier_id: user-defined numerical checkpoint id
   */
  void ClearCheckpoint(uint64_t barrier_id);
  void ClearPartialCheckpoint(uint64_t global_barrier_id, uint64_t partial_barrier_id);

  void Run();

  void Stop();

  void Rescale(const std::vector<ObjectID> &id_vec) override;

  std::shared_ptr<BufferPool> GetBufferPool(const ObjectID &qid);
};

}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_WRITER_H
