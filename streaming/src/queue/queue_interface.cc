#include "ray/util/util.h"

#include "queue_interface.h"

namespace ray {
namespace streaming {

// Queue factory of writer and reader
std::shared_ptr<StreamingQueueWriter> CreateQueueWriter(
    const JobID &job_id,
    const std::vector<ObjectID> &queue_ids, CoreWorker *core_worker,
    ray::RayFunction &async_func, ray::RayFunction &sync_func) {
  std::shared_ptr<StreamingQueueWriter> instance;

  instance = std::make_shared<StreamingQueueWriter>(core_worker, async_func, sync_func);
  return instance;
}

std::shared_ptr<StreamingQueueReader> CreateQueueReader(
    const JobID &job_id,
    const std::vector<ObjectID> &queue_ids, CoreWorker *core_worker,
    ray::RayFunction &async_func, ray::RayFunction &sync_func) {
  std::shared_ptr<StreamingQueueReader> instance;

  instance = std::make_shared<StreamingQueueReader>(core_worker, async_func, sync_func);
  return instance;
}

/// code below is interface implementation of streaming queue
StreamingQueueWriter::StreamingQueueWriter(CoreWorker *core_worker,
                                           RayFunction &async_func,
                                           RayFunction &sync_func)
    : core_worker_(core_worker), async_func_(async_func), sync_func_(sync_func) {
  actor_id_ = core_worker_->GetWorkerContext().GetCurrentActorID();
  queue_manager_ = ray::streaming::QueueManager::GetInstance(actor_id_);
}

void StreamingQueueWriter::CreateQueue(const ObjectID &queue_id, int64_t data_size,
                                         ActorID &peer_actor_id) {
  STREAMING_LOG(INFO) << "CreateQueue qid: " << queue_id << " data_size: " << data_size;
  if (queue_manager_->UpstreamQueueExists(queue_id)) {
    RAY_LOG(INFO) << "StreamingQueueWriter::CreateQueue duplicate!!!";
    return;
  }

  queue_manager_->AddOutTransport(
      queue_id, std::make_shared<ray::streaming::Transport>(
                    core_worker_, peer_actor_id, async_func_, sync_func_));

  auto queue = queue_manager_->CreateUpstreamQueue(queue_id, actor_id_, peer_actor_id, data_size);
  STREAMING_CHECK(queue != nullptr);

  queue_manager_->UpdateUpActor(queue_id, actor_id_);
  queue_manager_->UpdateDownActor(queue_id, peer_actor_id);

  std::vector<ObjectID> queue_ids, failed_queues;
  queue_ids.push_back(queue_id);
  WaitQueuesInCluster(queue_ids, 10*1000, failed_queues);
}

Status StreamingQueueWriter::SetQueueEvictionLimit(const ObjectID &queue_id,
                                                   uint64_t eviction_limit) {
  std::shared_ptr<WriterQueue> queue = queue_manager_->GetUpQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);

  queue->SetQueueEvictionLimit(eviction_limit);
  return Status::OK();
}

void StreamingQueueWriter::WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids,
                                               int64_t timeout_ms,
                                               std::vector<ObjectID> &failed_queues) {
  STREAMING_LOG(INFO) << "QueueWriter::WaitQueues timeout_ms: " << timeout_ms;
  queue_manager_->WaitQueues(queue_ids, timeout_ms, failed_queues, DOWNSTREAM);
}

void StreamingQueueWriter::GetMinConsumedSeqID(const ObjectID &queue_id,
                                               uint64_t &min_consumed_id) {
  std::shared_ptr<WriterQueue> queue = queue_manager_->GetUpQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);

  min_consumed_id = queue->GetMinConsumedSeqID();
}

Status StreamingQueueWriter::PushQueueItem(const ObjectID &queue_id, uint64_t seq_id,
                                           uint8_t *data, uint32_t data_size,
                                           uint64_t timestamp) {
  STREAMING_LOG(INFO) << "QueueWriter::PushSync:"
                       << " qid: " << queue_id << " seq_id: " << seq_id
                       << " data_size: " << data_size;
  std::shared_ptr<WriterQueue> queue = queue_manager_->GetUpQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);

  Status status = queue->Push(seq_id, data, data_size, timestamp, false);
  if (status.IsOutOfMemory()) {
    Status st = queue->TryEvictItems();
    if (!st.ok()) {
      STREAMING_LOG(INFO) << "Evict fail.";
      return st;
    }

    st = queue->Push(seq_id, data, data_size, timestamp, false);
    STREAMING_LOG(INFO) << "After evict PushSync: " << st.ok();
    return st;
  }

  queue->Send();
  return status;
}

Status StreamingQueueWriter::DeleteQueue(const ObjectID &queue_id) {
  return Status::OK();
}

StreamingQueueReader::StreamingQueueReader(CoreWorker *core_worker,
                                           RayFunction &async_func,
                                           RayFunction &sync_func)
    : core_worker_(core_worker), async_func_(async_func), sync_func_(sync_func) {
  actor_id_ = core_worker_->GetWorkerContext().GetCurrentActorID();
  queue_manager_ = ray::streaming::QueueManager::GetInstance(actor_id_);
}

/// Create queue and pull queue (if needed), synchronously.
bool StreamingQueueReader::GetQueue(const ObjectID &queue_id, int64_t timeout_ms,
                                    uint64_t start_seq_id, ActorID &peer_actor_id) {
  STREAMING_LOG(INFO) << "GetQueue qid: " << queue_id << " start_seq_id: " << start_seq_id;
  if (queue_manager_->DownstreamQueueExists(queue_id)) {
    RAY_LOG(INFO) << "StreamingQueueReader::GetQueue duplicate!!!";
    return true;
  }
  // ActorHandle *actor_handle_ptr = reinterpret_cast<ActorHandle *>(actor_handle);
  queue_manager_->AddOutTransport(
      queue_id, std::make_shared<ray::streaming::Transport>(
                    core_worker_, peer_actor_id, async_func_, sync_func_));
  queue_manager_->UpdateUpActor(queue_id, peer_actor_id);
  queue_manager_->UpdateDownActor(queue_id, actor_id_);

  STREAMING_LOG(INFO) << "Create ReaderQueue " << queue_id
                      << " pull from start_seq_id: " << start_seq_id;
  queue_manager_->CreateDownstreamQueue(queue_id, actor_id_, peer_actor_id);
  return true;
}

Status StreamingQueueReader::GetQueueItem(const ObjectID &queue_id, uint8_t *&data,
                                          uint32_t &data_size, uint64_t &seq_id,
                                          uint64_t timeout_ms) {
  STREAMING_LOG(INFO) << "GetQueueItem qid: " << queue_id;
  auto queue = queue_manager_->GetDownQueue(queue_id);
  QueueItem item = queue->PopPendingBlockTimeout(timeout_ms * 1000);
  if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
    STREAMING_LOG(INFO) << "GetQueueItem timeout.";
    data = nullptr;
    data_size = 0;
    seq_id = QUEUE_INVALID_SEQ_ID;
    return Status::OK();
  }

  data = item.Buffer()->Data();
  seq_id = item.SeqId();
  data_size = item.Buffer()->Size();

  STREAMING_LOG(DEBUG) << "GetQueueItem qid: " << queue_id
                       << " seq_id: " << seq_id
                       << " msg_id: " << item.MaxMsgId()
                       << " data_size: " << data_size;
  return Status::OK();
}

void StreamingQueueReader::NotifyConsumedItem(const ObjectID &queue_id, uint64_t seq_id) {
  STREAMING_LOG(DEBUG) << "QueueReader::NotifyConsumedItem";
  auto queue = queue_manager_->GetDownQueue(queue_id);
  queue->OnConsumed(seq_id);
}

void StreamingQueueReader::WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids,
                                               int64_t timeout_ms,
                                               std::vector<ObjectID> &failed_queues) {
  // We don't need to wait queue in reader side under StreamingQueue
  // queue_reader_->WaitQueues(queue_ids, timeout_ms, failed_queues);
  failed_queues.clear();
}

Status StreamingQueueReader::DeleteQueue(const ObjectID &queue_id) {
  return Status::OK();
}

}  // namespace streaming
}  // namespace ray
