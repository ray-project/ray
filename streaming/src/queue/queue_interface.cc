#include "ray/util/util.h"

#include "queue_interface.h"

namespace ray {
namespace streaming {

/// code below is interface implementation of streaming queue
StreamingQueueWriter::StreamingQueueWriter(const ActorID& actor_id)
    : actor_id_(actor_id) {
  upstream_service_ = ray::streaming::UpstreamService::GetService(nullptr, actor_id_);
}

void StreamingQueueWriter::CreateQueue(const ObjectID &queue_id, int64_t max_size,
                                         ActorID &peer_actor_id) {
  STREAMING_LOG(INFO) << "CreateQueue qid: " << queue_id << " data_size: " << max_size;
  if (upstream_service_->UpstreamQueueExists(queue_id)) {
    RAY_LOG(INFO) << "StreamingQueueWriter::CreateQueue duplicate!!!";
    return;
  }

  upstream_service_->AddPeerActor(queue_id, peer_actor_id);
  auto queue = upstream_service_->CreateUpstreamQueue(queue_id, actor_id_, peer_actor_id, max_size);
  STREAMING_CHECK(queue != nullptr);

  std::vector<ObjectID> queue_ids, failed_queues;
  queue_ids.push_back(queue_id);
  upstream_service_->WaitQueues(queue_ids, 10*1000, failed_queues, DOWNSTREAM);
}

Status StreamingQueueWriter::SetQueueEvictionLimit(const ObjectID &queue_id,
                                                   uint64_t eviction_limit) {
  std::shared_ptr<WriterQueue> queue = upstream_service_->GetUpQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);

  queue->SetQueueEvictionLimit(eviction_limit);
  return Status::OK();
}

void StreamingQueueWriter::GetMinConsumedSeqID(const ObjectID &queue_id,
                                               uint64_t &min_consumed_id) {
  std::shared_ptr<WriterQueue> queue = upstream_service_->GetUpQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);

  min_consumed_id = queue->GetMinConsumedSeqID();
}

Status StreamingQueueWriter::PushQueueItem(const ObjectID &queue_id, uint64_t seq_id,
                                           uint8_t *data, uint32_t data_size,
                                           uint64_t timestamp) {
  STREAMING_LOG(INFO) << "QueueWriter::PushQueueItem:"
                       << " qid: " << queue_id << " seq_id: " << seq_id
                       << " data_size: " << data_size;
  std::shared_ptr<WriterQueue> queue = upstream_service_->GetUpQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);

  Status status = queue->Push(seq_id, data, data_size, timestamp, false);
  if (status.IsOutOfMemory()) {
    Status st = queue->TryEvictItems();
    if (!st.ok()) {
      STREAMING_LOG(INFO) << "Evict fail.";
      return st;
    }

    st = queue->Push(seq_id, data, data_size, timestamp, false);
    STREAMING_LOG(INFO) << "After evict PushQueueItem: " << st.ok();
    return st;
  }

  queue->Send();
  return status;
}

Status StreamingQueueWriter::DeleteQueue(const ObjectID &queue_id) {
  return Status::OK();
}

StreamingQueueReader::StreamingQueueReader(const ActorID& actor_id)
    : actor_id_(actor_id) {
  downstream_service_ = ray::streaming::DownstreamService::GetService(nullptr, actor_id_);
}

/// Create queue and pull queue (if needed), synchronously.
bool StreamingQueueReader::GetQueue(const ObjectID &queue_id,
                                    uint64_t start_seq_id, ActorID &peer_actor_id) {
  STREAMING_LOG(INFO) << "GetQueue qid: " << queue_id << " start_seq_id: " << start_seq_id;
  if (downstream_service_->DownstreamQueueExists(queue_id)) {
    RAY_LOG(INFO) << "StreamingQueueReader::GetQueue duplicate!!!";
    return true;
  }

  downstream_service_->AddPeerActor(queue_id, peer_actor_id);

  STREAMING_LOG(INFO) << "Create ReaderQueue " << queue_id
                      << " pull from start_seq_id: " << start_seq_id;
  downstream_service_->CreateDownstreamQueue(queue_id, actor_id_, peer_actor_id);
  return true;
}

Status StreamingQueueReader::GetQueueItem(const ObjectID &queue_id, uint8_t *&data,
                                          uint32_t &data_size, uint64_t &seq_id,
                                          uint64_t timeout_ms) {
  STREAMING_LOG(INFO) << "GetQueueItem qid: " << queue_id;
  auto queue = downstream_service_->GetDownQueue(queue_id);
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
  auto queue = downstream_service_->GetDownQueue(queue_id);
  queue->OnConsumed(seq_id);
}

Status StreamingQueueReader::DeleteQueue(const ObjectID &queue_id) {
  return Status::OK();
}

}  // namespace streaming
}  // namespace ray
