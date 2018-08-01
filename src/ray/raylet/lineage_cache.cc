#include "lineage_cache.h"

namespace ray {

namespace raylet {

LineageEntry::LineageEntry(const Task &task, GcsStatus status)
    : status_(status), task_(task) {}

GcsStatus LineageEntry::GetStatus() const { return status_; }

bool LineageEntry::SetStatus(GcsStatus new_status) {
  if (status_ < new_status) {
    status_ = new_status;
    return true;
  } else {
    return false;
  }
}

void LineageEntry::ResetStatus(GcsStatus new_status) {
  RAY_CHECK(new_status < status_);
  status_ = new_status;
}

const TaskID LineageEntry::GetEntryId() const {
  return task_.GetTaskSpecification().TaskId();
}

const std::unordered_set<TaskID> LineageEntry::GetParentTaskIds() const {
  std::unordered_set<TaskID> parent_ids;
  // A task's parents are the tasks that created its arguments.
  auto dependencies = task_.GetDependencies();
  for (auto &dependency : dependencies) {
    parent_ids.insert(ComputeTaskId(dependency));
  }
  return parent_ids;
}

const Task &LineageEntry::TaskData() const { return task_; }

Task &LineageEntry::TaskDataMutable() { return task_; }

Lineage::Lineage() {}

Lineage::Lineage(const protocol::ForwardTaskRequest &task_request) {
  // Deserialize and set entries for the uncommitted tasks.
  auto tasks = task_request.uncommitted_tasks();
  for (auto it = tasks->begin(); it != tasks->end(); it++) {
    const auto &task = **it;
    RAY_CHECK(SetEntry(task, GcsStatus::UNCOMMITTED_REMOTE));
  }
}

boost::optional<const LineageEntry &> Lineage::GetEntry(const TaskID &task_id) const {
  auto entry = entries_.find(task_id);
  if (entry != entries_.end()) {
    return entry->second;
  } else {
    return boost::optional<const LineageEntry &>();
  }
}

boost::optional<LineageEntry &> Lineage::GetEntryMutable(const TaskID &task_id) {
  auto entry = entries_.find(task_id);
  if (entry != entries_.end()) {
    return entry->second;
  } else {
    return boost::optional<LineageEntry &>();
  }
}

bool Lineage::SetEntry(const Task &task, GcsStatus status) {
  // Get the status of the current entry at the key.
  auto task_id = task.GetTaskSpecification().TaskId();
  auto current_entry = GetEntryMutable(task_id);
  if (current_entry) {
    if (current_entry->SetStatus(status)) {
      // SetStatus() would check if the new status is greater,
      // if it succeeds, go ahead to update the task field.
      current_entry->TaskDataMutable().CopyTaskExecutionSpec(task);
      return true;
    }
    return false;
  } else {
    LineageEntry new_entry(task, status);
    entries_.emplace(std::make_pair(task_id, std::move(new_entry)));
    return true;
  }
}

boost::optional<LineageEntry> Lineage::PopEntry(const TaskID &task_id) {
  auto entry = entries_.find(task_id);
  if (entry != entries_.end()) {
    LineageEntry entry = std::move(entries_.at(task_id));
    entries_.erase(task_id);
    return entry;
  } else {
    return boost::optional<LineageEntry>();
  }
}

const std::unordered_map<const TaskID, LineageEntry> &Lineage::GetEntries() const {
  return entries_;
}

flatbuffers::Offset<protocol::ForwardTaskRequest> Lineage::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb, const TaskID &task_id) const {
  RAY_CHECK(GetEntry(task_id));
  // Serialize the task and object entries.
  std::vector<flatbuffers::Offset<protocol::Task>> uncommitted_tasks;
  for (const auto &entry : entries_) {
    uncommitted_tasks.push_back(entry.second.TaskData().ToFlatbuffer(fbb));
  }

  auto request = protocol::CreateForwardTaskRequest(fbb, to_flatbuf(fbb, task_id),
                                                    fbb.CreateVector(uncommitted_tasks));
  return request;
}

LineageCache::LineageCache(const ClientID &client_id,
                           gcs::TableInterface<TaskID, protocol::Task> &task_storage,
                           gcs::PubsubInterface<TaskID> &task_pubsub,
                           uint64_t max_lineage_size)
    : client_id_(client_id),
      task_storage_(task_storage),
      task_pubsub_(task_pubsub),
      max_lineage_size_(max_lineage_size) {}

/// A helper function to merge one lineage into another, in DFS order.
///
/// \param task_id The current entry to merge from lineage_from into
/// lineage_to.
/// \param lineage_from The lineage to merge entries from. This lineage is
/// traversed by following each entry's parent pointers in DFS order,
/// until an entry is not found or the stopping condition is reached.
/// \param lineage_to The lineage to merge entries into.
/// \param stopping_condition A stopping condition for the DFS over
/// lineage_from. This should return true if the merge should stop.
void MergeLineageHelper(const TaskID &task_id, const Lineage &lineage_from,
                        Lineage &lineage_to,
                        std::function<bool(GcsStatus)> stopping_condition) {
  // If the entry is not found in the lineage to merge, then we stop since
  // there is nothing to copy into the merged lineage.
  auto entry = lineage_from.GetEntry(task_id);
  if (!entry) {
    return;
  }
  // Check whether we should stop at this entry in the DFS.
  auto status = entry->GetStatus();
  if (stopping_condition(status)) {
    return;
  }

  // Insert a copy of the entry into lineage_to.
  auto parent_ids = entry->GetParentTaskIds();
  // If the insert is successful, then continue the DFS. The insert will fail
  // if the new entry has an equal or lower GCS status than the current entry
  // in lineage_to. This also prevents us from traversing the same node twice.
  if (lineage_to.SetEntry(entry->TaskData(), entry->GetStatus())) {
    for (const auto &parent_id : parent_ids) {
      MergeLineageHelper(parent_id, lineage_from, lineage_to, stopping_condition);
    }
  }
}

void LineageCache::AddWaitingTask(const Task &task, const Lineage &uncommitted_lineage) {
  auto task_id = task.GetTaskSpecification().TaskId();
  // Merge the uncommitted lineage into the lineage cache.
  MergeLineageHelper(task_id, uncommitted_lineage, lineage_, [](GcsStatus status) {
    if (status != GcsStatus::NONE) {
      // We received the uncommitted lineage from a remote node, so make sure
      // that all entries in the lineage to merge have status
      // UNCOMMITTED_REMOTE.
      RAY_CHECK(status == GcsStatus::UNCOMMITTED_REMOTE);
    }
    // The only stopping condition is that an entry is not found.
    return false;
  });

  // If the task was previously remote, then we may have been subscribed to
  // it. Unsubscribe since we are now responsible for committing the task.
  auto entry = lineage_.GetEntry(task_id);
  if (entry) {
    RAY_CHECK(entry->GetStatus() == GcsStatus::UNCOMMITTED_REMOTE);
    UnsubscribeTask(task_id);
  }

  // Add the submitted task to the lineage cache as UNCOMMITTED_WAITING. It
  // should be marked as UNCOMMITTED_READY once the task starts execution.
  RAY_CHECK(lineage_.SetEntry(task, GcsStatus::UNCOMMITTED_WAITING));
}

void LineageCache::AddReadyTask(const Task &task) {
  const TaskID task_id = task.GetTaskSpecification().TaskId();

  // Tasks can only become READY if they were in WAITING.
  auto entry = lineage_.GetEntryMutable(task_id);
  RAY_CHECK(entry);
  RAY_CHECK(entry->GetStatus() == GcsStatus::UNCOMMITTED_WAITING);

  entry->SetStatus(GcsStatus::UNCOMMITTED_READY);
  // TaskSepc is immutable, just update TaskExecSpec.
  entry->TaskDataMutable().CopyTaskExecutionSpec(task);
  // Attempt to flush the task.
  bool flushed = FlushTask(task_id);
  if (!flushed) {
    // If we fail to flush the task here, due to uncommitted parents, then add
    // the task to a cache to be flushed in the future.
    uncommitted_ready_tasks_.insert(task_id);
  }
}

uint64_t LineageCache::CountUnsubscribedLineage(const TaskID &task_id) const {
  if (subscribed_tasks_.count(task_id) == 1) {
    return 0;
  }
  auto entry = lineage_.GetEntry(task_id);
  if (!entry) {
    return 0;
  }
  uint64_t cnt = 1;
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    cnt += CountUnsubscribedLineage(parent_id);
  }
  return cnt;
}

void LineageCache::RemoveWaitingTask(const TaskID &task_id) {
  auto entry = lineage_.GetEntryMutable(task_id);
  if (!entry) {
    return;
  }

  // It's only okay to remove a task that is waiting for execution.
  // TODO(swang): Is this necessarily true when there is reconstruction?
  RAY_CHECK(entry->GetStatus() == GcsStatus::UNCOMMITTED_WAITING);
  // Reset the status to REMOTE. We keep the task instead of removing it
  // completely in case another task is submitted locally that depends on this
  // one.
  entry->ResetStatus(GcsStatus::UNCOMMITTED_REMOTE);

  // Request a notification for every max_lineage_size_ tasks,
  // so that the task and its uncommitted lineage can be evicted
  // once the commit notification is received.
  // By doing this, we make sure that the unevicted lineage won't be more than
  // max_lineage_size_, and the number of subscribed tasks won't be more than
  // N / max_lineage_size_, where N is the size of the task chain.
  // NOTE(swang): The number of entries in the uncommitted lineage also
  // includes local tasks that haven't been committed yet, not just remote
  // tasks, so this is an overestimate.
  if (CountUnsubscribedLineage(task_id) > max_lineage_size_) {
    // Since this task was in state WAITING, check that we were not
    // already subscribed to the task.
    RAY_CHECK(SubscribeTask(task_id));
  }
}

Lineage LineageCache::GetUncommittedLineage(const TaskID &task_id) const {
  Lineage uncommitted_lineage;
  // Add all uncommitted ancestors from the lineage cache to the uncommitted
  // lineage of the requested task.
  MergeLineageHelper(task_id, lineage_, uncommitted_lineage, [](GcsStatus status) {
    // The stopping condition for recursion is that the entry has been
    // committed to the GCS.
    return false;
  });
  return uncommitted_lineage;
}

bool LineageCache::FlushTask(const TaskID &task_id) {
  auto entry = lineage_.GetEntry(task_id);
  RAY_CHECK(entry);
  RAY_CHECK(entry->GetStatus() == GcsStatus::UNCOMMITTED_READY);

  // Check if all arguments have been committed to the GCS before writing
  // this task.
  bool all_arguments_committed = true;
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    auto parent = lineage_.GetEntry(parent_id);
    // If a parent entry exists in the lineage cache but has not been
    // committed yet, then as far as we know, it's still in flight to the
    // GCS. Skip this task for now.
    if (parent) {
      RAY_CHECK(parent->GetStatus() != GcsStatus::UNCOMMITTED_WAITING)
          << "Children should not become ready to flush before their parents.";
      // Request notifications about the parent entry's commit in the GCS if
      // the parent is remote. Otherwise, the parent is local and will
      // eventually be flushed. In either case, once we receive a
      // notification about the task's commit via HandleEntryCommitted, then
      // this task will be ready to write on the next call to Flush().
      if (parent->GetStatus() == GcsStatus::UNCOMMITTED_REMOTE) {
        SubscribeTask(parent_id);
      }
      all_arguments_committed = false;
      // Track the fact that this task is dependent on a parent that hasn't yet
      // been committed, for fast lookup. Once all parents are committed, the
      // child will be flushed.
      uncommitted_ready_children_[parent_id].insert(task_id);
    }
  }
  if (all_arguments_committed) {
    gcs::raylet::TaskTable::WriteCallback task_callback = [this](
        ray::gcs::AsyncGcsClient *client, const TaskID &id, const protocol::TaskT &data) {
      HandleEntryCommitted(id);
    };
    auto task = lineage_.GetEntry(task_id);
    // TODO(swang): Make this better...
    flatbuffers::FlatBufferBuilder fbb;
    auto message = task->TaskData().ToFlatbuffer(fbb);
    fbb.Finish(message);
    auto task_data = std::make_shared<protocol::TaskT>();
    auto root = flatbuffers::GetRoot<protocol::Task>(fbb.GetBufferPointer());
    root->UnPackTo(task_data.get());
    RAY_CHECK_OK(task_storage_.Add(task->TaskData().GetTaskSpecification().DriverId(),
                                   task_id, task_data, task_callback));

    // We successfully wrote the task, so mark it as committing.
    // TODO(swang): Use a batched interface and write with all object entries.
    auto entry = lineage_.GetEntryMutable(task_id);
    RAY_CHECK(entry);
    RAY_CHECK(entry->SetStatus(GcsStatus::COMMITTING));
  }
  return all_arguments_committed;
}

void LineageCache::Flush() {
  // Iterate through all tasks that are PLACEABLE.
  for (auto it = uncommitted_ready_tasks_.begin();
       it != uncommitted_ready_tasks_.end();) {
    bool flushed = FlushTask(*it);
    // Erase the task from the cache of uncommitted ready tasks.
    if (flushed) {
      it = uncommitted_ready_tasks_.erase(it);
    } else {
      it++;
    }
  }
}

bool LineageCache::SubscribeTask(const TaskID &task_id) {
  auto inserted = subscribed_tasks_.insert(task_id);
  bool unsubscribed = inserted.second;
  if (unsubscribed) {
    // Request notifications for the task if we haven't already requested
    // notifications for it.
    RAY_CHECK_OK(task_pubsub_.RequestNotifications(JobID::nil(), task_id, client_id_));
  }
  // Return whether we were previously unsubscribed to this task and are now
  // subscribed.
  return unsubscribed;
}

bool LineageCache::UnsubscribeTask(const TaskID &task_id) {
  auto it = subscribed_tasks_.find(task_id);
  bool subscribed = (it != subscribed_tasks_.end());
  if (subscribed) {
    // Cancel notifications for the task if we previously requested
    // notifications for it.
    RAY_CHECK_OK(task_pubsub_.CancelNotifications(JobID::nil(), task_id, client_id_));
    subscribed_tasks_.erase(it);
  }
  // Return whether we were previously subscribed to this task and are now
  // unsubscribed.
  return subscribed;
}

boost::optional<LineageEntry> LineageCache::EvictTask(const TaskID &task_id) {
  auto entry = lineage_.PopEntry(task_id);
  if (!entry) {
    return entry;
  }

  // Stop listening for notifications about this task.
  UnsubscribeTask(task_id);

  // Try to flush the children of the committed task. These are the tasks that
  // have a dependency on the committed task.
  auto children_entry = uncommitted_ready_children_.find(task_id);
  if (children_entry != uncommitted_ready_children_.end()) {
    // Get the children of the committed task that are uncommitted but ready.
    auto children = std::move(children_entry->second);
    uncommitted_ready_children_.erase(children_entry);

    // Try to flush the children.  If all of the child's parents are committed,
    // then the child will be flushed here.
    for (const auto &child_id : children) {
      bool flushed = FlushTask(child_id);
      // Erase the child task from the cache of uncommitted ready tasks.
      if (flushed) {
        auto erased = uncommitted_ready_tasks_.erase(child_id);
        RAY_CHECK(erased == 1);
      }
    }
  }

  return entry;
}

void LineageCache::EvictRemoteLineage(const TaskID &task_id) {
  // Remove the ancestor task.
  auto entry = EvictTask(task_id);
  if (!entry) {
    return;
  }
  // Tasks are committed in data dependency order per node, so the only
  // ancestors of a committed task should be other remote tasks.
  auto status = entry->GetStatus();
  RAY_CHECK(status == GcsStatus::UNCOMMITTED_REMOTE);
  // Recurse and remove this task's ancestors.
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    EvictRemoteLineage(parent_id);
  }
}

void LineageCache::HandleEntryCommitted(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "task committed: " << task_id;
  auto entry = EvictTask(task_id);
  if (!entry) {
    // The committed entry has already been evicted. Check that the committed
    // entry does not have any dependent tasks, since we should've already
    // attempted to flush these tasks on the first commit notification.
    RAY_CHECK(uncommitted_ready_children_.count(task_id) == 0);
    // Check that we already unsubscribed from the task when handling the
    // first commit notification.
    RAY_CHECK(subscribed_tasks_.count(task_id) == 0);
    // Do nothing if the committed entry has already been evicted.
    return;
  }

  // Evict the committed task's uncommitted lineage. Since local tasks are
  // written in data dependency order, the uncommitted lineage should only
  // include remote tasks, i.e. tasks that were committed by a different node.
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    EvictRemoteLineage(parent_id);
  }
}

}  // namespace raylet

}  // namespace ray
