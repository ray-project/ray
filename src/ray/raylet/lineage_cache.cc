#include "lineage_cache.h"

namespace ray {

namespace raylet {

LineageEntry::LineageEntry(const Task &task, GcsStatus status)
    : status_(status), task_(task) {
  ComputeParentTaskIds();
}

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

void LineageEntry::MarkExplicitlyForwarded(const ClientID &node_id) {
  forwarded_to_.insert(node_id);
}

bool LineageEntry::WasExplicitlyForwarded(const ClientID &node_id) const {
  return forwarded_to_.find(node_id) != forwarded_to_.end();
}

const TaskID LineageEntry::GetEntryId() const {
  return task_.GetTaskSpecification().TaskId();
}

const std::unordered_set<TaskID> &LineageEntry::GetParentTaskIds() const {
  return parent_task_ids_;
}

void LineageEntry::ComputeParentTaskIds() {
  parent_task_ids_.clear();
  // A task's parents are the tasks that created its arguments.
  for (const auto &dependency : task_.GetDependencies()) {
    parent_task_ids_.insert(ComputeTaskId(dependency));
  }
}

const Task &LineageEntry::TaskData() const { return task_; }

Task &LineageEntry::TaskDataMutable() { return task_; }

void LineageEntry::UpdateTaskData(const Task &task) {
  task_.CopyTaskExecutionSpec(task);
  ComputeParentTaskIds();
}

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
      current_entry->UpdateTaskData(task);
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
                        std::function<bool(const LineageEntry &)> stopping_condition) {
  // If the entry is not found in the lineage to merge, then we stop since
  // there is nothing to copy into the merged lineage.
  auto entry = lineage_from.GetEntry(task_id);
  if (!entry) {
    return;
  }
  // Check whether we should stop at this entry in the DFS.
  if (stopping_condition(entry.get())) {
    return;
  }

  // Insert a copy of the entry into lineage_to.
  const auto &parent_ids = entry->GetParentTaskIds();
  // If the insert is successful, then continue the DFS. The insert will fail
  // if the new entry has an equal or lower GCS status than the current entry
  // in lineage_to. This also prevents us from traversing the same node twice.
  if (lineage_to.SetEntry(entry->TaskData(), entry->GetStatus())) {
    for (const auto &parent_id : parent_ids) {
      MergeLineageHelper(parent_id, lineage_from, lineage_to, stopping_condition);
    }
  }
}

bool LineageCache::AddWaitingTask(const Task &task, const Lineage &uncommitted_lineage) {
  auto task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "add waiting task " << task_id << " on " << client_id_;
  // Merge the uncommitted lineage into the lineage cache.
  MergeLineageHelper(task_id, uncommitted_lineage, lineage_,
                     [](const LineageEntry &entry) {
                       if (entry.GetStatus() != GcsStatus::NONE) {
                         // We received the uncommitted lineage from a remote node, so
                         // make sure that all entries in the lineage to merge have
                         // status UNCOMMITTED_REMOTE.
                         RAY_CHECK(entry.GetStatus() == GcsStatus::UNCOMMITTED_REMOTE);
                       }
                       // The only stopping condition is that an entry is not found.
                       return false;
                     });

  auto entry = lineage_.GetEntry(task_id);
  if (entry) {
    if (entry->GetStatus() == GcsStatus::UNCOMMITTED_REMOTE) {
      // The task was previously remote, so we may have been subscribed to it.
      // Unsubscribe since we are now responsible for committing the task.
      UnsubscribeTask(task_id);
    }
  }

  // Add the submitted task to the lineage cache as UNCOMMITTED_WAITING. It
  // should be marked as UNCOMMITTED_READY once the task starts execution.
  return lineage_.SetEntry(task, GcsStatus::UNCOMMITTED_WAITING);
}

bool LineageCache::AddReadyTask(const Task &task) {
  const TaskID task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "add ready task " << task_id << " on " << client_id_;

  // Set the task to READY.
  if (lineage_.SetEntry(task, GcsStatus::UNCOMMITTED_READY)) {
    // Attempt to flush the task.
    bool flushed = FlushTask(task_id);
    if (!flushed) {
      // If we fail to flush the task here, due to uncommitted parents, then add
      // the task to a cache to be flushed in the future.
      uncommitted_ready_tasks_.insert(task_id);
    }
    return true;
  } else {
    // The task was already ready to be committed (UNCOMMITTED_READY) or
    // committing (COMMITTING).
    return false;
  }
}

uint64_t LineageCache::CountUnsubscribedLineage(const TaskID &task_id,
                                                std::unordered_set<TaskID> &seen) const {
  if (seen.count(task_id) == 1) {
    return 0;
  }
  seen.insert(task_id);
  if (subscribed_tasks_.count(task_id) == 1) {
    return 0;
  }
  auto entry = lineage_.GetEntry(task_id);
  // Only count tasks that are remote. Tasks that are local will be evicted
  // once they are committed in the GCS, along with their lineage.
  if (!entry || entry->GetStatus() != GcsStatus::UNCOMMITTED_REMOTE) {
    return 0;
  }
  uint64_t cnt = 1;
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    cnt += CountUnsubscribedLineage(parent_id, seen);
  }
  return cnt;
}

bool LineageCache::RemoveWaitingTask(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "remove waiting task " << task_id << " on " << client_id_;
  auto entry = lineage_.GetEntryMutable(task_id);
  if (!entry) {
    // The task was already evicted.
    return false;
  }

  // If the task is already not in WAITING status, then exit. This should only
  // happen when there are two copies of the task executing at the node, due to
  // a spurious reconstruction. Then, either the task is already past WAITING
  // status, in which case it will be committed, or it is in
  // UNCOMMITTED_REMOTE, in which case it was already removed.
  if (entry->GetStatus() != GcsStatus::UNCOMMITTED_WAITING) {
    return false;
  }

  // Reset the status to REMOTE. We keep the task instead of removing it
  // completely in case another task is submitted locally that depends on this
  // one.
  entry->ResetStatus(GcsStatus::UNCOMMITTED_REMOTE);

  // Subscribe to the task if necessary. We do this if it has any local
  // children that must be written to the GCS, or if its uncommitted remote
  // lineage is too large.
  if (uncommitted_ready_children_.find(task_id) != uncommitted_ready_children_.end()) {
    // Subscribe to the task if it has any children in UNCOMMITTED_READY. We
    // will attempt to flush its children once we receive a notification for
    // this task's commit.  Since this task was in state WAITING, check that we
    // were not already subscribed to the task.
    RAY_CHECK(SubscribeTask(task_id));
  } else {
    // Check if the uncommitted remote lineage is too large.  Request a
    // notification for every max_lineage_size_ tasks, so that the task and its
    // uncommitted lineage can be evicted once the commit notification is
    // received.  By doing this, we make sure that the unevicted lineage won't
    // be more than max_lineage_size_, and the number of subscribed tasks won't
    // be more than N / max_lineage_size_, where N is the size of the task
    // chain.
    // NOTE(swang): The number of entries in the uncommitted lineage also
    // includes local tasks that haven't been committed yet, not just remote
    // tasks, so this is an overestimate.
    std::unordered_set<TaskID> seen;
    auto count = CountUnsubscribedLineage(task_id, seen);
    if (count >= max_lineage_size_) {
      // Since this task was in state WAITING, check that we were not
      // already subscribed to the task.
      RAY_CHECK(SubscribeTask(task_id));
    }
  }
  // The task was successfully reset to UNCOMMITTED_REMOTE.
  return true;
}

void LineageCache::MarkTaskAsForwarded(const TaskID &task_id, const ClientID &node_id) {
  RAY_CHECK(!node_id.is_nil());
  lineage_.GetEntryMutable(task_id)->MarkExplicitlyForwarded(node_id);
}

Lineage LineageCache::GetUncommittedLineage(const TaskID &task_id,
                                            const ClientID &node_id) const {
  Lineage uncommitted_lineage;
  // Add all uncommitted ancestors from the lineage cache to the uncommitted
  // lineage of the requested task.
  MergeLineageHelper(
      task_id, lineage_, uncommitted_lineage, [&](const LineageEntry &entry) {
        // The stopping condition for recursion is that the entry has
        // been committed to the GCS or has already been forwarded.
        return entry.WasExplicitlyForwarded(node_id);
      });
  // The lineage always includes the requested task id, so add the task if it
  // wasn't already added. The requested task may not have been added if it was
  // already explicitly forwarded to this node before.
  if (uncommitted_lineage.GetEntries().empty()) {
    auto entry = lineage_.GetEntry(task_id);
    RAY_CHECK(entry);
    RAY_CHECK(uncommitted_lineage.SetEntry(entry->TaskData(), entry->GetStatus()));
  }
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
  RAY_LOG(DEBUG) << "evicting task " << task_id << " on " << client_id_;
  auto entry = lineage_.PopEntry(task_id);
  if (!entry) {
    // The entry has already been evicted. Check that the entry does not have
    // any dependent tasks, since we should've already attempted to flush these
    // tasks on the first eviction.
    RAY_CHECK(uncommitted_ready_children_.count(task_id) == 0);
    // Check that we already unsubscribed from the task when handling the
    // first eviction.
    RAY_CHECK(subscribed_tasks_.count(task_id) == 0);
    // Do nothing if the entry has already been evicted.
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
  auto entry = lineage_.GetEntry(task_id);
  if (!entry) {
    return;
  }
  // Only evict tasks that are remote. Other tasks, and their lineage, will be
  // evicted once they are committed.
  if (entry->GetStatus() == GcsStatus::UNCOMMITTED_REMOTE) {
    // Remove the ancestor task.
    auto evicted_entry = EvictTask(task_id);
    // Recurse and remove this task's ancestors.
    for (const auto &parent_id : evicted_entry->GetParentTaskIds()) {
      EvictRemoteLineage(parent_id);
    }
  }
}

void LineageCache::HandleEntryCommitted(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "task committed: " << task_id;
  auto entry = EvictTask(task_id);
  if (!entry) {
    // The task has already been evicted due to a previous commit notification,
    // or because one of its descendants was committed.
    return;
  }

  // Evict the committed task's uncommitted lineage. Since local tasks are
  // written in data dependency order, the uncommitted lineage should only
  // include remote tasks, i.e. tasks that were committed by a different node.
  // In case of reconstruction, the uncommitted lineage may also include local
  // tasks that were resubmitted. These tasks are not evicted.
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    EvictRemoteLineage(parent_id);
  }
}

const Task &LineageCache::GetTask(const TaskID &task_id) const {
  const auto &entries = lineage_.GetEntries();
  auto it = entries.find(task_id);
  RAY_CHECK(it != entries.end());
  return it->second.TaskData();
}

bool LineageCache::ContainsTask(const TaskID &task_id) const {
  const auto &entries = lineage_.GetEntries();
  auto it = entries.find(task_id);
  return it != entries.end();
}

}  // namespace raylet

}  // namespace ray
