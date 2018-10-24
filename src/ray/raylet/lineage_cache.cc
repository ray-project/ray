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
    : client_id_(client_id), task_storage_(task_storage), task_pubsub_(task_pubsub) {}

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

/// A helper function to merge one lineage into another, in DFS order.
void LineageCache::AddUncommittedLineage(const TaskID &task_id,
                                         const Lineage &uncommitted_lineage,
                                         std::unordered_set<TaskID> &subscribe_tasks) {
  // If the entry is not found in the lineage to merge, then we stop since
  // there is nothing to copy into the merged lineage.
  auto entry = uncommitted_lineage.GetEntry(task_id);
  if (!entry) {
    return;
  }
  RAY_CHECK(entry->GetStatus() == GcsStatus::UNCOMMITTED_REMOTE);

  // Insert a copy of the entry into lineage_to.
  const auto &parent_ids = entry->GetParentTaskIds();
  // If the insert is successful, then continue the DFS. The insert will fail
  // if the new entry has an equal or lower GCS status than the current entry
  // in lineage_to. This also prevents us from traversing the same node twice.
  if (lineage_.SetEntry(entry->TaskData(), entry->GetStatus())) {
    subscribe_tasks.insert(task_id);
    for (const auto &parent_id : parent_ids) {
      children_[parent_id].insert(task_id);
      AddUncommittedLineage(parent_id, uncommitted_lineage, subscribe_tasks);
    }
  }
}

bool LineageCache::AddWaitingTask(const Task &task, const Lineage &uncommitted_lineage) {
  auto task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "add waiting task " << task_id << " on " << client_id_;

  // Merge the uncommitted lineage into the lineage cache. Collect the IDs of
  // tasks that we should subscribe to. These are all of the tasks that were
  // included in the uncommitted lineage that we did not already have in our
  // stash.
  std::unordered_set<TaskID> subscribe_tasks;
  AddUncommittedLineage(task_id, uncommitted_lineage, subscribe_tasks);

  // Do not subscribe to the waiting task.
  subscribe_tasks.erase(task_id);
  // Subscribe to all other tasks that were included in the uncommitted lineage
  // and that were not already in the local stash. These tasks haven't been
  // committed yet and will be committed by a different node, so we will not
  // evict them until a notification for their commit is received.
  for (const auto &task_id : subscribe_tasks) {
    RAY_CHECK(SubscribeTask(task_id));
  }

  // Add the submitted task to the lineage cache as UNCOMMITTED_WAITING. It
  // should be marked as UNCOMMITTED_READY once the task starts execution.
  auto added = lineage_.SetEntry(task, GcsStatus::UNCOMMITTED_WAITING);
  auto entry = lineage_.GetEntry(task_id);
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    children_[parent_id].insert(task_id);
  }

  return added;
}

bool LineageCache::AddReadyTask(const Task &task) {
  const TaskID task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "add ready task " << task_id << " on " << client_id_;

  // Set the task to READY.
  if (lineage_.SetEntry(task, GcsStatus::UNCOMMITTED_READY)) {
    // Attempt to flush the task.
    FlushTask(task_id);
    return true;
  } else {
    // The task was already ready to be committed (UNCOMMITTED_READY) or
    // committing (COMMITTING).
    return false;
  }
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
  // The task is now remote, so subscribe to the task to make sure that we'll
  // eventually clean it up.
  RAY_CHECK(SubscribeTask(task_id));
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

void LineageCache::FlushTask(const TaskID &task_id) {
  auto entry = lineage_.GetEntryMutable(task_id);
  RAY_CHECK(entry);
  RAY_CHECK(entry->GetStatus() == GcsStatus::UNCOMMITTED_READY);

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
  RAY_CHECK(entry->SetStatus(GcsStatus::COMMITTING));
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

void LineageCache::EvictTask(const TaskID &task_id) {
  // If we haven't received a commit for this task yet, do not evict.
  auto commit_it = committed_tasks_.find(task_id);
  if (commit_it == committed_tasks_.end()) {
    return;
  }
  // If the entry ahs already been evicted, exit.
  auto entry = lineage_.GetEntry(task_id);
  if (!entry) {
    return;
  }
  // Only evict tasks that we were subscribed to or that we were committing.
  if (!(entry->GetStatus() == GcsStatus::UNCOMMITTED_REMOTE ||
        entry->GetStatus() == GcsStatus::COMMITTING)) {
    return;
  }
  // Entries cannot be safely evicted until their parents are all evicted.
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    if (ContainsTask(parent_id)) {
      return;
    }
  }

  // Evict the task.
  RAY_LOG(DEBUG) << "evicting task " << task_id << " on " << client_id_;
  lineage_.PopEntry(task_id);
  committed_tasks_.erase(commit_it);
  UnsubscribeTask(task_id);
  // Try to evict the children of the evict task. These are the tasks that have
  // a dependency on the evicted task.
  auto children_entry = children_.find(task_id);
  if (children_entry != children_.end()) {
    // Get the children of the evicted task.
    auto children = std::move(children_entry->second);
    children_.erase(children_entry);
    // Try to evict the children.  If all of the child's parents are evicted,
    // then the child will be evicted here.
    for (const auto &child_id : children) {
      EvictTask(child_id);
    }
  }

  return;
}

void LineageCache::HandleEntryCommitted(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "task committed: " << task_id;
  auto entry = lineage_.GetEntry(task_id);
  if (!entry) {
    // The task has already been evicted due to a previous commit notification.
    return;
  }
  // Record the commit acknowledgement and attempt to evict the task.
  committed_tasks_.insert(task_id);
  EvictTask(task_id);
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

size_t LineageCache::NumEntries() const { return lineage_.GetEntries().size(); }

}  // namespace raylet

}  // namespace ray
