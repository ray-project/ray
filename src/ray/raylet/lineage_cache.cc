#include "lineage_cache.h"
#include "ray/stats/stats.h"

#include <sstream>

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

void Lineage::RemoveChild(const TaskID &parent_id, const TaskID &child_id) {
  auto parent_it = children_.find(parent_id);
  RAY_CHECK(parent_it->second.erase(child_id) == 1);
  if (parent_it->second.empty()) {
    children_.erase(parent_it);
  }
}

void Lineage::AddChild(const TaskID &parent_id, const TaskID &child_id) {
  auto inserted = children_[parent_id].insert(child_id);
  RAY_CHECK(inserted.second);
}

bool Lineage::SetEntry(const Task &task, GcsStatus status) {
  // Get the status of the current entry at the key.
  auto task_id = task.GetTaskSpecification().TaskId();
  auto it = entries_.find(task_id);
  bool updated = false;
  std::unordered_set<TaskID> old_parents;
  if (it != entries_.end()) {
    if (it->second.SetStatus(status)) {
      // The task's spec may have changed, so record its old dependencies.
      old_parents = it->second.GetParentTaskIds();
      // SetStatus() would check if the new status is greater,
      // if it succeeds, go ahead to update the task field.
      it->second.UpdateTaskData(task);
      updated = true;
    }
  } else {
    LineageEntry new_entry(task, status);
    it = entries_.emplace(std::make_pair(task_id, std::move(new_entry))).first;
    updated = true;
  }

  // If the task data was updated, then record which tasks it depends on. Add
  // all new tasks that it depends on and remove any old tasks that it no
  // longer depends on.
  // TODO(swang): Updating the task data every time could be inefficient for
  // tasks that have lots of dependencies and/or large specs. A flag could be
  // passed in for tasks whose data has not changed.
  if (updated) {
    for (const auto &parent_id : it->second.GetParentTaskIds()) {
      if (old_parents.count(parent_id) == 0) {
        AddChild(parent_id, task_id);
      } else {
        old_parents.erase(parent_id);
      }
    }
    for (const auto &old_parent_id : old_parents) {
      RemoveChild(old_parent_id, task_id);
    }
  }
  return updated;
}

boost::optional<LineageEntry> Lineage::PopEntry(const TaskID &task_id) {
  auto entry = entries_.find(task_id);
  if (entry != entries_.end()) {
    LineageEntry entry = std::move(entries_.at(task_id));

    // Remove the task's dependencies.
    for (const auto &parent_id : entry.GetParentTaskIds()) {
      RemoveChild(parent_id, task_id);
    }
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

const std::unordered_set<TaskID> &Lineage::GetChildren(const TaskID &task_id) const {
  static const std::unordered_set<TaskID> empty_children;
  const auto it = children_.find(task_id);
  if (it != children_.end()) {
    return it->second;
  } else {
    return empty_children;
  }
}

LineageCache::LineageCache(const ClientID &client_id,
                           gcs::TableInterface<TaskID, protocol::Task> &task_storage,
                           gcs::PubsubInterface<TaskID> &task_pubsub,
                           uint64_t max_lineage_size)
    : client_id_(client_id), task_storage_(task_storage), task_pubsub_(task_pubsub) {}

/// A helper function to add some uncommitted lineage to the local cache.
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

  // Insert a copy of the entry into our cache.
  const auto &parent_ids = entry->GetParentTaskIds();
  // If the insert is successful, then continue the DFS. The insert will fail
  // if the new entry has an equal or lower GCS status than the current entry
  // in our cache. This also prevents us from traversing the same node twice.
  if (lineage_.SetEntry(entry->TaskData(), entry->GetStatus())) {
    subscribe_tasks.insert(task_id);
    for (const auto &parent_id : parent_ids) {
      AddUncommittedLineage(parent_id, uncommitted_lineage, subscribe_tasks);
    }
  }
}

bool LineageCache::AddWaitingTask(const Task &task, const Lineage &uncommitted_lineage) {
  auto task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "Add waiting task " << task_id << " on " << client_id_;

  // Merge the uncommitted lineage into the lineage cache. Collect the IDs of
  // tasks that we should subscribe to. These are all of the tasks that were
  // included in the uncommitted lineage that we did not already have in our
  // stash.
  std::unordered_set<TaskID> subscribe_tasks;
  AddUncommittedLineage(task_id, uncommitted_lineage, subscribe_tasks);
  // Add the submitted task to the lineage cache as UNCOMMITTED_WAITING. It
  // should be marked as UNCOMMITTED_READY once the task starts execution.
  auto added = lineage_.SetEntry(task, GcsStatus::UNCOMMITTED_WAITING);

  // Do not subscribe to the waiting task itself. We just added it as
  // UNCOMMITTED_WAITING, so the task is local.
  subscribe_tasks.erase(task_id);
  // Unsubscribe to the waiting task since we may have previously been
  // subscribed to it.
  UnsubscribeTask(task_id);
  // Subscribe to all other tasks that were included in the uncommitted lineage
  // and that were not already in the local stash. These tasks haven't been
  // committed yet and will be committed by a different node, so we will not
  // evict them until a notification for their commit is received.
  for (const auto &task_id : subscribe_tasks) {
    RAY_CHECK(SubscribeTask(task_id));
  }

  return added;
}

bool LineageCache::AddReadyTask(const Task &task) {
  const TaskID task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "Add ready task " << task_id << " on " << client_id_;

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
  RAY_LOG(DEBUG) << "Remove waiting task " << task_id << " on " << client_id_;
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

/// A helper function to get the uncommitted lineage of a task.
void GetUncommittedLineageHelper(const TaskID &task_id, const Lineage &lineage_from,
                                 Lineage &lineage_to, const ClientID &node_id) {
  // If the entry is not found in the lineage to merge, then we stop since
  // there is nothing to copy into the merged lineage.
  auto entry = lineage_from.GetEntry(task_id);
  if (!entry) {
    return;
  }
  // If this task has already been forwarded to this node, then we can stop.
  if (entry->WasExplicitlyForwarded(node_id)) {
    return;
  }

  // Insert a copy of the entry into lineage_to.  If the insert is successful,
  // then continue the DFS. The insert will fail if the new entry has an equal
  // or lower GCS status than the current entry in lineage_to. This also
  // prevents us from traversing the same node twice.
  if (lineage_to.SetEntry(entry->TaskData(), entry->GetStatus())) {
    for (const auto &parent_id : entry->GetParentTaskIds()) {
      GetUncommittedLineageHelper(parent_id, lineage_from, lineage_to, node_id);
    }
  }
}

Lineage LineageCache::GetUncommittedLineageOrDie(const TaskID &task_id,
                                                 const ClientID &node_id) const {
  Lineage uncommitted_lineage;
  // Add all uncommitted ancestors from the lineage cache to the uncommitted
  // lineage of the requested task.
  GetUncommittedLineageHelper(task_id, lineage_, uncommitted_lineage, node_id);
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
  RAY_CHECK_OK(
      task_storage_.Add(DriverID(task->TaskData().GetTaskSpecification().DriverId()),
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
    RAY_CHECK_OK(task_pubsub_.RequestNotifications(DriverID::nil(), task_id, client_id_));
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
    RAY_CHECK_OK(task_pubsub_.CancelNotifications(DriverID::nil(), task_id, client_id_));
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
  // If the entry has already been evicted, exit.
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
  RAY_LOG(DEBUG) << "Evicting task " << task_id << " on " << client_id_;
  lineage_.PopEntry(task_id);
  committed_tasks_.erase(commit_it);
  // Try to evict the children of the evict task. These are the tasks that have
  // a dependency on the evicted task.
  const auto children = lineage_.GetChildren(task_id);
  for (const auto &child_id : children) {
    EvictTask(child_id);
  }

  return;
}

void LineageCache::HandleEntryCommitted(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Task committed: " << task_id;
  auto entry = lineage_.GetEntry(task_id);
  if (!entry) {
    // The task has already been evicted due to a previous commit notification.
    return;
  }
  // Record the commit acknowledgement and attempt to evict the task.
  committed_tasks_.insert(task_id);
  EvictTask(task_id);
  // We got the notification about the task's commit, so no longer need any
  // more notifications.
  UnsubscribeTask(task_id);
}

const Task &LineageCache::GetTaskOrDie(const TaskID &task_id) const {
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

const Lineage &LineageCache::GetLineage() const { return lineage_; }

std::string LineageCache::DebugString() const {
  std::stringstream result;
  result << "LineageCache:";
  result << "\n- committed tasks: " << committed_tasks_.size();
  result << "\n- child map size: " << lineage_.GetChildrenSize();
  result << "\n- num subscribed tasks: " << subscribed_tasks_.size();
  result << "\n- lineage size: " << lineage_.GetEntries().size();
  return result.str();
}

void LineageCache::RecordMetrics() const {
  stats::LineageCacheStats().Record(committed_tasks_.size(),
                                    {{stats::ValueTypeKey, "num_committed_tasks"}});
  stats::LineageCacheStats().Record(lineage_.GetChildrenSize(),
                                    {{stats::ValueTypeKey, "num_children"}});
  stats::LineageCacheStats().Record(subscribed_tasks_.size(),
                                    {{stats::ValueTypeKey, "num_subscribed_tasks"}});
  stats::LineageCacheStats().Record(lineage_.GetEntries().size(),
                                    {{stats::ValueTypeKey, "num_lineages"}});
}

}  // namespace raylet

}  // namespace ray
