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
    parent_task_ids_.insert(dependency.TaskId());
  }
}

const Task &LineageEntry::TaskData() const { return task_; }

Task &LineageEntry::TaskDataMutable() { return task_; }

void LineageEntry::UpdateTaskData(const Task &task) {
  task_.CopyTaskExecutionSpec(task);
  ComputeParentTaskIds();
}

Lineage::Lineage() {}

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
  if (it != entries_.end()) {
    if (it->second.SetStatus(status)) {
      // We assume here that the new `task` has the same fields as the task
      // already in the lineage cache. If this is not true, then it is
      // necessary to update the task data of the existing lineage cache entry
      // with LineageEntry::UpdateTaskData.
      updated = true;
    }
  } else {
    LineageEntry new_entry(task, status);
    it = entries_.emplace(std::make_pair(task_id, std::move(new_entry))).first;
    updated = true;

    // New task data was added to the local cache, so record which tasks it
    // depends on. Add all new tasks that it depends on.
    for (const auto &parent_id : it->second.GetParentTaskIds()) {
      AddChild(parent_id, task_id);
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
                           gcs::TableInterface<TaskID, TaskTableData> &task_storage,
                           gcs::PubsubInterface<TaskID> &task_pubsub,
                           uint64_t max_lineage_size)
    : client_id_(client_id), task_storage_(task_storage), task_pubsub_(task_pubsub) {}

/// A helper function to add some uncommitted lineage to the local cache.
void LineageCache::AddUncommittedLineage(const TaskID &task_id,
                                         const Lineage &uncommitted_lineage) {
  RAY_LOG(DEBUG) << "Adding uncommitted task " << task_id << " on " << client_id_;
  // If the entry is not found in the lineage to merge, then we stop since
  // there is nothing to copy into the merged lineage.
  auto entry = uncommitted_lineage.GetEntry(task_id);
  if (!entry) {
    return;
  }
  RAY_CHECK(entry->GetStatus() == GcsStatus::UNCOMMITTED);

  // Insert a copy of the entry into our cache.
  const auto &parent_ids = entry->GetParentTaskIds();
  // If the insert is successful, then continue the DFS. The insert will fail
  // if the new entry has an equal or lower GCS status than the current entry
  // in our cache. This also prevents us from traversing the same node twice.
  if (lineage_.SetEntry(entry->TaskData(), entry->GetStatus())) {
    RAY_CHECK(SubscribeTask(task_id));
    for (const auto &parent_id : parent_ids) {
      AddUncommittedLineage(parent_id, uncommitted_lineage);
    }
  }
}

bool LineageCache::CommitTask(const Task &task) {
  const TaskID task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "Committing task " << task_id << " on " << client_id_;

  if (lineage_.SetEntry(task, GcsStatus::UNCOMMITTED) ||
      lineage_.GetEntry(task_id)->GetStatus() == GcsStatus::UNCOMMITTED) {
    // Attempt to flush the task if the task is uncommitted.
    FlushTask(task_id);
    return true;
  } else {
    // The task was already committing (COMMITTING).
    return false;
  }
}

void LineageCache::FlushAllUncommittedTasks() {
  size_t num_flushed = 0;
  for (const auto &entry : lineage_.GetEntries()) {
    // Flush all tasks that have not yet committed.
    if (entry.second.GetStatus() == GcsStatus::UNCOMMITTED) {
      RAY_CHECK(UnsubscribeTask(entry.first));
      FlushTask(entry.first);
      num_flushed++;
    }
  }

  RAY_LOG(DEBUG) << "Flushed " << num_flushed << " uncommitted tasks";
}

void LineageCache::MarkTaskAsForwarded(const TaskID &task_id, const ClientID &node_id) {
  RAY_CHECK(!node_id.IsNil());
  auto entry = lineage_.GetEntryMutable(task_id);
  if (entry) {
    entry->MarkExplicitlyForwarded(node_id);
  }
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

Lineage LineageCache::GetUncommittedLineage(const TaskID &task_id,
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
    if (entry) {
      RAY_CHECK(uncommitted_lineage.SetEntry(entry->TaskData(), entry->GetStatus()));
    }
  }
  return uncommitted_lineage;
}

void LineageCache::FlushTask(const TaskID &task_id) {
  auto entry = lineage_.GetEntryMutable(task_id);
  RAY_CHECK(entry);
  RAY_CHECK(entry->GetStatus() < GcsStatus::COMMITTING);

  gcs::raylet::TaskTable::WriteCallback task_callback =
      [this](ray::gcs::RedisGcsClient *client, const TaskID &id,
             const TaskTableData &data) { HandleEntryCommitted(id); };
  auto task = lineage_.GetEntry(task_id);
  auto task_data = std::make_shared<TaskTableData>();
  task_data->mutable_task()->mutable_task_spec()->CopyFrom(
      task->TaskData().GetTaskSpecification().GetMessage());
  task_data->mutable_task()->mutable_task_execution_spec()->CopyFrom(
      task->TaskData().GetTaskExecutionSpec().GetMessage());
  RAY_CHECK_OK(task_storage_.Add(JobID(task->TaskData().GetTaskSpecification().JobId()),
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
    RAY_CHECK_OK(task_pubsub_.RequestNotifications(JobID::Nil(), task_id, client_id_));
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
    RAY_CHECK_OK(task_pubsub_.CancelNotifications(JobID::Nil(), task_id, client_id_));
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
