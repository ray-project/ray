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

const std::unordered_set<UniqueID, UniqueIDHasher> LineageEntry::GetParentTaskIds()
    const {
  std::unordered_set<UniqueID, UniqueIDHasher> parent_ids;
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
    auto task = Task(**it);
    LineageEntry entry(task, GcsStatus_UNCOMMITTED_REMOTE);
    RAY_CHECK(SetEntry(std::move(entry)));
  }
}

boost::optional<const LineageEntry &> Lineage::GetEntry(const UniqueID &task_id) const {
  auto entry = entries_.find(task_id);
  if (entry != entries_.end()) {
    return entry->second;
  } else {
    return boost::optional<const LineageEntry &>();
  }
}

boost::optional<LineageEntry &> Lineage::GetEntryMutable(const UniqueID &task_id) {
  auto entry = entries_.find(task_id);
  if (entry != entries_.end()) {
    return entry->second;
  } else {
    return boost::optional<LineageEntry &>();
  }
}

bool Lineage::SetEntry(LineageEntry &&new_entry) {
  // Get the status of the current entry at the key.
  auto task_id = new_entry.GetEntryId();
  GcsStatus current_status = GcsStatus_NONE;
  auto current_entry = PopEntry(task_id);
  if (current_entry) {
    current_status = current_entry->GetStatus();
  }

  if (current_status < new_entry.GetStatus()) {
    // If the new status is greater, then overwrite the current entry.
    entries_.emplace(std::make_pair(task_id, std::move(new_entry)));
    return true;
  } else {
    // If the new status is not greater, then the new entry is invalid. Replace
    // the current entry at the key.
    entries_.emplace(std::make_pair(task_id, std::move(*current_entry)));
    return false;
  }
}

boost::optional<LineageEntry> Lineage::PopEntry(const UniqueID &task_id) {
  auto entry = entries_.find(task_id);
  if (entry != entries_.end()) {
    LineageEntry entry = std::move(entries_.at(task_id));
    entries_.erase(task_id);
    return entry;
  } else {
    return boost::optional<LineageEntry>();
  }
}

const std::unordered_map<const UniqueID, LineageEntry, UniqueIDHasher>
    &Lineage::GetEntries() const {
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

LineageCache::LineageCache(gcs::TableInterface<TaskID, protocol::Task> &task_storage)
    : task_storage_(task_storage) {}

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
void MergeLineageHelper(const UniqueID &task_id, const Lineage &lineage_from,
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
  LineageEntry entry_copy = *entry;
  auto parent_ids = entry_copy.GetParentTaskIds();
  // If the insert is successful, then continue the DFS. The insert will fail
  // if the new entry has an equal or lower GCS status than the current entry
  // in lineage_to. This also prevents us from traversing the same node twice.
  if (lineage_to.SetEntry(std::move(entry_copy))) {
    for (const auto &parent_id : parent_ids) {
      MergeLineageHelper(parent_id, lineage_from, lineage_to, stopping_condition);
    }
  }
}

void LineageCache::AddWaitingTask(const Task &task, const Lineage &uncommitted_lineage) {
  auto task_id = task.GetTaskSpecification().TaskId();
  // Merge the uncommitted lineage into the lineage cache.
  MergeLineageHelper(task_id, uncommitted_lineage, lineage_, [](GcsStatus status) {
    if (status != GcsStatus_NONE) {
      // We received the uncommitted lineage from a remote node, so make sure
      // that all entries in the lineage to merge have status
      // UNCOMMITTED_REMOTE.
      RAY_CHECK(status == GcsStatus_UNCOMMITTED_REMOTE);
    }
    // The only stopping condition is that an entry is not found.
    return false;
  });

  // Add the submitted task to the lineage cache as UNCOMMITTED_WAITING. It
  // should be marked as UNCOMMITTED_READY once the task starts execution.
  LineageEntry task_entry(task, GcsStatus_UNCOMMITTED_WAITING);
  RAY_CHECK(lineage_.SetEntry(std::move(task_entry)));
}

void LineageCache::AddReadyTask(const Task &task) {
  auto new_entry = LineageEntry(task, GcsStatus_UNCOMMITTED_READY);
  RAY_CHECK(lineage_.SetEntry(std::move(new_entry)));
}

void LineageCache::RemoveWaitingTask(const TaskID &task_id) {
  auto entry = lineage_.PopEntry(task_id);
  // It's only okay to remove a task that is waiting for execution.
  // TODO(swang): Is this necessarily true when there is reconstruction?
  RAY_CHECK(entry->GetStatus() == GcsStatus_UNCOMMITTED_WAITING);
  // Reset the status to REMOTE. We keep the task instead of removing it
  // completely in case another task is submitted locally that depends on this
  // one.
  entry->ResetStatus(GcsStatus_UNCOMMITTED_REMOTE);
  RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
}

Lineage LineageCache::GetUncommittedLineage(const TaskID &task_id) const {
  Lineage uncommitted_lineage;
  // Add all uncommitted ancestors from the lineage cache to the uncommitted
  // lineage of the requested task.
  MergeLineageHelper(task_id, lineage_, uncommitted_lineage, [](GcsStatus status) {
    // The stopping condition for recursion is that the entry has been
    // committed to the GCS.
    return status == GcsStatus_COMMITTED;
  });
  return uncommitted_lineage;
}

Status LineageCache::Flush() {
  // Find all tasks that are READY and whose arguments have been committed in the GCS.
  std::vector<TaskID> ready_task_ids;
  for (const auto &pair : lineage_.GetEntries()) {
    auto task_id = pair.first;
    auto entry = pair.second;
    // Skip task entries that are not ready to be written yet. These tasks
    // either have not started execution yet, are being executed on a remote
    // node, or have already been written to the GCS.
    if (entry.GetStatus() != GcsStatus_UNCOMMITTED_READY) {
      continue;
    }
    // Check if all arguments have been committed to the GCS before writing
    // this task.
    bool all_arguments_committed = true;
    for (const auto &parent_id : entry.GetParentTaskIds()) {
      auto parent = lineage_.GetEntry(parent_id);
      // If a parent entry exists in the lineage cache but has not been
      // committed yet, then as far as we know, it's still in flight to the
      // GCS. Skip this task for now.
      if (parent && parent->GetStatus() != GcsStatus_COMMITTED) {
        // TODO(swang): Once GCS notifications for the task table are ready,
        // request notification for commit of the parent task here.
        all_arguments_committed = false;
        break;
      }
    }
    if (all_arguments_committed) {
      // All arguments have been committed to the GCS. Add this task to the
      // list of tasks to write back to the GCS.
      ready_task_ids.push_back(task_id);
    }
  }

  // Write back all ready tasks whose arguments have been committed to the GCS.
  gcs::raylet::TaskTable::WriteCallback task_callback = [this](
      ray::gcs::AsyncGcsClient *client, const TaskID &id,
      const std::shared_ptr<protocol::TaskT> data) { HandleEntryCommitted(id); };
  for (const auto &ready_task_id : ready_task_ids) {
    auto task = lineage_.GetEntry(ready_task_id);
    // TODO(swang): Make this better...
    flatbuffers::FlatBufferBuilder fbb;
    auto message = task->TaskData().ToFlatbuffer(fbb);
    fbb.Finish(message);
    auto task_data = std::make_shared<protocol::TaskT>();
    auto root = flatbuffers::GetRoot<protocol::Task>(fbb.GetBufferPointer());
    root->UnPackTo(task_data.get());
    RAY_CHECK_OK(task_storage_.Add(task->TaskData().GetTaskSpecification().DriverId(),
                                   ready_task_id, task_data, task_callback));

    // We successfully wrote the task, so mark it as committing.
    // TODO(swang): Use a batched interface and write with all object entries.
    auto entry = lineage_.PopEntry(ready_task_id);
    RAY_CHECK(entry->SetStatus(GcsStatus_COMMITTING));
    RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
  }

  return ray::Status::OK();
}

void PopAncestorTasks(const UniqueID &task_id, Lineage &lineage) {
  auto entry = lineage.PopEntry(task_id);
  if (!entry) {
    return;
  }
  auto status = entry->GetStatus();
  RAY_CHECK(status == GcsStatus_UNCOMMITTED_REMOTE || status == GcsStatus_COMMITTED);
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    PopAncestorTasks(parent_id, lineage);
  }
}

void LineageCache::HandleEntryCommitted(const UniqueID &task_id) {
  auto entry = lineage_.PopEntry(task_id);
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    PopAncestorTasks(parent_id, lineage_);
  }
  RAY_CHECK(entry->SetStatus(GcsStatus_COMMITTED));
  RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
}

}  // namespace raylet

}  // namespace ray
