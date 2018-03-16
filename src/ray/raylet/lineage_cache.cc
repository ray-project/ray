#include "lineage_cache.h"

namespace ray {

namespace raylet {

LineageEntry::LineageEntry(const TaskID &entry_id, const Task &task, GcsStatus status)
    : entry_id_(entry_id), status_(status), task_(new Task(task)) {}

LineageEntry::LineageEntry(const ObjectID &entry_id, const Object &object,
                           GcsStatus status)
    : entry_id_(entry_id), status_(status), object_(new Object(object)) {}

LineageEntry::LineageEntry(const LineageEntry &entry)
    : entry_id_(entry.entry_id_), status_(entry.status_) {
  if (entry.task_ != nullptr) {
    task_ = std::unique_ptr<Task>(new Task(*entry.task_.get()));
  }
  if (entry.object_ != nullptr) {
    object_ = std::unique_ptr<Object>(new Object(*entry.object_.get()));
  }
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

const UniqueID &LineageEntry::GetUniqueId() const { return entry_id_; }

const std::vector<UniqueID> LineageEntry::GetParentIds() const {
  std::vector<UniqueID> parent_ids;
  if (task_ != nullptr) {
    // A task entry's parents are the task's arguments.
    parent_ids = task_->GetDependencies();
  } else {
    // An object entry's parent is the task that created it.
    parent_ids.push_back(ComputeTaskId(object_->GetObjectId()));
  }
  return parent_ids;
}

const Task &LineageEntry::TaskData() const {
  RAY_CHECK(IsTask());
  return *task_.get();
}

bool LineageEntry::IsTask() const { return (task_ != nullptr); }

flatbuffers::Offset<TaskFlatbuffer> LineageEntry::ToTaskFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  RAY_CHECK(IsTask());
  return task_->ToFlatbuffer(fbb);
}

flatbuffers::Offset<flatbuffers::String> LineageEntry::ToObjectFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  RAY_CHECK(!IsTask());
  return to_flatbuf(fbb, object_->GetObjectId());
}

Lineage::Lineage() {}

Lineage::Lineage(const ForwardTaskRequest &task_request) {
  // Deserialize and set entries for the uncommitted tasks.
  auto tasks = task_request.uncommitted_tasks();
  for (auto it = tasks->begin(); it != tasks->end(); it++) {
    auto task = Task(**it);
    LineageEntry entry(task.GetTaskSpecification().TaskId(), task,
                       GcsStatus_UNCOMMITTED_REMOTE);
    RAY_CHECK(SetEntry(std::move(entry)));
  }
  // Deserialize and set entries for the uncommitted objects.
  auto objects = task_request.uncommitted_objects();
  for (auto it = objects->begin(); it != objects->end(); it++) {
    auto object_id = from_flatbuf(**it);
    LineageEntry entry(object_id, Object(object_id), GcsStatus_UNCOMMITTED_REMOTE);
    RAY_CHECK(SetEntry(std::move(entry)));
  }
}

boost::optional<const LineageEntry &> Lineage::GetEntry(const UniqueID &entry_id) const {
  auto entry = entries_.find(entry_id);
  if (entry != entries_.end()) {
    return entry->second;
  } else {
    return boost::optional<const LineageEntry &>();
  }
}

bool Lineage::SetEntry(LineageEntry &&new_entry) {
  // Get the status of the current entry at the key.
  auto entry_id = new_entry.GetUniqueId();
  GcsStatus current_status = GcsStatus_NONE;
  auto current_entry = PopEntry(entry_id);
  if (current_entry) {
    current_status = current_entry->GetStatus();
  }

  if (current_status < new_entry.GetStatus()) {
    // If the new status is greater, then overwrite the current entry.
    entries_.emplace(std::make_pair(entry_id, std::move(new_entry)));
    return true;
  } else {
    // If the new status is not greater, then the new entry is invalid. Replace
    // the current entry at the key.
    entries_.emplace(std::make_pair(entry_id, std::move(*current_entry)));
    return false;
  }
}

boost::optional<LineageEntry> Lineage::PopEntry(const UniqueID &entry_id) {
  auto entry = entries_.find(entry_id);
  if (entry != entries_.end()) {
    LineageEntry entry = std::move(entries_.at(entry_id));
    entries_.erase(entry_id);
    return entry;
  } else {
    return boost::optional<LineageEntry>();
  }
}

const std::unordered_map<const UniqueID, LineageEntry, UniqueIDHasher>
    &Lineage::GetEntries() const {
  return entries_;
}

flatbuffers::Offset<ForwardTaskRequest> Lineage::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb, const TaskID &task_id) const {
  RAY_CHECK(GetEntry(task_id));
  // Serialize the task and object entries.
  std::vector<flatbuffers::Offset<TaskFlatbuffer>> uncommitted_tasks;
  std::vector<flatbuffers::Offset<flatbuffers::String>> uncommitted_objects;
  for (const auto &entry : entries_) {
    if (entry.second.IsTask()) {
      uncommitted_tasks.push_back(entry.second.ToTaskFlatbuffer(fbb));
    } else {
      uncommitted_objects.push_back(entry.second.ToObjectFlatbuffer(fbb));
    }
  }

  auto request = CreateForwardTaskRequest(fbb, to_flatbuf(fbb, task_id),
                                          fbb.CreateVector(uncommitted_tasks),
                                          fbb.CreateVector(uncommitted_objects));
  return request;
}

LineageCache::LineageCache(const ClientID &client_id,
                           gcs::Storage<TaskID, TaskFlatbuffer> &task_storage,
                           gcs::Storage<ObjectID, ObjectTableData> &object_storage)
    : client_id_(client_id),
      task_storage_(task_storage),
      object_storage_(object_storage) {}

/// A helper function to merge one lineage into another, in DFS order.
///
/// \param entry_id The current entry to merge from lineage_from into
///        lineage_to.
/// \param lineage_from The lineage to merge entries from. This lineage is
///        traversed by following each entry's parent pointers in DFS order,
///        until an entry is not found or the stopping condition is reached.
/// \param lineage_to The lineage to merge entries into.
/// \param stopping_condition A stopping condition for the DFS over
///        lineage_from. This should return true if the merge should stop.
void MergeLineageHelper(const UniqueID &entry_id, const Lineage &lineage_from,
                        Lineage &lineage_to,
                        std::function<bool(GcsStatus)> stopping_condition) {
  // If the entry is not found in the lineage to merge, then we stop since
  // there is nothing to copy into the merged lineage.
  auto entry = lineage_from.GetEntry(entry_id);
  if (!entry) {
    return;
  }
  // Check whether we should stop at this entry in the DFS.
  auto status = entry->GetStatus();
  if (stopping_condition(status)) {
    return;
  }

  // Insert a copy of the entry into lineage_to.
  auto entry_copy = LineageEntry(*entry);
  auto parent_ids = entry_copy.GetParentIds();
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
  LineageEntry task_entry(task_id, task, GcsStatus_UNCOMMITTED_WAITING);
  RAY_CHECK(lineage_.SetEntry(std::move(task_entry)));
  // Add the return values of the task to the lineage cache as
  // UNCOMMITTED_WAITING. They should be marked as UNCOMMITTED_READY once the
  // task completes.
  for (int64_t i = 0; i < task.GetTaskSpecification().NumReturns(); i++) {
    ObjectID return_id = task.GetTaskSpecification().ReturnId(i);
    LineageEntry object_entry(return_id, Object(return_id),
                              GcsStatus_UNCOMMITTED_WAITING);
    RAY_CHECK(lineage_.SetEntry(std::move(object_entry)));
  }
}

void LineageCache::AddReadyTask(const Task &task) {
  auto task_id = task.GetTaskSpecification().TaskId();
  auto new_entry = LineageEntry(task_id, task, GcsStatus_UNCOMMITTED_READY);
  RAY_CHECK(lineage_.SetEntry(std::move(new_entry)));
}

void LineageCache::AddReadyObject(const ObjectID &object_id, bool remote) {
  RAY_CHECK(remote == false)
      << "Lineage cache AddReadyObject for remote transfers not implemented";
  LineageEntry entry(object_id, Object(object_id), GcsStatus_UNCOMMITTED_READY);
  RAY_CHECK(lineage_.SetEntry(std::move(entry)));
}

void LineageCache::RemoveWaitingTask(const TaskID &task_id) {
  auto entry = lineage_.PopEntry(task_id);
  const Task &task = entry->TaskData();
  // It's only okay to remove a task that is waiting for execution.
  // TODO(swang): Is this necessarily true when there is reconstruction?
  RAY_CHECK(entry->GetStatus() == GcsStatus_UNCOMMITTED_WAITING);
  // Reset the status to REMOTE. We keep the task instead of removing it
  // completely in case another task is submitted locally that depends on this
  // one.
  entry->ResetStatus(GcsStatus_UNCOMMITTED_REMOTE);
  RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
  // Remove the return values of the task.
  for (int64_t i = 0; i < task.GetTaskSpecification().NumReturns(); i++) {
    ObjectID return_id = task.GetTaskSpecification().ReturnId(i);
    auto entry = lineage_.PopEntry(return_id);
    RAY_CHECK(entry->GetStatus() == GcsStatus_UNCOMMITTED_WAITING);
    // Reset the status to REMOTE for the return values, too.
    entry->ResetStatus(GcsStatus_UNCOMMITTED_REMOTE);
    RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
  }
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
    auto entry_id = pair.first;
    auto entry = pair.second;
    // Skip object entries.
    if (!entry.IsTask()) {
      continue;
    }
    // Skip task entries that are not ready to be written yet. These tasks
    // either have not started execution yet, are being executed on a remote
    // node, or have already been written to the GCS.
    if (entry.GetStatus() != GcsStatus_UNCOMMITTED_READY) {
      continue;
    }
    // Check if all arguments have been committed to the GCS before writing
    // this task.
    bool all_arguments_committed = true;
    for (const auto &parent_id : entry.GetParentIds()) {
      auto parent = lineage_.GetEntry(parent_id);
      // If a parent entry exists in the lineage cache but has not been
      // committed yet, then as far as we know, it's still in flight to the
      // GCS. Skip this task for now.
      if (parent && parent->GetStatus() != GcsStatus_COMMITTED) {
        all_arguments_committed = false;
        break;
      }
    }
    if (all_arguments_committed) {
      // All arguments have been committed to the GCS. Add this task to the
      // list of tasks to write back to the GCS.
      ready_task_ids.push_back(entry_id);
    }
  }

  // Write back all ready tasks whose arguments have been committed to the GCS.
  gcs::TaskTable::Callback task_callback = [this](
      ray::gcs::AsyncGcsClient *client, const UniqueID &id, std::shared_ptr<TaskT> data) {
    HandleEntryCommitted(id);
  };
  for (const auto &ready_task_id : ready_task_ids) {
    auto task = lineage_.GetEntry(ready_task_id);
    // TODO(swang): Make this better...
    flatbuffers::FlatBufferBuilder fbb;
    auto message = task->TaskData().ToFlatbuffer(fbb);
    fbb.Finish(message);
    auto task_data = std::make_shared<TaskT>();
    auto root = flatbuffers::GetRoot<TaskFlatbuffer>(fbb.GetBufferPointer());
    root->UnPackTo(task_data.get());
    RAY_CHECK_OK(task_storage_.Add(task->TaskData().GetTaskSpecification().DriverId(),
                                   ready_task_id, task_data, task_callback));

    // We successfully wrote the task, so mark it as committing.
    // TODO(swang): Use a batched interface and write with all object entries.
    auto entry = lineage_.PopEntry(ready_task_id);
    RAY_CHECK(entry->SetStatus(GcsStatus_COMMITTING));
    RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
  }

  // Find all objects that are READY and whose parent task is committed in the
  // GCS.
  std::vector<ObjectID> ready_object_ids;
  for (const auto &pair : lineage_.GetEntries()) {
    auto entry_id = pair.first;
    auto entry = pair.second;
    // Skip task entries.
    if (entry.IsTask()) {
      continue;
    }
    // Skip object entries that are not ready to be written yet. These objects
    // are waiting to be created by a task, are being created on a remote node,
    // or have already been written to the GCS.
    if (entry.GetStatus() != GcsStatus_UNCOMMITTED_READY) {
      continue;
    }
    // Check if the parent task that created this object has been written to the GCS.
    auto parent_ids = entry.GetParentIds();
    RAY_CHECK(parent_ids.size() == 1);
    auto parent = lineage_.GetEntry(parent_ids[0]);
    // If a parent entry exists in the lineage cache but has not been written
    // yet, then as far as we know, it's still in flight to the GCS. Skip this
    // entry for now. NOTE(swang): For objects, we only need to wait for the
    // parent to enter the COMMITTING status since a task and the objects that
    // it creates are always co-located on the same GCS shard. Therefore, we
    // can depend on the ordering at the shard to make sure that the task is
    // written before or simultaneously with the objects.
    if (parent && parent->GetStatus() < GcsStatus_COMMITTING) {
      continue;
    }
    // The parent task has been written to the GCS. Add this object to the list
    // of objects to write back to the GCS.
    ready_object_ids.push_back(entry_id);
  }

  // Write back all objects whose parent tasks have been written to the GCS.
  gcs::ObjectTable::Callback object_callback = [this](
      ray::gcs::AsyncGcsClient *client, const UniqueID &id,
      std::shared_ptr<ObjectTableDataT> data) { HandleEntryCommitted(id); };
  for (const auto &ready_object_id : ready_object_ids) {
    // TODO(swang): Fill out object metadata.
    auto data = std::make_shared<ObjectTableDataT>();
    data->managers.push_back(client_id_.hex());
    RAY_CHECK_OK(
        object_storage_.Add(JobID::nil(), ready_object_id, data, object_callback));

    // We successfully wrote the object, so mark it as committing.
    auto entry = lineage_.PopEntry(ready_object_id);
    RAY_CHECK(entry->SetStatus(GcsStatus_COMMITTING));
    RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
  }

  return ray::Status::OK();
}

boost::optional<LineageEntry> PopAncestors(
    const UniqueID &entry_id, Lineage &lineage,
    std::function<bool(GcsStatus)> check_condition) {
  auto entry = lineage.PopEntry(entry_id);
  if (!entry) {
    return boost::optional<LineageEntry>();
  }
  RAY_CHECK(check_condition(entry->GetStatus()));
  check_condition = [](GcsStatus status) {
    return (status == GcsStatus_UNCOMMITTED_REMOTE || status == GcsStatus_COMMITTED);
  };
  for (const auto &parent_id : entry->GetParentIds()) {
    PopAncestors(parent_id, lineage, check_condition);
  }
  return entry;
}

void LineageCache::HandleEntryCommitted(const UniqueID &entry_id) {
  auto first_condition = [](GcsStatus status) { return status == GcsStatus_COMMITTING; };
  auto entry = PopAncestors(entry_id, lineage_, first_condition);
  RAY_CHECK(entry->SetStatus(GcsStatus_COMMITTED));
  RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
}

} // namespace raylet

}  // namespace ray
