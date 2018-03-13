#include "lineage_cache.h"

namespace ray {

namespace raylet {

LineageCacheEntry::LineageCacheEntry(const TaskID &entry_id, const Task &task,
                                     GcsStatus status)
    : entry_id_(entry_id), status_(status), task_(new Task(task)) {}

LineageCacheEntry::LineageCacheEntry(const ObjectID &entry_id, const Object &object,
                                     GcsStatus status)
    : entry_id_(entry_id), status_(status), object_(new Object(object)) {}

LineageCacheEntry::LineageCacheEntry(const LineageCacheEntry &entry)
    : entry_id_(entry.entry_id_), status_(entry.status_) {
  if (entry.task_ != nullptr) {
    task_ = std::unique_ptr<Task>(new Task(*entry.task_.get()));
  }
  if (entry.object_ != nullptr) {
    object_ = std::unique_ptr<Object>(new Object(*entry.object_.get()));
  }
}

GcsStatus LineageCacheEntry::GetStatus() const { return status_; }

bool LineageCacheEntry::SetStatus(GcsStatus new_status) {
  if (status_ < new_status) {
    status_ = new_status;
    return true;
  } else {
    return false;
  }
}

const UniqueID &LineageCacheEntry::GetUniqueId() const { return entry_id_; }

const std::vector<UniqueID> LineageCacheEntry::GetParentIds() const {
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

bool LineageCacheEntry::IsTask() const { return (task_ != nullptr); }

flatbuffers::Offset<TaskFlatbuffer> LineageCacheEntry::ToTaskFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  RAY_CHECK(IsTask());
  return task_->ToFlatbuffer(fbb);
}

flatbuffers::Offset<flatbuffers::String> LineageCacheEntry::ToObjectFlatbuffer(
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
    LineageCacheEntry entry(task.GetTaskSpecification().TaskId(), task,
                            GcsStatus_UNCOMMITTED_REMOTE);
    RAY_CHECK(SetEntry(std::move(entry)));
  }
  // Deserialize and set entries for the uncommitted objects.
  auto objects = task_request.uncommitted_objects();
  for (auto it = objects->begin(); it != objects->end(); it++) {
    auto object_id = from_flatbuf(**it);
    LineageCacheEntry entry(object_id, Object(object_id), GcsStatus_UNCOMMITTED_REMOTE);
    RAY_CHECK(SetEntry(std::move(entry)));
  }
}

boost::optional<const LineageCacheEntry &> Lineage::GetEntry(
    const UniqueID &entry_id) const {
  auto entry = entries_.find(entry_id);
  if (entry != entries_.end()) {
    return entry->second;
  } else {
    return boost::optional<const LineageCacheEntry &>();
  }
}

bool Lineage::SetEntry(LineageCacheEntry &&new_entry) {
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

boost::optional<LineageCacheEntry> Lineage::PopEntry(const UniqueID &entry_id) {
  auto entry = entries_.find(entry_id);
  if (entry != entries_.end()) {
    LineageCacheEntry entry = std::move(entries_.at(entry_id));
    entries_.erase(entry_id);
    return entry;
  } else {
    return boost::optional<LineageCacheEntry>();
  }
}

const std::unordered_map<const UniqueID, LineageCacheEntry, UniqueIDHasher>
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

LineageCache::LineageCache() {}

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
  auto entry_copy = LineageCacheEntry(*entry);
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
  LineageCacheEntry task_entry(task_id, task, GcsStatus_UNCOMMITTED_WAITING);
  RAY_CHECK(lineage_.SetEntry(std::move(task_entry)));
  // Add the return values of the task to the lineage cache as
  // UNCOMMITTED_WAITING. They should be marked as UNCOMMITTED_READY once the
  // task completes.
  for (int64_t i = 0; i < task.GetTaskSpecification().NumReturns(); i++) {
    ObjectID return_id = task.GetTaskSpecification().ReturnId(i);
    LineageCacheEntry object_entry(return_id, Object(return_id),
                                   GcsStatus_UNCOMMITTED_WAITING);
    RAY_CHECK(lineage_.SetEntry(std::move(object_entry)));
  }
}

void LineageCache::AddReadyTask(const Task &task) {
  auto task_id = task.GetTaskSpecification().TaskId();
  auto new_entry = LineageCacheEntry(task_id, task, GcsStatus_UNCOMMITTED_READY);
  RAY_CHECK(lineage_.SetEntry(std::move(new_entry)));
}

void LineageCache::AddReadyObject(const ObjectID &object_id, bool remote) {
  RAY_CHECK(remote == false)
      << "Lineage cache AddReadyObject for remote transfers not implemented";
  LineageCacheEntry entry(object_id, Object(object_id), GcsStatus_UNCOMMITTED_READY);
  RAY_CHECK(lineage_.SetEntry(std::move(entry)));
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
  throw std::runtime_error("method not implemented");
  return ray::Status::OK();
}

} // namespace raylet

}  // namespace ray
