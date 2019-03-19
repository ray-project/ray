#include "lineage_cache.h"

#include <sstream>

namespace ray {

namespace raylet {

LineageEntry::LineageEntry(const Task &task)
    : task_(task) {
  ComputeParentTaskIds();
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
    RAY_CHECK(AddTask(task));
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

bool Lineage::AddTask(const Task &task) {
  // Get the status of the current entry at the key.
  auto task_id = task.GetTaskSpecification().TaskId();
  auto it = entries_.find(task_id);
  bool added = false;
  std::unordered_set<TaskID> old_parents;
  if (it == entries_.end()) {
    LineageEntry new_entry(task);
    it = entries_.emplace(std::make_pair(task_id, std::move(new_entry))).first;
    added = true;
  }

  // If the task data was updated, then record which tasks it depends on. Add
  // all new tasks that it depends on and remove any old tasks that it no
  // longer depends on.
  // TODO(swang): Updating the task data every time could be inefficient for
  // tasks that have lots of dependencies and/or large specs. A flag could be
  // passed in for tasks whose data has not changed.
  if (added) {
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
  return added;
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
    : client_id_(client_id), task_storage_(task_storage), task_pubsub_(task_pubsub),
      num_evicted_tasks_(0) {}

/// A helper function to add some uncommitted lineage to the local cache.
void LineageCache::AddUncommittedLineage(const TaskID &task_id,
                                         const Lineage &uncommitted_lineage) {
  // If the entry is not found in the lineage to merge, then we stop since
  // there is nothing to copy into the merged lineage.
  auto entry = uncommitted_lineage.GetEntry(task_id);
  if (!entry) {
    return;
  }

  // Insert a copy of the entry into our cache.
  const auto &parent_ids = entry->GetParentTaskIds();
  // If the insert is successful, then continue the DFS. The insert will fail
  // if the new entry has an equal or lower GCS status than the current entry
  // in our cache. This also prevents us from traversing the same node twice.
  if (lineage_.AddTask(entry->TaskData())) {
    for (const auto &parent_id : parent_ids) {
      AddUncommittedLineage(parent_id, uncommitted_lineage);
    }
  }
}

bool LineageCache::AddTask(const Task &task, const Lineage &uncommitted_lineage) {
  const TaskID task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "Adding task " << task_id << " on " << client_id_;

  if (lineage_.AddTask(task)) {
    AddUncommittedLineage(task_id, uncommitted_lineage);
    // Attempt to flush the task.
    FlushTask(task_id);
    return true;
  } else {
    // The task was already present.
    return false;
  }
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

  // Insert a copy of the entry into lineage_to. If the insert is successful,
  // then continue the DFS. The insert will fail if the new entry has an equal
  // or lower GCS status than the current entry in lineage_to. This also
  // prevents us from traversing the same node twice.
  if (lineage_to.AddTask(entry->TaskData())) {
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
    RAY_CHECK(uncommitted_lineage.AddTask(entry->TaskData()));
  }
  return uncommitted_lineage;
}

void LineageCache::FlushTask(const TaskID &task_id) {
  auto entry = lineage_.GetEntryMutable(task_id);
  RAY_CHECK(entry);

  gcs::TaskTable::WriteCallback task_callback = [this](
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
      task_storage_.Add(JobID(task->TaskData().GetTaskSpecification().DriverId()),
                        task_id, task_data, task_callback));

  // We successfully wrote the task.
  // TODO(swang): Use a batched interface and write with all object entries.
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
  num_evicted_tasks_++;
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
  result << "\n- lineage size: " << lineage_.GetEntries().size();
  result << "\n- num evicted tasks: " << num_evicted_tasks_;
  return result.str();
}

}  // namespace raylet

}  // namespace ray
