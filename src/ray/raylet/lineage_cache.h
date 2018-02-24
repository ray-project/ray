#ifndef LINEAGE_CACHE_H
#define LINEAGE_CACHE_H

namespace ray {

class Lineage {
};

class LineageCacheEntry {
 private:
  // TODO(swang): This should be an enum of the state of the entry - goes from
  // completely local, to dirty, to in flight, to committed.
  bool dirty_;
};

class LineageCacheTaskEntry : public LineageCacheEntry {
};
class LineageCacheObjectEntry : public LineageCacheEntry {
};

class LineageCache {
 public:
  LineageCache(/* TODO(swang): Pass in the policy (interface?) and a GCS client. */);
  // Add a task and its object outputs asynchronously to the GCS. This
  // overwrites the task’s execution edges.
  ray::Status AddTask(const Task &task);
  // Add a task and its uncommitted lineage asynchronously to the GCS.
  ray::Status AddTask(const Task &task, const Lineage &uncommitted_lineage);
  // Add this node as an object location, to be asynchronously committed to GCS.
  ray::Status AddObjectLocation(const ObjectID &object);
  // Get the uncommitted lineage of an object.
  Lineage &GetUncommittedLineage(const ObjectID &object);
  // Asynchronously write any tasks and object locations that have been added
  // since the last flush to the GCS. When each write is acknowledged, its
  // entry will be marked as committed.
  void Flush();
 private:
  std::unordered_map<TaskID, LineageCacheTaskEntry, UniqueIDHasher> task_table_;
  std::unordered_map<ObjectID, LineageCacheObjectEntry, UniqueIDHasher> object_table_;
};

};

#endif
