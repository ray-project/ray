#ifndef RAY_RAYLET_ACTOR_TRANSPORT_H
#define RAY_RAYLET_ACTOR_TRANSPORT_H

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/gcs/format/gcs_generated.h"
#include "ray/id.h"
#include "ray/raylet/raylet_client.h"
#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

class InlineResult {
 public:
  InlineResult(const int len, uint8_t *data) : inline_len(len), inline_data(data){};

  bool IsInline() { return true; }

  const int inline_len;
  const uint8_t *inline_data;

  // TODO(ekl) support non-inline results
};

/// \class ActorTransport
///
/// A virtual class that represents a strategy for executing actor tasks.
class ActorTransport {
 public:
  /////////// Client interface ///////////

  /// Enqueue an actor task for execution. The transport implementation may delay dispatch
  /// of the
  /// task to enable batching.
  ///
  /// \param task_spec A pointer to the task. Ownership is transferred to the transport
  /// class.
  virtual void SubmitTask(std::unique_ptr<TaskSpecification> task_spec) = 0;

  /// Retrieve the results of tasks executed with SendTask(). This call blocks until the
  /// results
  /// of all the given ids are available.
  ///
  /// \param ids List of ids to retrieve.
  ///
  /// \return List of result references. Small results will be included inline; larger
  /// results
  /// are represented as an ObjectID that can be retrieved from the object store.
  virtual std::vector<InlineResult> GetResults(std::vector<ObjectID> ids) = 0;

  /////////// Worker interface ///////////

  /// Return a list of tasks this actor should execute. This call blocks until tasks to
  /// execute
  /// are available.
  ///
  /// \return List of task specs to execute.
  virtual std::vector<std::unique_ptr<TaskSpecification>> GetTasksToExecute() = 0;

  /// Enqueue a batch of results to be returned to the caller.
  ///
  /// \param List of inline results.
  virtual void SendResults(std::vector<InlineResult> results) = 0;

  virtual ~ActorTransport() = 0;
};

/// \class DummyTransport
///
/// An actor transport that discards all task executions. This can be useful for measuring
/// client serialization performance in isolation.
class DummyTransport : ActorTransport {
 public:
  DummyTransport() {}

  ~DummyTransport() {}

  void SubmitTask(std::unique_ptr<TaskSpecification> task_spec) override {}

  std::vector<InlineResult> GetResults(std::vector<ObjectID> ids) override {
    std::vector<InlineResult> results;
    for (int i = 0; i < ids.size(); i++) {
      results.push_back(InlineResult(-1, nullptr));
    }
    return results;
  }

  void SendResults(std::vector<InlineResult> results) override {}
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_ACTOR_TRANSPORT_H
