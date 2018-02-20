#ifndef TASK_SPECIFICATION_H
#define TASK_SPECIFICATION_H

#include <cstddef>
#include <string>
#include <vector>
#include <unordered_map>

#include "ray/id.h"
#include "format/common_generated.h"

extern "C" {
#include "sha256.h"
}

using namespace std;
namespace ray {

/// A task argument that can be serialized and hashed as part of the
/// TaskSpecification.
class TaskArgument {
 public:
  virtual flatbuffers::Offset<Arg> ToFlatbuffer(flatbuffers::FlatBufferBuilder &fbb) const = 0;
  virtual const BYTE *HashData() const = 0;
  virtual size_t HashDataLength() const = 0;
};

/// A TaskSpecification argument consisting of a list of object ID references.
class TaskArgumentByReference : virtual public TaskArgument {
  TaskArgumentByReference(const std::vector<ObjectID> &references);
  flatbuffers::Offset<Arg> ToFlatbuffer(flatbuffers::FlatBufferBuilder &fbb) const override;
  const BYTE *HashData() const override;
  size_t HashDataLength() const override;
  private:
    const std::vector<ObjectID> references_;
};

/// A TaskSpecification argument by raw value.
class TaskArgumentByValue : public TaskArgument {
  TaskArgumentByValue(const uint8_t *value, size_t length);
  flatbuffers::Offset<Arg> ToFlatbuffer(flatbuffers::FlatBufferBuilder &fbb) const override;
  const BYTE *HashData() const override;
  size_t HashDataLength() const override;
  private:
    std::vector<uint8_t> value_;
};

class TaskSpecification {
public:
  /// Create a copy from another TaskSpecification.
  TaskSpecification(const TaskSpecification &spec);
  /// Deserialize a TaskSpecification from a flatbuffer.
  TaskSpecification(const flatbuffers::String &string);
  /// A constructor from raw arguments.
  // TODO(swang): Define an actor task constructor.
  TaskSpecification(
      UniqueID driver_id,
      TaskID parent_task_id,
      int64_t parent_counter,
      //UniqueID actor_id,
      //UniqueID actor_handle_id,
      //int64_t actor_counter,
      FunctionID function_id,
      const vector<TaskArgument> &arguments,
      int64_t num_returns,
      const unordered_map<string, double> &required_resources);
  ~TaskSpecification() {}
  /// Serialize the TaskSpecification to a flatbuffer.
  flatbuffers::Offset<flatbuffers::String> ToFlatbuffer(flatbuffers::FlatBufferBuilder &fbb) const;

  TaskID TaskId() const;
  UniqueID DriverId() const;
  TaskID ParentTaskId() const;
  int64_t ParentCounter() const;
  FunctionID FunctionId() const;
  int64_t NumArgs() const;
  int64_t NumReturns() const;
  bool ArgByRef(int64_t arg_index) const;
  int ArgIdCount(int64_t arg_index) const;
  ObjectID ArgId(int64_t arg_index, int64_t id_index) const;
  const uint8_t *ArgVal(int64_t arg_index) const;
  size_t ArgValLength(int64_t arg_index) const;
  double GetRequiredResource(const std::string &resource_name) const;
  const std::unordered_map<std::string, double> GetRequiredResources() const;
private:
  /// Task specification constructor from a pointer.
  TaskSpecification(const uint8_t *spec, size_t spec_size);
  const uint8_t *data() const;
  size_t size() const;
  std::vector<uint8_t> spec_;
};
}

#endif
