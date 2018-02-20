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
  /// Task specification constructor from a pointer.
  TaskSpecification(const uint8_t *spec, size_t spec_size);
  /// Create a copy from another TaskSpecification.
  TaskSpecification(const TaskSpecification &spec);
  /// A constructor from raw arguments.
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
  const uint8_t *Data() const;
  size_t Size() const;
private:
  std::vector<uint8_t> spec_;
};
}

#endif
