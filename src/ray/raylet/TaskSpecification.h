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

class TaskArgument {
 public:
  virtual flatbuffers::Offset<Arg> ToFlatbuffer() const;
  virtual const BYTE *HashData() const;
  virtual size_t HashDataLength() const;
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
