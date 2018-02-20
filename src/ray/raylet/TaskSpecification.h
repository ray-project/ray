#ifndef TASK_SPECIFICATION_H
#define TASK_SPECIFICATION_H

#include <cstddef>
#include <vector>

namespace ray {
class TaskSpecification {
public:
  /// Task specification constructor
  TaskSpecification(const char *spec, size_t spec_size) {
    spec_.assign(spec, spec + spec_size);
  }
  ~TaskSpecification() {}
  const char *Data();
  size_t Size();
private:
  std::vector<char> spec_;
};
}

#endif
