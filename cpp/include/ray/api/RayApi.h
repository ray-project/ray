
#pragma once

#include <ray/api/Blob.h>
#include <cstdint>
#include <memory>
#include <typeinfo>
#include <vector>

#include "ray/api/UniqueId.h"
#include "ray/util/type-util.h"

namespace ray {

struct member_function_ptr_holder {
  uintptr_t value[2];
};

struct remote_function_ptr_holder {
  uintptr_t value[2];
};

class RayApi {
 public:
 
  virtual std::unique_ptr<UniqueId> put(std::vector< ::ray::blob> &&data) = 0;
  virtual del_unique_ptr< ::ray::blob> get(const UniqueId &id) = 0;

  virtual std::unique_ptr<UniqueId> call(remote_function_ptr_holder &fptr,
                                         std::vector< ::ray::blob> &&args) = 0;
  virtual std::unique_ptr<UniqueId> create(remote_function_ptr_holder &fptr,
                                           std::vector< ::ray::blob> &&args) = 0;
  virtual std::unique_ptr<UniqueId> call(const remote_function_ptr_holder &fptr,
                                         const UniqueId &actor,
                                         std::vector< ::ray::blob> &&args) = 0;
};
}