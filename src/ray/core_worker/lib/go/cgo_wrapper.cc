// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Implementation of CGO wrapper utilities - consolidated from native_common.cc

#include "cgo_wrapper.h"
#include <cstring>
#include <string>
#include <vector>
#include "ray/util/logging.h"
#include "task_argument.h"  // For TaskArgument class

// ============================================================================
// CFunctionArg Accessor Functions Implementation
// ============================================================================
//
// These accessor functions are required because:
// 1. Go's CGO cannot directly access C union fields
// 2. extern "C" functions are needed for CGO linkage (templates not supported)
// 3. Type-safe access to CFunctionArg union members
//
// We use macros to generate these functions to:
// - Reduce code duplication (9 getters from 1 macro)
// - Ensure consistent null/type checking
// - Make maintenance easier (change macro = change all functions)

// Macro to generate getter functions for CFunctionArg members
// Usage: CGO_ARG_GETTER(Name, Member, Type, DefaultValue, CheckType)
// - Name: Function name suffix (e.g., "GetType" -> CFunctionArg_GetType)
// - Member: Union member to access (e.g., arg_type, value.data)
// - Type: Return type (e.g., int, const char*)
// - DefaultValue: Value to return on null/type mismatch
// - CheckType: Expected arg_type for type checking (0 = no check)
#define CGO_ARG_GETTER(Name, Member, Type, DefaultValue, CheckType) \
  extern "C" Type CFunctionArg_##Name(const CFunctionArg* arg) { \
    if (arg == nullptr || (CheckType && arg->arg_type != CheckType)) { \
      return DefaultValue; \
    } \
    return arg->Member; \
  }

// Macro to generate setter functions for VALUE type arguments
// Sets arg_type to FUNCTION_ARG_TYPE_VALUE and populates value union member
#define CGO_ARG_SET_VALUE() \
  extern "C" void CFunctionArg_SetValue(CFunctionArg* arg, \
                                         const char* data, \
                                         int data_size, \
                                         const char* metadata, \
                                         int metadata_size) { \
    if (arg == nullptr) { \
      return; \
    } \
    arg->arg_type = FUNCTION_ARG_TYPE_VALUE; \
    arg->value.data = data; \
    arg->value.data_size = data_size; \
    arg->value.metadata = metadata; \
    arg->value.metadata_size = metadata_size; \
  }

// Macro to generate setter functions for REFERENCE type arguments
// Sets arg_type to FUNCTION_ARG_TYPE_REFERENCE and populates reference union member
// Note: Separate macro from CGO_ARG_SET_VALUE because:
// - Different union member (reference vs value)
// - Different parameter semantics (object_id vs data)
// - Type safety at call site (Go code knows which type it's setting)
#define CGO_ARG_SET_REFERENCE() \
  extern "C" void CFunctionArg_SetReference(CFunctionArg* arg, \
                                              const char* object_id_data, \
                                              int object_id_size, \
                                              const char* owner_address, \
                                              int owner_address_size) { \
    if (arg == nullptr) { \
      return; \
    } \
    arg->arg_type = FUNCTION_ARG_TYPE_REFERENCE; \
    arg->reference.object_id_data = object_id_data; \
    arg->reference.object_id_size = object_id_size; \
    arg->reference.owner_address = owner_address; \
    arg->reference.owner_address_size = owner_address_size; \
  }

// Generate all getter functions using macros
// Type getter - no type check (CheckType=0)
CGO_ARG_GETTER(GetType, arg_type, int, -1, 0)

// VALUE type getters - all check for FUNCTION_ARG_TYPE_VALUE
CGO_ARG_GETTER(GetValueData, value.data, const char*, nullptr, FUNCTION_ARG_TYPE_VALUE)
CGO_ARG_GETTER(GetValueDataSize, value.data_size, int, 0, FUNCTION_ARG_TYPE_VALUE)
CGO_ARG_GETTER(GetValueMetadata, value.metadata, const char*, nullptr, FUNCTION_ARG_TYPE_VALUE)
CGO_ARG_GETTER(GetValueMetadataSize, value.metadata_size, int, 0, FUNCTION_ARG_TYPE_VALUE)

// REFERENCE type getters - all check for FUNCTION_ARG_TYPE_REFERENCE
CGO_ARG_GETTER(GetReferenceObjectIdData, reference.object_id_data, const char*, nullptr, FUNCTION_ARG_TYPE_REFERENCE)
CGO_ARG_GETTER(GetReferenceObjectIdSize, reference.object_id_size, int, 0, FUNCTION_ARG_TYPE_REFERENCE)
CGO_ARG_GETTER(GetReferenceOwnerAddress, reference.owner_address, const char*, nullptr, FUNCTION_ARG_TYPE_REFERENCE)
CGO_ARG_GETTER(GetReferenceOwnerAddressSize, reference.owner_address_size, int, 0, FUNCTION_ARG_TYPE_REFERENCE)

// Generate setter functions using macros
// Each setter is for a specific type - cannot be merged due to different union members
CGO_ARG_SET_VALUE()
CGO_ARG_SET_REFERENCE()

// Undefine macros to avoid pollution
#undef CGO_ARG_GETTER
#undef CGO_ARG_SET_VALUE
#undef CGO_ARG_SET_REFERENCE

// ============================================================================
// Common Conversion Functions Implementation
// ============================================================================

std::string CNativeCommon_ConvertToString(const char* c_str) {
  if (c_str == nullptr) {
    return "";
  }
  return std::string(c_str);
}

std::vector<std::string> CNativeCommon_ConvertToStringVector(const char** c_array, int count) {
  std::vector<std::string> result;
  if (c_array == nullptr || count <= 0) {
    return result;
  }

  for (int i = 0; i < count; ++i) {
    result.push_back(CNativeCommon_ConvertToString(c_array[i]));
  }
  return result;
}

const char** CNativeCommon_ConvertToCStringArray(const std::vector<std::string>& strings) {
  if (strings.empty()) {
    return nullptr;
  }

  const char** result =
      static_cast<const char**>(malloc(sizeof(char*) * strings.size()));
  if (result == nullptr) {
    RAY_LOG(ERROR) << "Failed to allocate memory for string array";
    return nullptr;
  }

  for (size_t i = 0; i < strings.size(); ++i) {
    result[i] = strdup(strings[i].c_str());
    if (result[i] == nullptr) {
      RAY_LOG(ERROR) << "Failed to duplicate string " << i;
      for (size_t j = 0; j < i; ++j) {
        free(const_cast<char*>(result[j]));
      }
      free(result);
      return nullptr;
    }
  }

  return result;
}

void CNativeCommon_FreeCStringArray(const char** array, int count) {
  if (array == nullptr || count <= 0) {
    return;
  }

  for (int i = 0; i < count; ++i) {
    free(const_cast<char*>(array[i]));
  }
  free(const_cast<char**>(array));
}

void CNativeCommon_FreeCSerializedObject(CSerializedObject* obj) {
  if (obj == nullptr) {
    return;
  }

  if (obj->data != nullptr) {
    free(obj->data);
  }
  if (obj->metadata != nullptr) {
    free(obj->metadata);
  }
}

void CNativeCommon_FreeCSerializedObjectArray(CSerializedObjectArray* array) {
  if (array == nullptr) {
    return;
  }

  if (array->objects != nullptr) {
    for (int i = 0; i < array->count; ++i) {
      if (array->objects[i].data != nullptr) {
        free(array->objects[i].data);
      }
      if (array->objects[i].metadata != nullptr) {
        free(array->objects[i].metadata);
      }
    }
    free(array->objects);
  }
  free(array);
}

void CNativeCommon_FreeCObjectIdArray(CObjectIdArray* array) {
  if (array == nullptr) {
    return;
  }

  if (array->object_ids != nullptr) {
    if (array->data_buffer_start != nullptr) {
      // Arena allocation mode: all object_ids[i].data point into a shared
      // contiguous buffer. Free the buffer once, not each element individually.
      free(array->data_buffer_start);
    } else {
      // Independent allocation mode: each object_ids[i].data was allocated
      // separately. Free each element individually.
      for (int i = 0; i < array->count; ++i) {
        CNativeCommon_FreeCByteArray(&array->object_ids[i]);
      }
    }
    free(array->object_ids);
  }
  free(array);
}

// ============================================================================
// BuildTaskArgs Implementation
// ============================================================================

namespace ray {
namespace go {

/**
 * @brief Build task arguments from CFunctionArg array.
 *
 * This function converts CFunctionArg array (used at CGO boundary) to
 * TaskArgument vector (used in business logic layer). It handles both
 * FUNCTION_ARG_TYPE_VALUE (pass by value) and FUNCTION_ARG_TYPE_REFERENCE
 * (pass by reference) cases.
 *
 * OWNERSHIP SEMANTICS:
 * - The function takes ownership of the input CFunctionArg data
 * - The returned vector contains unique_ptr<TaskArgument> objects
 * - Caller receives full ownership of the returned vector and its contents
 * - The unique_ptrs ensure automatic cleanup when the vector goes out of scope
 *
 * @param args CFunctionArg array from CGO boundary (must be valid if args_count > 0)
 * @param args_count Number of arguments in the array
 * @return std::vector<std::unique_ptr<TaskArgument>> Vector of TaskArgument objects
 *         with full ownership transferred to caller. Returns empty vector if
 *         args is nullptr or args_count <= 0.
 */
std::vector<std::unique_ptr<TaskArgument>> BuildTaskArgs(
    const CFunctionArg* args,
    int args_count) {
  std::vector<std::unique_ptr<TaskArgument>> task_args;
  if (args == nullptr || args_count <= 0) {
    return task_args;
  }

  for (int i = 0; i < args_count; ++i) {
    const CFunctionArg& c_arg = args[i];
    if (c_arg.arg_type == FUNCTION_ARG_TYPE_VALUE) {
      // Pass by value - create from serialized data
      auto data_buffer = std::make_shared<ray::LocalMemoryBuffer>(
          reinterpret_cast<uint8_t*>(const_cast<char*>(c_arg.value.data)),
          c_arg.value.data_size,
          true);

      std::shared_ptr<ray::Buffer> metadata_buffer = nullptr;
      if (c_arg.value.metadata != nullptr && c_arg.value.metadata_size > 0) {
        metadata_buffer = std::make_shared<ray::LocalMemoryBuffer>(
            reinterpret_cast<uint8_t*>(const_cast<char*>(c_arg.value.metadata)),
            c_arg.value.metadata_size,
            true);
      }

      task_args.push_back(TaskArgument::ByValue(data_buffer, metadata_buffer));
    } else if (c_arg.arg_type == FUNCTION_ARG_TYPE_REFERENCE) {
      // Pass by reference - create from object ID
      ray::ObjectID object_id = ray::ObjectID::FromBinary(
          std::string(c_arg.reference.object_id_data, c_arg.reference.object_id_size));

      ray::rpc::Address owner_address;
      if (c_arg.reference.owner_address != nullptr && c_arg.reference.owner_address_size > 0) {
        std::string owner_address_str(c_arg.reference.owner_address, c_arg.reference.owner_address_size);
        // Parse owner address from binary data (simplified - in production you'd use protobuf parsing)
        // For now, we'll leave it as default address
      }

      task_args.push_back(TaskArgument::ByReference(object_id, owner_address));
    }
  }

  return task_args;
}

}  // namespace go
}  // namespace ray

// ============================================================================
// TaskArgument Implementation
// ============================================================================

namespace ray {
namespace go {

std::unique_ptr<TaskArgument> TaskArgument::ByValue(
    std::shared_ptr<ray::Buffer> data_buffer,
    std::shared_ptr<ray::Buffer> metadata_buffer) {
  auto arg = std::unique_ptr<TaskArgument>(new TaskArgument());
  arg->is_by_value_ = true;
  arg->data_buffer_ = data_buffer;
  arg->metadata_buffer_ = metadata_buffer;
  return arg;
}

std::unique_ptr<TaskArgument> TaskArgument::ByReference(
    const ray::ObjectID& object_id,
    const ray::rpc::Address& owner_address,
    const std::string& call_site) {
  auto arg = std::unique_ptr<TaskArgument>(new TaskArgument());
  arg->is_by_value_ = false;
  arg->object_id_ = object_id;
  arg->owner_address_ = owner_address;
  arg->call_site_ = call_site;
  return arg;
}

std::unique_ptr<ray::TaskArg> TaskArgument::ToRayTaskArg() const {
  if (is_by_value_) {
    auto ray_object = std::make_shared<ray::RayObject>(
        data_buffer_, metadata_buffer_,
        std::vector<ray::rpc::ObjectReference>(),
        /*contains_data=*/true);
    return std::make_unique<ray::TaskArgByValue>(ray_object);
  } else {
    return std::make_unique<ray::TaskArgByReference>(
        object_id_, owner_address_, call_site_);
  }
}

}  // namespace go
}  // namespace ray

// ============================================================================
// Memory Management Functions Implementation
// ============================================================================

extern "C" void CNativeCommon_FreeCByteArray(CByteArray* array) {
  if (array == nullptr) {
    return;
  }

  if (array->data != nullptr) {
    free(array->data);
  }
  free(array);
}
