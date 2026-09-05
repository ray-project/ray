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

#ifndef RAY_CORE_WORKER_LIB_GO_CGO_WRAPPER_H
#define RAY_CORE_WORKER_LIB_GO_CGO_WRAPPER_H

#ifdef __cplusplus
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "ray/util/logging.h"
#else
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// C Type Definitions - CGO Boundary Layer
// ============================================================================

/**
 * @brief CByteArray represents a byte array for ID serialization.
 * Caller is responsible for freeing both data pointer and the struct itself.
 */
typedef struct CByteArray {
  char *data;  // Binary data
  int size;    // Size of data in bytes
} CByteArray;

/**
 * @brief CSerializedObject represents a serialized object (data + metadata).
 * Caller is responsible for freeing data and metadata pointers.
 */
typedef struct CSerializedObject {
  char *data;         // Serialized data
  int data_size;      // Size of data in bytes
  char *metadata;     // Object metadata
  int metadata_size;  // Size of metadata in bytes
} CSerializedObject;

/**
 * @brief CSerializedObjectArray represents an array of serialized objects.
 * Caller is responsible for freeing the entire structure and its contents.
 */
typedef struct CSerializedObjectArray {
  CSerializedObject *objects;  // Array of serialized objects
  int count;                   // Number of objects
} CSerializedObjectArray;

/**
 * @brief CObjectIdArray represents an array of object IDs.
 * Caller is responsible for freeing the entire structure and its contents.
 *
 * Memory ownership:
 * - If data_buffer_start is non-null, all object_ids[i].data point into a shared
 *   contiguous buffer (arena allocation). Free data_buffer_start once, not each element.
 * - If data_buffer_start is null, each object_ids[i].data is independently allocated.
 *   Free each object_ids[i].data individually.
 */
typedef struct CObjectIdArray {
  CByteArray *object_ids;  // Array of object IDs (each is a CByteArray)
  int count;               // Number of object IDs
  char *
      data_buffer_start;  // Start of shared data buffer (null if independently allocated)
} CObjectIdArray;

// ============================================================================
// Task Argument Types - CGO Boundary Layer
// ============================================================================
// These types are used for passing task arguments across the CGO boundary.
// They are used by both TaskSubmitter and TaskExecutor modules.
// ============================================================================

/**
 * @brief CFunctionArgType represents the type of function argument.
 */
typedef enum {
  FUNCTION_ARG_TYPE_VALUE = 0,      // Pass by value (serialized object)
  FUNCTION_ARG_TYPE_REFERENCE = 1,  // Pass by reference (object ID)
} CFunctionArgType;

/**
 * @brief CFunctionArgValue represents arguments passed by value (serialized object)
 */
typedef struct {
  const char *data;  // Serialized object data
  int data_size;
  const char *metadata;  // Object metadata
  int metadata_size;
} CFunctionArgValue;

/**
 * @brief CFunctionArgReference represents arguments passed by reference (object ID)
 */
typedef struct {
  const char *object_id_data;  // Object ID binary data
  int object_id_size;
  const char *owner_address;  // Serialized owner address
  int owner_address_size;
} CFunctionArgReference;

/**
 * @brief CFunctionArg represents a function argument for task submission/execution.
 * Uses union to save memory and clarify which fields are used for each type.
 */
typedef struct {
  int arg_type;  // CFunctionArgType - must be first for type safety
  union {
    CFunctionArgValue value;          // Used when arg_type == FUNCTION_ARG_TYPE_VALUE
    CFunctionArgReference reference;  // Used when arg_type == FUNCTION_ARG_TYPE_REFERENCE
  };
} CFunctionArg;

// ============================================================================
// CFunctionArg Accessor Functions
// ============================================================================
// These functions provide a clean interface for Go code to access CFunctionArg.
// Go's CGO doesn't handle unions well, so we use accessor functions instead
// of exposing union fields directly to Go code.
// ============================================================================

// Get the argument type (FUNCTION_ARG_TYPE_VALUE or FUNCTION_ARG_TYPE_REFERENCE)
int CFunctionArg_GetType(const CFunctionArg *arg);

// Set value argument (for FUNCTION_ARG_TYPE_VALUE)
void CFunctionArg_SetValue(CFunctionArg *arg,
                           const char *data,
                           int data_size,
                           const char *metadata,
                           int metadata_size);

// Get value argument data
const char *CFunctionArg_GetValueData(const CFunctionArg *arg);

// Get value argument data size
int CFunctionArg_GetValueDataSize(const CFunctionArg *arg);

// Get value argument metadata
const char *CFunctionArg_GetValueMetadata(const CFunctionArg *arg);

// Get value argument metadata size
int CFunctionArg_GetValueMetadataSize(const CFunctionArg *arg);

// Set reference argument (for FUNCTION_ARG_TYPE_REFERENCE)
void CFunctionArg_SetReference(CFunctionArg *arg,
                               const char *object_id_data,
                               int object_id_size,
                               const char *owner_address,
                               int owner_address_size);

// Get reference argument object ID data
const char *CFunctionArg_GetReferenceObjectIdData(const CFunctionArg *arg);

// Get reference argument object ID data size
int CFunctionArg_GetReferenceObjectIdSize(const CFunctionArg *arg);

// Get reference argument owner address
const char *CFunctionArg_GetReferenceOwnerAddress(const CFunctionArg *arg);

// Get reference argument owner address size
int CFunctionArg_GetReferenceOwnerAddressSize(const CFunctionArg *arg);

// ============================================================================
// Memory Management Functions (C-linkage for CGO)
// ============================================================================

/**
 * @brief Frees the memory allocated for a CByteArray.
 * @param array Pointer to CByteArray to free (may be NULL)
 */
void CNativeCommon_FreeCByteArray(CByteArray *array);

/**
 * @brief Frees the memory allocated for a CSerializedObjectArray.
 * @param array Pointer to CSerializedObjectArray to free (may be NULL)
 */
void CNativeCommon_FreeCSerializedObjectArray(CSerializedObjectArray *array);

/**
 * @brief Frees the memory allocated for a CObjectIdArray.
 * @param array Pointer to CObjectIdArray to free (may be NULL)
 */
void CNativeCommon_FreeCObjectIdArray(CObjectIdArray *array);

// Note: Free functions for CObjectReference, CObjectArray, CWaitResult are defined
// in native_object_store.cc because those types are defined in native_object_store.h

// ============================================================================
// Go Exported Functions - CGO Boundary Layer
// ============================================================================
// These functions are implemented in Go and exported via CGO.

/**
 * @brief Trigger garbage collection in Go runtime.
 * This is a CGO-exported function that calls runtime.GC() in Go.
 * Used by C++ code to request GC when memory pressure is detected.
 */
#ifdef __cplusplus
extern "C" void GoTriggerGC();
#else
void GoTriggerGC();
#endif

#ifdef __cplusplus
}  // extern "C"

// ============================================================================
// String Conversion Utilities (C++ only, internal use)
// ============================================================================

/**
 * @brief Convert C string to std::string
 * @param c_str The C string to convert
 * @return std::string The converted string
 */
std::string CNativeCommon_ConvertToString(const char *c_str);

/**
 * @brief Helper function to convert C string array to std::vector<std::string>
 * @param c_array The C string array
 * @param count The number of elements
 * @return std::vector<std::string> The converted string vector
 */
std::vector<std::string> CNativeCommon_ConvertToStringVector(const char **c_array,
                                                             int count);

/**
 * @brief Helper function to convert std::vector<std::string> to C string array
 * Caller is responsible for freeing the returned array and its strings
 * @param strings The string vector to convert
 * @return const char** The C string array (caller must free)
 */
const char **CNativeCommon_ConvertToCStringArray(const std::vector<std::string> &strings);

/**
 * @brief Helper function to free C string array
 * @param array The C string array to free
 * @param count The number of elements
 */
void CNativeCommon_FreeCStringArray(const char **array, int count);

#endif  // __cplusplus

#ifdef __cplusplus
// Forward declaration for BuildTaskArgs function
namespace ray {
namespace go {
class TaskArgument;  // Forward declaration to avoid including task_argument.h

/// @brief Build task arguments from CFunctionArg array
/// @param args CFunctionArg array from CGO boundary
/// @param args_count Number of arguments
/// @return Vector of TaskArgument unique pointers
///
/// This function converts CFunctionArg array (used at CGO boundary) to
/// TaskArgument vector (used in business logic layer). It handles both
/// FUNCTION_ARG_TYPE_VALUE (pass by value) and FUNCTION_ARG_TYPE_REFERENCE
/// (pass by reference) cases.
std::vector<std::unique_ptr<TaskArgument>> BuildTaskArgs(const CFunctionArg *args,
                                                         int args_count);
}  // namespace go
}  // namespace ray

namespace ray {
namespace go {

// ============================================================================
// RAII Memory Manager - Automatic C structure deallocation
// ============================================================================

/**
 * @brief Deleter functor for RAII management of C structures.
 * @tparam T The C structure type.
 * @tparam FreeFunc Pointer to the C function that frees the structure.
 */
template <typename T, void (*FreeFunc)(T *)>
struct CgoDeleter {
  void operator()(T *ptr) const {
    if (ptr != nullptr) {
      FreeFunc(ptr);
    }
  }
};

/**
 * @brief Unique pointer for C structures with automatic memory management.
 * @tparam T The C structure type.
 * @tparam FreeFunc Pointer to the C function that frees the structure.
 */
template <typename T, void (*FreeFunc)(T *)>
using CgoUniquePtr = std::unique_ptr<T, CgoDeleter<T, FreeFunc>>;

/**
 * @brief Unique pointer for CByteArray with automatic memory management.
 */
using CByteArrayPtr = CgoUniquePtr<CByteArray, &CNativeCommon_FreeCByteArray>;

/**
 * @brief Unique pointer for CObjectIdArray with automatic memory management.
 */
using CObjectIdArrayPtr = CgoUniquePtr<CObjectIdArray, &CNativeCommon_FreeCObjectIdArray>;

/**
 * @brief Unique pointer for CSerializedObjectArray with automatic memory management.
 */
using CSerializedObjectArrayPtr =
    CgoUniquePtr<CSerializedObjectArray, &CNativeCommon_FreeCSerializedObjectArray>;

// ============================================================================
// Error Handler - Unified exception to C error conversion
// ============================================================================

/**
 * @brief Provides unified exception handling for CGO wrapper functions.
 *
 * This class captures exceptions thrown by C++ code and converts them to
 * appropriate C-style error returns (typically nullptr for pointer returns).
 * All exceptions are logged to stderr before being caught.
 */
class CgoErrorHandler {
 public:
  /**
   * @brief Execute a function that returns void and catch exceptions.
   *
   * This overload handles functions that return void.
   *
   * @tparam Func The function type.
   * @param func_name Name of the function for logging purposes.
   * @param func The function to execute.
   */
  template <typename Func>
  static void ExecuteVoid(const char *func_name, Func &&func) {
    try {
      std::forward<Func>(func)();
    } catch (const std::exception &e) {
      RAY_LOG(ERROR) << "CGO Error: " << func_name << " failed: " << e.what();
    } catch (...) {
      RAY_LOG(ERROR) << "CGO Error: " << func_name << " failed with unknown exception";
    }
  }

  /**
   * @brief Execute a function and catch exceptions, returning nullptr on failure.
   *
   * @tparam Func The function type.
   * @param func_name Name of the function for logging purposes.
   * @param func The function to execute.
   * @return decltype(func()) The result of the function, or nullptr if an exception
   * occurred.
   */
  template <typename Func>
  static auto Execute(const char *func_name, Func &&func) -> decltype(func()) {
    using ResultType = decltype(func());
    try {
      return std::forward<Func>(func)();
    } catch (const std::exception &e) {
      RAY_LOG(ERROR) << "CGO Error: " << func_name << " failed: " << e.what();
      return ResultType{};
    } catch (...) {
      RAY_LOG(ERROR) << "CGO Error: " << func_name << " failed with unknown exception";
      return ResultType{};
    }
  }

  /**
   * @brief Execute a function that returns an integer and catch exceptions.
   *
   * For integer return types, we return -1 on error (a common convention).
   *
   * @tparam Func The function type.
   * @param func_name Name of the function for logging purposes.
   * @param func The function to execute.
   * @return int The result of the function, or -1 if an exception occurred.
   */
  template <typename Func>
  static int ExecuteInt(const char *func_name, Func &&func) {
    try {
      return std::forward<Func>(func)();
    } catch (const std::exception &e) {
      RAY_LOG(ERROR) << "CGO Error: " << func_name << " failed: " << e.what();
      return -1;
    } catch (...) {
      RAY_LOG(ERROR) << "CGO Error: " << func_name << " failed with unknown exception";
      return -1;
    }
  }
};

// ============================================================================
// Type Converter - Bidirectional C <-> C++ conversion
// ============================================================================

/**
 * @brief Provides bidirectional conversion between C and C++ types.
 *
 * This class handles all type conversions needed at the CGO boundary,
 * including strings, byte arrays, and complex structures.
 */
class CgoTypeConverter {
 public:
  /**
   * @brief Convert C string to std::string.
   *
   * @param c_str The C string to convert (may be nullptr).
   * @return std::string The converted string (empty if c_str is nullptr).
   */
  static std::string ToStdString(const char *c_str) {
    if (c_str == nullptr) {
      return "";
    }
    return std::string(c_str);
  }

  /**
   * @brief Convert C string array to std::vector<std::string>.
   *
   * @param c_array The C string array to convert.
   * @param count The number of elements in the array.
   * @return std::vector<std::string> The converted string vector.
   */
  static std::vector<std::string> ToStringVector(const char **c_array, int count) {
    std::vector<std::string> result;
    if (c_array == nullptr || count <= 0) {
      return result;
    }
    result.reserve(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
      result.push_back(ToStdString(c_array[i]));
    }
    return result;
  }

  /**
   * @brief Convert std::string to C string (caller must free).
   *
   * @param str The std::string to convert.
   * @return char* The C string (caller must free with free()), or nullptr if allocation
   * fails.
   */
  static char *ToCString(const std::string &str) { return strdup(str.c_str()); }

  /**
   * @brief Convert CByteArray to std::vector<uint8_t>.
   *
   * @param c_bytes The CByteArray to convert (may be nullptr).
   * @return std::vector<uint8_t> The converted byte vector (empty if c_bytes is nullptr).
   */
  static std::vector<uint8_t> ToByteVector(const CByteArray *c_bytes) {
    if (c_bytes == nullptr || c_bytes->data == nullptr) {
      return {};
    }
    const uint8_t *data_ptr = reinterpret_cast<const uint8_t *>(c_bytes->data);
    return std::vector<uint8_t>(data_ptr, data_ptr + c_bytes->size);
  }

  /**
   * @brief Convert raw byte array to CByteArray (caller must free).
   *
   * @param data Pointer to the byte data.
   * @param size Size of the byte data.
   * @return CByteArray* The CByteArray (caller must free with
   * CNativeCommon_FreeCByteArray()), or nullptr if allocation fails or data is null/size
   * <= 0.
   */
  static CByteArray *ToCByteArray(const uint8_t *data, int size) {
    if (data == nullptr || size <= 0) {
      return nullptr;
    }

    auto *result = static_cast<CByteArray *>(malloc(sizeof(CByteArray)));
    if (result == nullptr) {
      RAY_LOG(ERROR) << "Failed to allocate CByteArray structure";
      return nullptr;
    }

    result->size = size;
    result->data = static_cast<char *>(malloc(static_cast<size_t>(size)));
    if (result->data == nullptr) {
      free(result);
      RAY_LOG(ERROR) << "Failed to allocate data buffer of size " << size;
      return nullptr;
    }

    memcpy(result->data, data, static_cast<size_t>(size));
    return result;
  }

  /**
   * @brief Convert std::vector<uint8_t> to CByteArray (caller must free).
   * must free).
   *
   * @param bytes The byte vector to convert.
   * @return CByteArray* The CByteArray (caller must free), or nullptr if allocation fails
   * or bytes is empty.
   */
  static CByteArray *ToCByteArray(const std::vector<uint8_t> &bytes) {
    if (bytes.empty()) {
      return nullptr;
    }
    return ToCByteArray(bytes.data(), static_cast<int>(bytes.size()));
  }

  /**
   * @brief Convert std::string to CByteArray (caller must free).
   *
   * @param str The string to convert (treated as byte sequence).
   * @return CByteArray* The CByteArray (caller must free), or nullptr if allocation fails
   * or str is empty.
   */
  static CByteArray *StringToCByteArray(const std::string &str) {
    if (str.empty()) {
      return nullptr;
    }
    return ToCByteArray(reinterpret_cast<const uint8_t *>(str.data()),
                        static_cast<int>(str.size()));
  }

  /**
   * @brief Convert Ray ID type to CByteArray (generic template).
   *
   * @tparam IdType The Ray ID type (must have Binary() method).
   * @param id The ID to convert.
   * @return CByteArray* The CByteArray (caller must free), or nullptr if allocation
   * fails.
   */
  template <typename IdType>
  static CByteArray *IdToCByteArray(const IdType &id) {
    const auto &binary = id.Binary();
    return ToCByteArray(reinterpret_cast<const uint8_t *>(binary.data()),
                        static_cast<int>(binary.size()));
  }

  /**
   * @brief Convert std::vector<std::string> to C string array (caller must free).
   *
   * @param strings The string vector to convert.
   * @return const char** The C string array (caller must free with
   * CNativeCommon_FreeCStringArray()), or nullptr if allocation fails or strings is
   * empty.
   */
  static const char **ToCStringArray(const std::vector<std::string> &strings) {
    if (strings.empty()) {
      return nullptr;
    }

    const char **result =
        static_cast<const char **>(malloc(sizeof(char *) * strings.size()));
    if (result == nullptr) {
      RAY_LOG(ERROR) << "Failed to allocate string array";
      return nullptr;
    }

    for (size_t i = 0; i < strings.size(); ++i) {
      result[i] = strdup(strings[i].c_str());
      if (result[i] == nullptr) {
        RAY_LOG(ERROR) << "Failed to duplicate string at index " << i;
        // Free already allocated strings
        for (size_t j = 0; j < i; ++j) {
          free(const_cast<char *>(result[j]));
        }
        free(result);
        return nullptr;
      }
    }

    return result;
  }
};

}  // namespace go
}  // namespace ray

#endif  // __cplusplus

#endif  // RAY_CORE_WORKER_LIB_GO_CGO_WRAPPER_H
