#ifndef _Included_rustlang_worker
#define _Included_rustlang_worker
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" { // Also declare: __declspec(dllexport)?
#endif
#ifndef RAY_NATIVE_PROLOGUE_H
#define RAY_NATIVE_PROLOGUE_H
typedef unsigned long long RayInt64;
typedef RayInt64 RayInt;
typedef struct RaySlice {
  // Shouldn't this be `char *data`?
  // Actually no, we want this to be some arbitrary type
  // (either a DataBuffer or a DataValue)
  void *data;
  RayInt64 len;
  RayInt64 cap;
} RaySlice;
#endif

// Rename BufferWrapper?
typedef struct DataBuffer {
  // TODO: replace with a less-headache-inducing uint64_t here...
  size_t size;
  const uint8_t *p;
} DataBuffer;

// Rename RayValueWrapper?
typedef struct DataValue {
  struct DataBuffer *data;
  struct DataBuffer *meta;
} DataValue;

// TODO: Write detailed description of methods


///
/// Public methods related to storing and retrieving objects.
///

/// Execute a NORMAL_TASK, ACTOR_TASK, or ACTOR_CREATION_TASK.
///
/// \param[in/out] actor_ptr ptr to an actor ptr. For ACTOR_TASK, the actor method
/// will accept its value as its first arg. For ACTOR_CREATION_TASK,
/// the ptr's value will be assigned to the newly created actor's pointer.
/// \param[in] task_type The IDs serialized in this object.
/// \param[out] return_values Generated ID of the object.
/// \return Status.

typedef void (*c_worker_ExecuteCallback)(bool is_async, void **actor_ptr, int task_type,
                                         RaySlice ray_function_info,
                                         const DataValue* const args[], size_t args_len,
                                         RaySlice return_values);
/// Register the language-worker's execution callback to the c_worker
///
/// \param[in]
int c_worker_RegisterExecutionCallback(const c_worker_ExecuteCallback callback);

typedef int (*c_worker_SetAsyncResultCallback)(void * future_object, DataValue *data_value);

/// Register the language-worker's `set_async_result` callback to the `c_worker`
///
/// \param[in]
int c_worker_RegisterSetAsyncResultCallback(const c_worker_SetAsyncResultCallback callback);

/**
* AllocateDataValue merely wraps
* It should be renamed
*/
const DataValue *c_worker_AllocateDataValue(const uint8_t *data_ptr, size_t data_size,
                                            const uint8_t *meta_ptr, size_t meta_size);

void c_worker_DeallocateDataValue(const DataValue *dv_ptr);

void c_worker_InitConfig(int workerMode, int language, int num_workers,
                         const char *code_search_path, const char *head_args,
                         int argc, char** argv);

void c_worker_Initialize();

void c_worker_Run();

void c_worker_Log(const char* msg);

void c_worker_AddLocalRef(const char* id);

void c_worker_RemoveLocalRef(const char* id);

// int c_worker_GetNextJobID(void *p);
//
// TODO: Why is result a list? Do we ever create more than one actor...?
int c_worker_CreateActor(const char *create_fn_name, const bool *input_is_ref,
                         const DataValue* const input_values[], const char **input_refs,
                         int num_input_value, char **result, bool is_async);

int c_worker_SubmitTask(int task_type, /*optional*/ const char *actor_id,
                        const char *method_name, const bool *input_is_ref,
                        const DataValue* const input_values[], const char **input_refs,
                        int num_input_value,
                        int num_returns, char **object_ids);

int c_worker_Get(const char* const object_ids[], int object_ids_size, int timeout, DataValue **objects);

void c_worker_GetAsync(const char *object_id, void *future_object);

int c_worker_Put(char **object_ids, int timeout, const DataValue **objects, int objects_size);

void c_worker_Shutdown();

void *c_worker_CreateFiberEvent();

void c_worker_NotifyReady(void *e);

void c_worker_YieldFiberAndAwait(void *e);

#ifdef __cplusplus
}
#endif
#endif
