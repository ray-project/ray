#ifndef _Included_rustlang_worker
#define _Included_rustlang_worker
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif
#ifndef RAY_NATIVE_PROLOGUE_H
#define RAY_NATIVE_PROLOGUE_H
typedef unsigned long long RayInt64;
typedef RayInt64 RayInt;
typedef struct RaySlice {
  // Shouldn't this be `char *data`?
  void *data;
  RayInt64 len;
  RayInt64 cap;
} RaySlice;
#endif

typedef struct DataBuffer {
  // TODO: replace with standard uint64_t here...
  size_t size;
  void *p;
} DataBuffer;

typedef struct DataValue {
  struct DataBuffer *data;
  struct DataBuffer *meta;
} DataValue;

// TODO: Write detailed description of methods
typedef void (*c_worker_ExecuteCallback)(RayInt task_type, RaySlice ray_function_info,
                                         RaySlice args, RaySlice return_values);

int c_worker_RegisterExecutionCallback(c_worker_ExecuteCallback callback);

// Why not char instead of void ptr?
DataValue *c_worker_AllocateDataValue(void *data_ptr, size_t data_size, void *meta_ptr, size_t meta_size);

void c_worker_InitConfig(int workerMode, int language, int num_workers,
                                    char *code_search_path, char *head_args,
                                    int argc, char** argv);

void c_worker_Initialize();

void c_worker_Run();
//
// int c_worker_GetNextJobID(void *p);
//
// int c_worker_CreateActor(char *type_name, char **result);
//
// // todo calloptions
// int c_worker_SubmitActorTask(void *actor_id, char *method_name,
//                               DataValue **input_values, int num_input_value,
//                               int num_returns, void **object_ids);
//
int c_worker_SubmitTask(char *method_name, bool *input_is_ref,
                                   DataValue **input_values, char **input_refs,
                                   int num_input_value,
                                   int num_returns, char **object_ids);

int c_worker_Get(char **object_ids, int object_ids_size, int timeout, DataValue **objects);

int c_worker_Put(char **object_ids, int timeout, DataValue **objects, int objects_size);

void c_worker_Shutdown();

#ifdef __cplusplus
}
#endif
#endif
