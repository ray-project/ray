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
  void *data;
  RayInt64 len;
  RayInt64 cap;
} RaySlice;
#endif

typedef struct DataBuffer {
  size_t size;
  void *p;
} DataBuffer;

typedef struct DataValue {
  struct DataBuffer *data;
  struct DataBuffer *meta;
} DataValue;

typedef void (*execute_callback)(RayInt task_type, RaySlice ray_function_info, RaySlice args,
                              RaySlice return_values);

int c_worker_RegisterCallback(execute_callback callback);

// Why not char instead of void ptr?
DataValue *c_worker_AllocateDataValue(void *data_ptr, size_t data_size, void *meta_ptr, size_t meta_size);

void c_worker_InitConfig(int workerMode, int language, int num_workers,
                                    char *code_search_path, char *head_args,
                                    int argc, char** argv);

void c_worker_Initialize();

// void c_worker_Run();
//
// void *c_worker_CreateGlobalStateAccessor(char *redis_address, char *redis_password);
//
// bool c_worker_GlobalStateAccessorConnet(void *p);
//
// char *c_worker_GlobalStateAccessorGetInternalKV(void *p, char *key);
//
// int c_worker_GetNodeToConnectForDriver(void *p, char *node_ip_address, char **result);
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
int c_worker_Get(void **object_ids, int object_ids_size, int timeout, void **objects);

int c_worker_Put(char **object_ids, int timeout, DataValue **objects, int objects_size);

void c_worker_Shutdown();

#ifdef __cplusplus
}
#endif
#endif
