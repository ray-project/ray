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

// Why not char instead of void ptr?
DataValue *rust_worker_AllocateDataValue(void *data_ptr, size_t data_size, void *meta_ptr, size_t meta_size);

void rust_worker_Initialize(int workerMode, char *store_socket, char *raylet_socket,
                          char *log_dir, char *node_ip_address, int node_manager_port,
                          char *raylet_ip_address, char *driver_name, int jobId,
                          char *redis_address, int redis_port, char *redis_password,
                          char *serialized_job_config);

// void rust_worker_Run();
//
// void *rust_worker_CreateGlobalStateAccessor(char *redis_address, char *redis_password);
//
// bool rust_worker_GlobalStateAccessorConnet(void *p);
//
// char *rust_worker_GlobalStateAccessorGetInternalKV(void *p, char *key);
//
// int rust_worker_GetNodeToConnectForDriver(void *p, char *node_ip_address, char **result);
//
// int rust_worker_GetNextJobID(void *p);
//
// int rust_worker_CreateActor(char *type_name, char **result);
//
// // todo calloptions
// int rust_worker_SubmitActorTask(void *actor_id, char *method_name,
//                               DataValue **input_values, int num_input_value,
//                               int num_returns, void **object_ids);
//
// int rust_worker_Get(void **object_ids, int object_ids_size, int timeout, void **objects);
//
extern void rust_worker_execute(RayInt task_type, RaySlice ray_function_info, RaySlice args,
                              RaySlice return_values);
//
// void rust_worker_Shutdown();

#ifdef __cplusplus
}
#endif
#endif
