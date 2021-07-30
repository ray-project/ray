#ifndef _Included_golang_worker
#define _Included_golang_worker
#ifdef __cplusplus
extern "C" {
#endif

void go_worker_Initialize(int workerMode, char *store_socket, char *raylet_socket,
                          char *log_dir, char *node_ip_address, int node_manager_port,
                          char *raylet_ip_address, char *driver_name, int jobId,
                          char *redis_address, int redis_port, char *redis_password);

void *go_worker_CreateGlobalStateAccessor(char *redis_address, char *redis_password);

bool go_worker_GlobalStateAccessorConnet(void *p);

int go_worker_GetNextJobID(void *p);

void SayHello(char *s);

#ifdef __cplusplus
}
#endif
#endif
