#ifndef _Included_golang_worker
#define _Included_golang_worker
#ifdef __cplusplus
extern "C" {
#endif

void goInitialize(
    int workerMode, char *store_socket, char *raylet_socket, char *log_dir,
    char *node_ip_address, int node_manager_port, char *raylet_ip_address, char* driver_name);

void SayHello(char* s);

#ifdef __cplusplus
}
#endif
#endif
