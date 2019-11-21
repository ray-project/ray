#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "ray.h"

bool startsWith(const char *pre, const char *str);

ExecutionResult *on_execute(Ray *ray, TaskType tt, const char *func,
                            const ResourceMap *resources, const Objects *args,
                            const ObjectIds *arg_reference_ids,
                            const ObjectIds *return_ids, Objects *results) {
  printf("==> Told to execute: %s\n", func);

  printf("==> Allocating space for results\n");
  size_t sizes[] = {sizeof(int)};
  ExecutionResult *s = ray_allocate_returns(ray, return_ids, sizes, results);
  if (!is_ok(s)) {
    printf(" -> Allocation failed with error code: %d\n", err_code(s));
    return s;
  }

  if (objects_len(results) != 1) {
    printf(" -> Allocated space for %zu results, not 1\n", objects_len(results));
  }

  printf("==> Writing out result\n");
  int rval = 42;
  Object *ret = objects_at(results, 0);
  printf(" -> Size of object buf: %zu\n", object_buf_size(ret));
  memcpy(object_buf(ret), &rval, sizeof(int));
  printf(" -> Results written\n");

  return NULL;
}

int main(int argc, char *argv[]) {
  setbuf(stdout, NULL);

  if (argc > 1 && startsWith("--node-ip-address=", argv[1])) {
    // we're being run as a worker -- don't run the driver code
    printf(":: Starting a native worker for test driver\n");
    become_ray_worker(argc, argv, on_execute);
    return 0;
  }

  if (argc < 3) {
    printf("Usage: %s IP NODE_MANAGER_PORT REDIS_PORT\n", argv[0]);
    return 1;
  }

  printf(":: Starting test driver\n");

  char *ip = argv[1];
  int node_manager_port = atoi(argv[2]);
  int redis_port = atoi(argv[3]);

  printf("==> Connecting to ray\n");

  struct GcsConnectArgs gcs_args = {
      .redis_ip = ip, .redis_port = redis_port, .redis_password = ""};
  Ray *w = ray_new("/tmp/ray/session_latest/sockets/plasma_store",
                   "/tmp/ray/session_latest/sockets/raylet", 123456, gcs_args, ip,
                   node_manager_port);
  ExecutionResult *s;

  printf("==> Submitting task\n");

  ResourceMap *rm = resource_map_new();
  Args *args = args_new();
  ObjectIds *rets = object_ids_new();
  struct TaskOptions task = {
      .resources = rm,
  };
  s = ray_submit_task(w, NULL, "hello world", task, args, rets);
  if (!is_ok(s)) {
    printf(" -> submit_taks failed with code: %d\n", err_code(s));
    ray_free(w);
    return 1;
  }

  if (object_ids_len(rets) != 1) {
    printf(" -> Task has # return values != 1: %zu", object_ids_len(rets));
  }

  printf("==> Waiting for task\n");

  Ready *ready = ready_with_capacity(object_ids_len(rets));
  s = ray_wait(w, rets, object_ids_len(rets), -1, ready);
  if (!is_ok(s)) {
    printf(" -> ray_wait failed with code: %d\n", err_code(s));
    ray_free(w);
    return 1;
  }

  printf("==> Fetching task results\n");

  Objects *results = objects_with_capacity(ready_len(ready));
  s = ray_get(w, rets, -1, results);
  if (!is_ok(s)) {
    printf(" -> ray_get failed with code: %d\n", err_code(s));
    ray_free(w);
    return 1;
  }

  printf("==> Remote task finshed\n");

  Object *ret = objects_at(results, 0);
  int got = *(int *)(object_buf(ret));

  printf(" -> It returned: %d\n", got);

  ray_free(w);
}

bool startsWith(const char *pre, const char *str) {
  size_t lenpre = strlen(pre), lenstr = strlen(str);
  return lenstr < lenpre ? false : memcmp(pre, str, lenpre) == 0;
}
