#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>

//
// Opaque Ray types
//

// ray::CoreWorker
struct Ray;
typedef struct Ray Ray;

// std::shared_ptr<ray::RayObject>
struct Object;
typedef struct Object Object;

// std::vector<std::shared_ptr<ray::RayObject>>
struct Objects;
typedef struct Objects Objects;

// ray::ObjectID
struct ObjectId;
typedef struct ObjectId ObjectId;

// std::vector<ray::ObjectID>
struct ObjectIds;
typedef struct ObjectIds ObjectIds;

// ray::ActorID
struct ActorId;
typedef struct ActorId ActorId;

// ray::TaskArg
struct Arg;
typedef struct Arg Arg;

// std::vector<ray::TaskArg>
struct Args;
typedef struct Args Args;

// std::vector<bool>
struct Ready;
typedef struct Ready Ready;

// std::unordered_map<std::string, double>
struct ResourceMap;
typedef struct ResourceMap ResourceMap;

// ray::Status
struct ExecutionResult;
typedef struct ExecutionResult ExecutionResult;

//
// Public Ray types
//

struct GcsConnectArgs {
  char *redis_ip;
  int redis_port;
  char *redis_password;
};

struct TaskOptions {
  ResourceMap *resources;
};

typedef uint32_t TaskType;

typedef ExecutionResult *(*TaskExecutionCallback)(Ray *, TaskType, const char *,
                                                  const ResourceMap *, const Objects *,
                                                  const ObjectIds *, const ObjectIds *,
                                                  Objects *);

//
// Worker methods
//

void become_ray_worker(int argc, char *argv[], TaskExecutionCallback callback);

ExecutionResult *ray_allocate_returns(Ray *ray, const ObjectIds *ids,
                                      const size_t sizes[], Objects *return_objects);

Ray *ray_new(char *store_socket, char *raylet_socket, int job, struct GcsConnectArgs gcs,
             char *node_ip_address, int node_manager_port);

ExecutionResult *ray_put(Ray *ray, const char *bytes, size_t len, ObjectId *oid);
ExecutionResult *ray_get(Ray *ray, const ObjectIds *ids, int64_t timeout_ms,
                         Objects *results);
ExecutionResult *ray_wait(Ray *ray, const ObjectIds *ids, size_t num_objects,
                          int64_t timeout_ms, Ready *r);
ExecutionResult *ray_submit_task(Ray *ray, ActorId *actor, const char *function,
                                 struct TaskOptions opts, Args *args,
                                 ObjectIds *return_ids);

void ray_free(Ray *ray);

// create_actor

//
// Helper methods: ObjectIds
//
ObjectIds *object_ids_new();
ObjectIds *object_ids_with_capacity(size_t cap);
ObjectId *object_ids_at(ObjectIds *oids, size_t index);
size_t object_ids_len(ObjectIds *oids);
void object_ids_clear(ObjectIds *oids);
void object_ids_free(ObjectIds *oids);

//
// Helper methods: Objects
//
Objects *objects_new();
Objects *objects_with_capacity(size_t cap);
Object *objects_at(Objects *os, size_t index);
size_t objects_len(Objects *os);
void objects_clear(Objects *os);
void objects_free(Objects *os);

//
// Helper methods: Object
//
char *object_buf(Object *o);
size_t object_buf_size(Object *o);

//
// Helper methods: Ready
//
Ready *ready_new();
Ready *ready_with_capacity(size_t cap);
bool ready_at(Ready *r, size_t index);
size_t ready_len(Ready *r);
void ready_clear(Ready *r);
void ready_free(Ready *r);

//
// Helper methods: Args
//
Args *args_new();
Args *args_with_capacity(size_t cap);
Arg *args_at(Args *as, size_t index);
size_t args_len(Args *as);
void args_clear(Args *as);
void args_free(Args *as);

//
// Helper methods: ResourceMap
//
ResourceMap *resource_map_new();
double resources_for(ResourceMap *rm, char *index);
void resource_map_free(ResourceMap *rm);

//
// Helper methods: ExecutionResult
//
bool is_ok(ExecutionResult *e);
int err_code(ExecutionResult *e);
void result_free(ExecutionResult *e);

// vim: expandtab:sw=2:ts=2:sts=2
