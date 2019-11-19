#include <chrono>
#include <cstring>
#include <string>
#include <thread>

extern "C" {
#include "ray.h"
}

#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"

extern "C" {
//
// Worker methods
//

void become_ray_worker(int argc, char *argv[], TaskExecutionCallback callback) {
  if (argc < 6) {
    // TODO: error
    return;
  }

  std::string store_socket = "";
  std::string raylet_socket = "";
  std::string node_ip_address = "";
  int node_manager_port = 0;
  std::string log_dir = "";
  std::string gcs_redis_ip = "";
  int gcs_redis_port = 0;
  std::string gcs_redis_password = "";

  // https://github.com/ray-project/ray/blob/33c768ebe414c2a2f93a8b8f8637e02b387bb7ea/python/ray/services.py#L1152
  for (int i = 1; i < argc; i++) {
    std::string arg(argv[i]);
    if (arg.rfind("--node-ip-address=", 0) == 0) {
      node_ip_address = arg.substr(arg.find("=") + 1);
    } else if (arg.rfind("--node-manager-port=", 0) == 0) {
      node_manager_port = stoi(arg.substr(arg.find("=") + 1));
    } else if (arg.rfind("--object-store-name=", 0) == 0) {
      store_socket = arg.substr(arg.find("=") + 1);
    } else if (arg.rfind("--raylet-name=", 0) == 0) {
      raylet_socket = arg.substr(arg.find("=") + 1);
    } else if (arg.rfind("--redis-address=", 0) == 0) {
      auto addr = arg.substr(arg.find("=") + 1);
      gcs_redis_ip = addr.substr(0, addr.find(":"));
      gcs_redis_port = stoi(addr.substr(addr.find(":") + 1));
    } else if (arg == "--redis-password") {
      gcs_redis_password = std::string(argv[i + 1]);
      i += 1;
    }
  }

  ray::gcs::GcsClientOptions gcs_opts(gcs_redis_ip, gcs_redis_port, gcs_redis_password);
  ray::CoreWorker *w = nullptr;
  w = new ray::CoreWorker(
      ray::WorkerType::WORKER, ray::Language::NATIVE, store_socket, raylet_socket,
      ray::JobID(), gcs_opts, log_dir, node_ip_address, node_manager_port,
      [&w, callback](
          TaskType task_type, const ray::RayFunction &ray_function,
          const std::unordered_map<std::string, double> &required_resources,
          const std::vector<std::shared_ptr<ray::RayObject>> &args,
          const std::vector<ray::ObjectID> &arg_reference_ids,
          const std::vector<ray::ObjectID> &return_ids,
          std::vector<std::shared_ptr<ray::RayObject>> *results) -> ray::Status {
        ray::Status *s =
            (ray::Status *)(callback((Ray *)(w), (uint32_t)task_type,
                                     ray_function.GetFunctionDescriptor()[0].c_str(),
                                     (ResourceMap *)&required_resources, (Objects *)&args,
                                     (ObjectIds *)&arg_reference_ids,
                                     (ObjectIds *)&return_ids, (Objects *)results));

        if (s == NULL) {
          return ray::Status::OK();
        }
        return *s;
      },
      nullptr, NULL);

  w->StartExecutingTasks();
}

Ray *ray_new(char *store_socket, char *raylet_socket, int job, struct GcsConnectArgs gcs,
             char *node_ip_address, int node_manager_port) {
  // TODO: avoid the copies here
  const std::string &store_socket_s(store_socket);
  const std::string &raylet_socket_s(raylet_socket);
  const std::string &node_ip_address_s(node_ip_address);
  const std::string &log_dir = "";
  ray::JobID jid = ray::JobID::FromInt(job);
  const ray::gcs::GcsClientOptions gcs_opts =
      ray::gcs::GcsClientOptions(gcs.redis_ip, gcs.redis_port, gcs.redis_password);

  ray::CoreWorker *w = new ray::CoreWorker(
      ray::WorkerType::DRIVER, ray::Language::NATIVE, store_socket_s, raylet_socket_s,
      jid, gcs_opts, log_dir, node_ip_address_s, node_manager_port, NULL, nullptr, NULL);
  return (Ray *)w;
}

void ray_free(Ray *ray) { delete (ray::CoreWorker *)ray; }

ExecutionResult *ray_allocate_returns(Ray *ray, const ObjectIds *ids,
                                      const size_t sizes[], Objects *return_objects) {
  auto ids_v = (std::vector<ray::ObjectID> *)(ids);
  auto sizes_v = std::vector<size_t>(sizes, sizes + ids_v->size());
  auto metadata_v = std::vector<std::shared_ptr<ray::Buffer>>(
      ids_v->size(),
      std::shared_ptr<ray::Buffer>(
          std::make_shared<ray::LocalMemoryBuffer>((uint8_t *)"", 1, true)));

  ray::Status s =
      ((ray::CoreWorker *)(ray))
          ->AllocateReturnObjects(
              *ids_v, sizes_v, metadata_v,
              (std::vector<std::shared_ptr<ray::RayObject>> *)(return_objects));
  if (s.ok()) {
    return NULL;
  }
  return (ExecutionResult *)(new ray::Status(s));
}

ExecutionResult *ray_put(Ray *ray, const char *bytes, size_t len, ObjectId *oid) {
  // TODO: this is awkward -- how do we do zero-copy?
  std::shared_ptr<ray::LocalMemoryBuffer> data =
      std::make_shared<ray::LocalMemoryBuffer>((uint8_t *)bytes, len, true);

  ray::ObjectID *object_id = (ray::ObjectID *)(oid);
  ray::RayObject obj = ray::RayObject(
      std::dynamic_pointer_cast<ray::Buffer, ray::LocalMemoryBuffer>(data), nullptr);

  ray::Status s = ((ray::CoreWorker *)(ray))->Put(obj, object_id);
  if (s.ok()) {
    return NULL;
  }
  // XXX: do we need to also call Seal here?
  return (ExecutionResult *)(new ray::Status(s));
}

ExecutionResult *ray_get(Ray *ray, const ObjectIds *ids, int64_t timeout_ms,
                         Objects *results) {
  ray::Status s = ((ray::CoreWorker *)(ray))
                      ->Get(*(std::vector<ray::ObjectID> *)(ids), timeout_ms,
                            (std::vector<std::shared_ptr<ray::RayObject>> *)(results));
  if (s.ok()) {
    return NULL;
  }
  return (ExecutionResult *)(new ray::Status(s));
}

ExecutionResult *ray_wait(Ray *ray, const ObjectIds *ids, size_t num_objects,
                          int64_t timeout_ms, Ready *r) {
  ray::Status s = ((ray::CoreWorker *)(ray))
                      ->Wait(*(std::vector<ray::ObjectID> *)(ids), num_objects,
                             timeout_ms, (std::vector<bool> *)(r));
  if (s.ok()) {
    return NULL;
  }
  return (ExecutionResult *)(new ray::Status(s));
}

ExecutionResult *ray_submit_task(Ray *ray, ActorId *actor, const char *function,
                                 struct TaskOptions opts, Args *args,
                                 ObjectIds *return_ids) {
  auto resources = (std::unordered_map<std::string, double> *)(opts.resources);
  ray::TaskOptions task_options(1, false, *resources);
  const std::string func(function);
  ray::Status s;
  if (actor == NULL) {
    s = ((ray::CoreWorker *)(ray))
            ->SubmitTask(
                ray::RayFunction(ray::Language::NATIVE, std::vector<std::string>({func})),
                *(std::vector<ray::TaskArg> *)(args), task_options,
                (std::vector<ray::ObjectID> *)(return_ids));
  } else {
    s = ((ray::CoreWorker *)(ray))
            ->SubmitActorTask(
                *(ray::ActorID *)(actor),
                ray::RayFunction(ray::Language::NATIVE, std::vector<std::string>({func})),
                *(std::vector<ray::TaskArg> *)(args), task_options,
                (std::vector<ray::ObjectID> *)(return_ids));
  }

  if (s.ok()) {
    return NULL;
  }
  return (ExecutionResult *)(new ray::Status(s));
}

//
// Helper methods: ObjectIds
// Equivalent to: std::vector<ray::ObjectID>
//
ObjectIds *object_ids_new() { return (ObjectIds *)(new std::vector<ray::ObjectID>()); }
ObjectIds *object_ids_with_capacity(size_t cap) {
  auto v = new std::vector<ray::ObjectID>();
  v->reserve(cap);
  return (ObjectIds *)(v);
}
ObjectId *object_ids_at(ObjectIds *oids, size_t index) {
  return (ObjectId *)(&((std::vector<ray::ObjectID> *)(oids))->at(index));
}
size_t object_ids_len(ObjectIds *oids) {
  return ((std::vector<ray::ObjectID> *)(oids))->size();
}
void object_ids_clear(ObjectIds *oids) {
  ((std::vector<ray::ObjectID> *)(oids))->clear();
}
void object_ids_free(ObjectIds *oids) { delete ((std::vector<ray::ObjectID> *)(oids)); }

//
// Helper methods: Objects
// Equivalent to: std::vector<std::shared_ptr<ray::RayObject>>
//
Objects *objects_new() {
  return (Objects *)(new std::vector<std::shared_ptr<ray::RayObject>>());
}
Objects *objects_with_capacity(size_t cap) {
  auto v = new std::vector<std::shared_ptr<ray::RayObject>>();
  v->reserve(cap);
  return (Objects *)(v);
}
Object *objects_at(Objects *os, size_t index) {
  return (Object *)(&((std::vector<std::shared_ptr<ray::RayObject>> *)(os))->at(index));
};
size_t objects_len(Objects *os) {
  return ((std::vector<std::shared_ptr<ray::RayObject>> *)(os))->size();
}
void objects_clear(Objects *os) {
  ((std::vector<std::shared_ptr<ray::RayObject>> *)(os))->clear();
}
void objects_free(Objects *os) {
  delete ((std::vector<std::shared_ptr<ray::RayObject>> *)(os));
}

//
// Helper methods: Object
// Equivalent to: std::shared_ptr<ray::RayObject>
//
char *object_buf(Object *o) {
  return (char *)((*(std::shared_ptr<ray::RayObject> *)(o))->GetData()->Data());
}
size_t object_buf_size(Object *o) {
  return ((std::shared_ptr<ray::RayObject> *)(o))->get()->GetData()->Size();
}

//
// Helper methods: Ready
// Equivalent to: std::vector<bool>
//
Ready *ready_new() { return (Ready *)(new std::vector<bool>()); }
Ready *ready_with_capacity(size_t cap) {
  auto v = new std::vector<bool>();
  v->reserve(cap);
  return (Ready *)(v);
}
bool ready_at(Ready *r, size_t index) { return ((std::vector<bool> *)(r))->at(index); }
size_t ready_len(Ready *r) { return ((std::vector<bool> *)(r))->size(); }
void ready_clear(Ready *r) { ((std::vector<bool> *)(r))->clear(); }
void ready_free(Ready *r) { delete ((std::vector<bool> *)(r)); }

//
// Helper methods: Args
// Equivalent to: std::vector<ray::TaskArg>
//
Args *args_new() { return (Args *)(new std::vector<ray::TaskArg>()); }
Args *args_with_capacity(size_t cap) {
  auto v = new std::vector<ray::TaskArg>();
  v->reserve(cap);
  return (Args *)(v);
}
Arg *args_at(Args *as, size_t index) {
  return (Arg *)(&((std::vector<ray::TaskArg> *)(as))->at(index));
};
size_t args_len(Args *as) { return ((std::vector<ray::TaskArg> *)(as))->size(); }
void args_clear(Args *as) { ((std::vector<ray::TaskArg> *)(as))->clear(); }
void args_free(Args *as) { delete ((std::vector<ray::TaskArg> *)(as)); }

//
// Helper methods: ResourceMap
// Equivalent to: std::unordered_map<std::string, double>
//
ResourceMap *resource_map_new() {
  return (ResourceMap *)(new std::unordered_map<std::string, double>());
}
double resources_for(ResourceMap *rm, char *index) {
  // TODO: avoid the copies here:
  const std::string &idx(index);
  return ((std::unordered_map<std::string, double> *)(rm))->at(idx);
}
void resource_map_free(ResourceMap *rm) {
  delete ((std::unordered_map<std::string, double> *)(rm));
}

//
// Helper methods: ExecutionResult
// Equivalent to: Status
//
bool is_ok(ExecutionResult *e) { return (e == NULL || ((ray::Status *)(e))->ok()); }
int err_code(ExecutionResult *e) {
  ray::Status *s = (ray::Status *)(e);
  if (s == NULL) {
    return 0;
  }
  return (int)(s->code());
}
void result_free(ExecutionResult *e) {
  if (e != NULL) {
    delete ((ray::Status *)(e));
  }
}
}

// vim: expandtab:sw=2:ts=2:sts=2
