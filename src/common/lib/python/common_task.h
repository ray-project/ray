#ifndef COMMON_TASK_H
#define COMMON_TASK_H

#include <vector>

#include <Python.h>
#include "marshal.h"
#include "structmember.h"

#include "common.h"
#include "task.h"
#include "ray/raylet/task_spec.h"
#include <memory>

typedef char TaskSpec;
class TaskBuilder;

extern TaskBuilder *g_task_builder;

class TaskInterface {
public:
  virtual ~TaskInterface() {}
  virtual PyObject *to_string() = 0;
  virtual FunctionID function_id() = 0;
  virtual ActorID actor_id() = 0;
  virtual int64_t actor_counter() = 0;
  virtual UniqueID driver_id() = 0;
  virtual TaskID task_id() = 0;
  virtual TaskID parent_task_id() = 0;
  virtual int64_t parent_counter() = 0;
  virtual int64_t num_args() = 0;
  virtual int arg_id_count(int index) = 0;
  virtual ObjectID arg_id(int64_t arg_index, int64_t id_index) = 0;
  virtual const uint8_t *arg_val(int64_t arg_index) = 0;
  virtual int64_t arg_length(int64_t arg_index) = 0;
  virtual ActorID actor_creation_id() = 0;
  virtual ObjectID actor_creation_dummy_object_id() = 0;
  virtual std::unordered_map<std::string, double> get_required_resources() = 0;
  virtual int64_t num_returns() = 0;
  virtual ObjectID return_id(int64_t return_index) = 0;
  virtual void to_serialized_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
    std::vector<ray::ObjectID> execution_dependencies) = 0;
  virtual void to_submit_message(flatbuffers::FlatBufferBuilder &fbb,
    const std::vector<ObjectID> &execution_dependencies) = 0;
};

class NonRayletTask : public TaskInterface {
public:
  NonRayletTask(UniqueID &driver_id, TaskID &parent_task_id, int parent_counter,
    ActorID &actor_creation_id, ObjectID &actor_creation_dummy_object_id, 
    UniqueID &actor_id, UniqueID &actor_handle_id, int actor_counter, 
    bool is_actor_checkpoint_method, FunctionID &function_id, int num_returns,
    PyObject *arguments,
    std::unordered_map<std::string, double>& required_resources,
    std::function<PyObject *(PyObject *)> callMethodObjArgs,
    std::function<bool(PyObject *)> isPyObjectIDType);
  NonRayletTask(const char* data, int data_size);
  NonRayletTask(TaskSpec *task_spec, int64_t task_size);
  ~NonRayletTask();
  PyObject *to_string();
  FunctionID function_id();
  ActorID actor_id();
  int64_t actor_counter();
  UniqueID driver_id();
  TaskID task_id();
  TaskID parent_task_id();
  int64_t parent_counter();
  int64_t num_args();
  int arg_id_count(int index);
  ObjectID arg_id(int64_t arg_index, int64_t id_index);
  const uint8_t *arg_val(int64_t arg_index);
  int64_t arg_length(int64_t arg_index);
  ActorID actor_creation_id();
  ObjectID actor_creation_dummy_object_id();
  std::unordered_map<std::string, double> get_required_resources();
  int64_t num_returns();
  ObjectID return_id(int64_t return_index);
  void to_serialized_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
    std::vector<ray::ObjectID> execution_dependencies);
  void to_submit_message(flatbuffers::FlatBufferBuilder &fbb,
    const std::vector<ObjectID> &execution_dependencies);
private:
  int64_t size;
  // The task spec to use in the non-raylet case.
  TaskSpec *spec;
};

class RayletTask : public TaskInterface {
public:
  RayletTask(UniqueID &driver_id, TaskID &parent_task_id, int parent_counter,
    ActorID &actor_creation_id, ObjectID &actor_creation_dummy_object_id, 
    UniqueID &actor_id, UniqueID &actor_handle_id, int actor_counter, 
    bool is_actor_checkpoint_method, FunctionID &function_id, int num_returns,
    PyObject *arguments,
    std::unordered_map<std::string, double>& required_resources,
    std::function<PyObject *(PyObject *)> callMethodObjArgs,
    std::function<bool(PyObject *)> isPyObjectIDType);
  PyObject *to_string();
  FunctionID function_id();
  ActorID actor_id();
  int64_t actor_counter();
  UniqueID driver_id();
  TaskID task_id();
  TaskID parent_task_id();
  int64_t parent_counter();
  int64_t num_args();
  int arg_id_count(int index);
  ObjectID arg_id(int64_t arg_index, int64_t id_index);
  const uint8_t *arg_val(int64_t arg_index);
  int64_t arg_length(int64_t arg_index);
  ActorID actor_creation_id();
  ObjectID actor_creation_dummy_object_id();
  std::unordered_map<std::string, double> get_required_resources();
  int64_t num_returns();
  ObjectID return_id(int64_t return_index);
  void to_serialized_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
    std::vector<ray::ObjectID> execution_dependencies);
  void to_submit_message(flatbuffers::FlatBufferBuilder &fbb,
    const std::vector<ObjectID> &execution_dependencies);
private:
  // The task spec to use in the raylet case.
  std::unique_ptr<ray::raylet::TaskSpecification> task_spec;
};

// clang-format off
typedef struct {
  PyObject_HEAD
  ray::ObjectID object_id;
} PyObjectID;

typedef struct {
  PyObject_HEAD
  TaskInterface* taskInterface;
  std::vector<ray::ObjectID> *execution_dependencies;
} PyTask;
// clang-format on
#endif /* COMMON_TASK_H */
