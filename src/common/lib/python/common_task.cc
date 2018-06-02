#include <common_task.h>
#include "common_protocol.h"
#include "format/local_scheduler_generated.h"


#include "ray/raylet/task.h"
#include "ray/raylet/task_spec.h"
#include "ray/raylet/task_execution_spec.h"
#include "task.h"

TaskBuilder *g_task_builder = NULL;

NonRayletTask::NonRayletTask(UniqueID &driver_id, TaskID &parent_task_id, int parent_counter,
  ActorID &actor_creation_id, ObjectID &actor_creation_dummy_object_id, UniqueID &actor_id, 
  UniqueID &actor_handle_id, int actor_counter, bool is_actor_checkpoint_method,
  FunctionID &function_id, int num_returns, PyObject *arguments, 
  std::unordered_map<std::string, double>& required_resources,
  std::function<PyObject *(PyObject *)> callMethodObjArgs,
  std::function<bool(PyObject *)> isPyObjectIDType) {
  // Construct the task specification.
  TaskSpec_start_construct(
      g_task_builder, driver_id, parent_task_id, parent_counter,
      actor_creation_id, actor_creation_dummy_object_id, actor_id,
      actor_handle_id, actor_counter, is_actor_checkpoint_method, function_id,
      num_returns);

  Py_ssize_t num_args = PyList_Size(arguments);

  // Add the task arguments.
  for (Py_ssize_t i = 0; i < num_args; ++i) {
    PyObject *arg = PyList_GetItem(arguments, i);
    if (isPyObjectIDType(arg)) {
      TaskSpec_args_add_ref(g_task_builder,
                            &(reinterpret_cast<PyObjectID *>(arg))->object_id,
                            1);
    } else {
      PyObject *data = callMethodObjArgs(arg);
      TaskSpec_args_add_val(
          g_task_builder, reinterpret_cast<uint8_t *>(PyBytes_AsString(data)),
          PyBytes_Size(data));
      Py_DECREF(data);
    }
  }
  // Set the resource requirements for the task.
  for (auto const &resource_pair : required_resources) {
    TaskSpec_set_required_resource(g_task_builder, resource_pair.first,
                                    resource_pair.second);
  }

  // Compute the task ID and the return object IDs.
  spec = TaskSpec_finish_construct(g_task_builder, &size);
}

NonRayletTask::NonRayletTask(const char* data, int data_size) 
  : size(data_size) {
  spec = TaskSpec_copy((TaskSpec *) data, size);
}

NonRayletTask::NonRayletTask(TaskSpec *task_spec, int64_t task_size) 
  :  size(task_size), spec(task_spec) {}

NonRayletTask::~NonRayletTask() {
  TaskSpec_free(spec);
}

PyObject *NonRayletTask::to_string() {
  return PyBytes_FromStringAndSize((char *) spec, size);
}

FunctionID NonRayletTask::function_id() {
  return TaskSpec_function(spec);
}

ActorID NonRayletTask::actor_id() {
  return TaskSpec_actor_id(spec);
}

int64_t NonRayletTask::actor_counter() {
  return TaskSpec_actor_counter(spec);
}

UniqueID NonRayletTask::driver_id() {
  return TaskSpec_driver_id(spec);
}

TaskID NonRayletTask::task_id() {
  return TaskSpec_task_id(spec);
}

TaskID NonRayletTask::parent_task_id() {
  return TaskSpec_parent_task_id(spec);
}

int64_t NonRayletTask::parent_counter() {
  return TaskSpec_parent_counter(spec);
}

int64_t NonRayletTask::num_args() {
  return TaskSpec_num_args(spec);
}

int NonRayletTask::arg_id_count(int index) {
  return TaskSpec_arg_id_count(spec, index);
}

ObjectID NonRayletTask::arg_id(int64_t arg_index, int64_t id_index) {
  return TaskSpec_arg_id(spec, arg_index, id_index);
}

const uint8_t *NonRayletTask::arg_val(int64_t arg_index) {
  return TaskSpec_arg_val(spec, arg_index);
}

int64_t NonRayletTask::arg_length(int64_t arg_index) {
  return TaskSpec_arg_length(spec, arg_index);
}

ActorID NonRayletTask::actor_creation_id() {
  return TaskSpec_actor_creation_id(spec);
}

ObjectID NonRayletTask::actor_creation_dummy_object_id() {
  if (TaskSpec_is_actor_task(spec)) {
    return TaskSpec_actor_creation_dummy_object_id(spec);
  } else {
    return ObjectID::nil();
  }
}

std::unordered_map<std::string, double> 
NonRayletTask::get_required_resources() {
  return TaskSpec_get_required_resources(spec);;
}

int64_t NonRayletTask::num_returns() {
  return TaskSpec_num_returns(spec);
}

ObjectID NonRayletTask::return_id(int64_t return_index) {
  return TaskSpec_return(spec, return_index);
}

void NonRayletTask::to_serialized_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
    std::vector<ray::ObjectID> execution_dependencies) {
  throw std::runtime_error("Method not implemented");
}

void NonRayletTask::to_submit_message(flatbuffers::FlatBufferBuilder &fbb,
  const std::vector<ObjectID> &dependencies) {
  TaskExecutionSpec execution_spec = TaskExecutionSpec(
        dependencies, spec, size);
  auto execution_dependencies =
      to_flatbuf(fbb, execution_spec.ExecutionDependencies());
  auto task_spec =
      fbb.CreateString(reinterpret_cast<char *>(execution_spec.Spec()),
                       execution_spec.SpecSize());
  auto message =
      CreateSubmitTaskRequest(fbb, execution_dependencies, task_spec);
  fbb.Finish(message);
}


RayletTask::RayletTask(UniqueID &driver_id, TaskID &parent_task_id, int parent_counter,
  ActorID &actor_creation_id, ObjectID &actor_creation_dummy_object_id, UniqueID &actor_id, 
  UniqueID &actor_handle_id, int actor_counter, bool is_actor_checkpoint_method, 
  FunctionID &function_id, int num_returns, PyObject *arguments,
  std::unordered_map<std::string, double>& required_resources,
  std::function<PyObject *(PyObject *)> callMethodObjArgs,
  std::function<bool(PyObject *)> isPyObjectIDType) {

  Py_ssize_t num_args = PyList_Size(arguments);

   // Parse the arguments from the list.
  std::vector<std::shared_ptr<ray::raylet::TaskArgument>> args;
  for (Py_ssize_t i = 0; i < num_args; ++i) {
    PyObject *arg = PyList_GetItem(arguments, i);
    if (isPyObjectIDType(arg)) {
      std::vector<ObjectID> references = {
          reinterpret_cast<PyObjectID *>(arg)->object_id};
      args.push_back(
          std::make_shared<ray::raylet::TaskArgumentByReference>(references));
    } else {
      PyObject *data = callMethodObjArgs(arg);
      args.push_back(std::make_shared<ray::raylet::TaskArgumentByValue>(
          reinterpret_cast<uint8_t *>(PyBytes_AsString(data)),
          PyBytes_Size(data)));
      Py_DECREF(data);
    }
  }

  task_spec.reset(new ray::raylet::TaskSpecification(
      driver_id, parent_task_id, parent_counter, actor_creation_id,
      actor_creation_dummy_object_id, actor_id, actor_handle_id,
      actor_counter, function_id, args, num_returns, required_resources));
}

PyObject *RayletTask::to_string() {
  flatbuffers::FlatBufferBuilder fbb;
  auto task_spec_string = task_spec->ToFlatbuffer(fbb);
  fbb.Finish(task_spec_string);
  return PyBytes_FromStringAndSize((char *) fbb.GetBufferPointer(),
                                    fbb.GetSize());
}

FunctionID RayletTask::function_id() {
  return task_spec->FunctionId();
}

ActorID RayletTask::actor_id() {
  return task_spec->ActorId();
}

int64_t RayletTask::actor_counter() {
  return task_spec->ActorCounter();
}

UniqueID RayletTask::driver_id() {
  return task_spec->DriverId();
}

TaskID RayletTask::task_id() {
  return task_spec->TaskId();
}

TaskID RayletTask::parent_task_id() {
  return task_spec->ParentTaskId();
}

int64_t RayletTask::parent_counter() {
  return task_spec->ParentCounter();
}

int64_t RayletTask::num_args() {
  return task_spec->NumArgs();;
}

int RayletTask::arg_id_count(int index) {
  return task_spec->ArgIdCount(index);
}

ObjectID RayletTask::arg_id(int64_t arg_index, int64_t id_index) {
  return task_spec->ArgId(arg_index, id_index);
}

const uint8_t *RayletTask::arg_val(int64_t arg_index) {
  return task_spec->ArgVal(arg_index);;
}

int64_t RayletTask::arg_length(int64_t arg_index) {
  return task_spec->ArgValLength(arg_index);
}

ActorID RayletTask::actor_creation_id() {
  return task_spec->ActorCreationId();
}

ObjectID RayletTask::actor_creation_dummy_object_id() {
  return task_spec->ActorCreationDummyObjectId();
}

std::unordered_map<std::string, double> 
RayletTask::get_required_resources() {
  return task_spec->GetRequiredResources().GetResourceMap();
}

int64_t RayletTask::num_returns() {
  return task_spec->NumReturns();
}

ObjectID RayletTask::return_id(int64_t return_index) {
  return task_spec->ReturnId(return_index);;
}

void RayletTask::to_serialized_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
    std::vector<ray::ObjectID> execution_dependencies) {
  ray::raylet::TaskExecutionSpecification 
    execution_spec(std::move(execution_dependencies));
  ray::raylet::Task task(execution_spec, *task_spec);

  auto task_flatbuffer = task.ToFlatbuffer(fbb);
  fbb.Finish(task_flatbuffer);
}

void RayletTask::to_submit_message(flatbuffers::FlatBufferBuilder &fbb, 
  const std::vector<ObjectID> &execution_dependencies) {
  auto execution_dependencies_message = to_flatbuf(fbb, execution_dependencies);
  auto message = CreateSubmitTaskRequest(fbb, execution_dependencies_message,
                                         task_spec->ToFlatbuffer(fbb));
  fbb.Finish(message);
}