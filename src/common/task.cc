#include <limits.h>

#include "common_protocol.h"

#include "task.h"

extern "C" {
#include "sha256.h"
}

ObjectID task_compute_return_id(TaskID task_id, int64_t return_index) {
  /* Here, return_indices need to be >= 0, so we can use negative
   * indices for put. */
  RAY_DCHECK(return_index >= 0);
  /* TODO(rkn): This line requires object and task IDs to be the same size. */
  ObjectID return_id = task_id;
  int64_t *first_bytes = (int64_t *) &return_id;
  /* XOR the first bytes of the object ID with the return index. We add one so
   * the first return ID is not the same as the task ID. */
  *first_bytes = *first_bytes ^ (return_index + 1);
  return return_id;
}

ObjectID task_compute_put_id(TaskID task_id, int64_t put_index) {
  RAY_DCHECK(put_index >= 0);
  /* TODO(pcm): This line requires object and task IDs to be the same size. */
  ObjectID put_id = task_id;
  int64_t *first_bytes = (int64_t *) &put_id;
  /* XOR the first bytes of the object ID with the return index. We add one so
   * the first return ID is not the same as the task ID. */
  *first_bytes = *first_bytes ^ (-put_index - 1);
  return put_id;
}

class TaskBuilder {
 public:
  void Start(UniqueID driver_id,
             TaskID parent_task_id,
             int64_t parent_counter,
             ActorID actor_creation_id,
             ObjectID actor_creation_dummy_object_id,
             ActorID actor_id,
             ActorHandleID actor_handle_id,
             int64_t actor_counter,
             bool is_actor_checkpoint_method,
             FunctionID function_id,
             int64_t num_returns) {
    driver_id_ = driver_id;
    parent_task_id_ = parent_task_id;
    parent_counter_ = parent_counter;
    actor_creation_id_ = actor_creation_id;
    actor_creation_dummy_object_id_ = actor_creation_dummy_object_id;
    actor_id_ = actor_id;
    actor_handle_id_ = actor_handle_id;
    actor_counter_ = actor_counter;
    is_actor_checkpoint_method_ = is_actor_checkpoint_method;
    function_id_ = function_id;
    num_returns_ = num_returns;

    /* Compute hashes. */
    sha256_init(&ctx);
    sha256_update(&ctx, (BYTE *) &driver_id, sizeof(driver_id));
    sha256_update(&ctx, (BYTE *) &parent_task_id, sizeof(parent_task_id));
    sha256_update(&ctx, (BYTE *) &parent_counter, sizeof(parent_counter));
    sha256_update(&ctx, (BYTE *) &actor_creation_id, sizeof(actor_creation_id));
    sha256_update(&ctx, (BYTE *) &actor_creation_dummy_object_id,
                  sizeof(actor_creation_dummy_object_id));
    sha256_update(&ctx, (BYTE *) &actor_id, sizeof(actor_id));
    sha256_update(&ctx, (BYTE *) &actor_counter, sizeof(actor_counter));
    sha256_update(&ctx, (BYTE *) &is_actor_checkpoint_method,
                  sizeof(is_actor_checkpoint_method));
    sha256_update(&ctx, (BYTE *) &function_id, sizeof(function_id));
  }

  void NextReferenceArgument(ObjectID object_ids[], int num_object_ids) {
    args.push_back(
        CreateArg(fbb, to_flatbuf(fbb, &object_ids[0], num_object_ids)));
    sha256_update(&ctx, (BYTE *) &object_ids[0],
                  sizeof(object_ids[0]) * num_object_ids);
  }

  void NextValueArgument(uint8_t *value, int64_t length) {
    auto arg = fbb.CreateString((const char *) value, length);
    auto empty_ids = fbb.CreateVectorOfStrings({});
    args.push_back(CreateArg(fbb, empty_ids, arg));
    sha256_update(&ctx, (BYTE *) value, length);
  }

  void SetRequiredResource(const std::string &resource_name, double value) {
    RAY_CHECK(resource_map_.count(resource_name) == 0);
    resource_map_[resource_name] = value;
  }

  uint8_t *Finish(int64_t *size) {
    /* Add arguments. */
    auto arguments = fbb.CreateVector(args);
    /* Update hash. */
    BYTE buff[DIGEST_SIZE];
    sha256_final(&ctx, buff);
    TaskID task_id;
    RAY_CHECK(sizeof(task_id) <= DIGEST_SIZE);
    memcpy(&task_id, buff, sizeof(task_id));
    /* Add return object IDs. */
    std::vector<flatbuffers::Offset<flatbuffers::String>> returns;
    for (int64_t i = 0; i < num_returns_; i++) {
      ObjectID return_id = task_compute_return_id(task_id, i);
      returns.push_back(to_flatbuf(fbb, return_id));
    }
    /* Create TaskInfo. */
    auto message = CreateTaskInfo(
        fbb, to_flatbuf(fbb, driver_id_), to_flatbuf(fbb, task_id),
        to_flatbuf(fbb, parent_task_id_), parent_counter_,
        to_flatbuf(fbb, actor_creation_id_),
        to_flatbuf(fbb, actor_creation_dummy_object_id_),
        to_flatbuf(fbb, actor_id_), to_flatbuf(fbb, actor_handle_id_),
        actor_counter_, is_actor_checkpoint_method_,
        to_flatbuf(fbb, function_id_), arguments, fbb.CreateVector(returns),
        map_to_flatbuf(fbb, resource_map_));
    /* Finish the TaskInfo. */
    fbb.Finish(message);
    *size = fbb.GetSize();
    uint8_t *result = (uint8_t *) malloc(*size);
    memcpy(result, fbb.GetBufferPointer(), *size);
    fbb.Clear();
    args.clear();
    resource_map_.clear();
    return result;
  }

 private:
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<flatbuffers::Offset<Arg>> args;
  SHA256_CTX ctx;

  /* Data for the builder. */
  UniqueID driver_id_;
  TaskID parent_task_id_;
  int64_t parent_counter_;
  ActorID actor_creation_id_;
  ObjectID actor_creation_dummy_object_id_;
  ActorID actor_id_;
  ActorID actor_handle_id_;
  int64_t actor_counter_;
  bool is_actor_checkpoint_method_;
  FunctionID function_id_;
  int64_t num_returns_;
  std::unordered_map<std::string, double> resource_map_;
};

TaskBuilder *make_task_builder(void) {
  return new TaskBuilder();
}

void free_task_builder(TaskBuilder *builder) {
  delete builder;
}

bool TaskID_equal(TaskID first_id, TaskID second_id) {
  return first_id == second_id;
}

bool TaskID_is_nil(TaskID id) {
  return id.is_nil();
}

bool ActorID_equal(ActorID first_id, ActorID second_id) {
  return first_id == second_id;
}

bool FunctionID_equal(FunctionID first_id, FunctionID second_id) {
  return first_id == second_id;
}

bool FunctionID_is_nil(FunctionID id) {
  return id.is_nil();
}

/* Functions for building tasks. */

void TaskSpec_start_construct(TaskBuilder *builder,
                              UniqueID driver_id,
                              TaskID parent_task_id,
                              int64_t parent_counter,
                              ActorID actor_creation_id,
                              ObjectID actor_creation_dummy_object_id,
                              ActorID actor_id,
                              ActorID actor_handle_id,
                              int64_t actor_counter,
                              bool is_actor_checkpoint_method,
                              FunctionID function_id,
                              int64_t num_returns) {
  builder->Start(driver_id, parent_task_id, parent_counter, actor_creation_id,
                 actor_creation_dummy_object_id, actor_id, actor_handle_id,
                 actor_counter, is_actor_checkpoint_method, function_id,
                 num_returns);
}

TaskSpec *TaskSpec_finish_construct(TaskBuilder *builder, int64_t *size) {
  return reinterpret_cast<TaskSpec *>(builder->Finish(size));
}

void TaskSpec_args_add_ref(TaskBuilder *builder,
                           ObjectID object_ids[],
                           int num_object_ids) {
  builder->NextReferenceArgument(&object_ids[0], num_object_ids);
}

void TaskSpec_args_add_val(TaskBuilder *builder,
                           uint8_t *value,
                           int64_t length) {
  builder->NextValueArgument(value, length);
}

void TaskSpec_set_required_resource(TaskBuilder *builder,
                                    const std::string &resource_name,
                                    double value) {
  builder->SetRequiredResource(resource_name, value);
}

/* Functions for reading tasks. */

TaskID TaskSpec_task_id(const TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(*message->task_id());
}

FunctionID TaskSpec_function(TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(*message->function_id());
}

ActorID TaskSpec_actor_id(TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(*message->actor_id());
}

ActorID TaskSpec_actor_handle_id(TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(*message->actor_handle_id());
}

bool TaskSpec_is_actor_task(TaskSpec *spec) {
  return !TaskSpec_actor_id(spec).is_nil();
}

ActorID TaskSpec_actor_creation_id(TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(*message->actor_creation_id());
}

ObjectID TaskSpec_actor_creation_dummy_object_id(TaskSpec *spec) {
  RAY_CHECK(spec);
  // The task must be an actor method.
  RAY_CHECK(TaskSpec_is_actor_task(spec));
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(*message->actor_creation_dummy_object_id());
}

bool TaskSpec_is_actor_creation_task(TaskSpec *spec) {
  return !TaskSpec_actor_creation_id(spec).is_nil();
}

int64_t TaskSpec_actor_counter(TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return std::abs(message->actor_counter());
}

bool TaskSpec_is_actor_checkpoint_method(TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->is_actor_checkpoint_method();
}

ObjectID TaskSpec_actor_dummy_object(TaskSpec *spec) {
  RAY_CHECK(TaskSpec_is_actor_task(spec));
  /* The last return value for actor tasks is the dummy object that
   * represents that this task has completed execution. */
  int64_t num_returns = TaskSpec_num_returns(spec);
  return TaskSpec_return(spec, num_returns - 1);
}

UniqueID TaskSpec_driver_id(const TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(*message->driver_id());
}

TaskID TaskSpec_parent_task_id(const TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(*message->parent_task_id());
}

int64_t TaskSpec_parent_counter(TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->parent_counter();
}

int64_t TaskSpec_num_args(TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->args()->size();
}

int64_t TaskSpec_num_args_by_ref(TaskSpec *spec) {
  int64_t num_args = TaskSpec_num_args(spec);
  int64_t num_args_by_ref = 0;
  for (int64_t i = 0; i < num_args; i++) {
    if (TaskSpec_arg_by_ref(spec, i)) {
      num_args_by_ref++;
    }
  }
  return num_args_by_ref;
}

int TaskSpec_arg_id_count(TaskSpec *spec, int64_t arg_index) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  auto ids = message->args()->Get(arg_index)->object_ids();
  if (ids == nullptr) {
    return 0;
  } else {
    return ids->size();
  }
}

ObjectID TaskSpec_arg_id(TaskSpec *spec, int64_t arg_index, int64_t id_index) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(
      *message->args()->Get(arg_index)->object_ids()->Get(id_index));
}

const uint8_t *TaskSpec_arg_val(TaskSpec *spec, int64_t arg_index) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return (uint8_t *) message->args()->Get(arg_index)->data()->c_str();
}

int64_t TaskSpec_arg_length(TaskSpec *spec, int64_t arg_index) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->args()->Get(arg_index)->data()->size();
}

int64_t TaskSpec_num_returns(TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->returns()->size();
}

bool TaskSpec_arg_by_ref(TaskSpec *spec, int64_t arg_index) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->args()->Get(arg_index)->object_ids()->size() != 0;
}

ObjectID TaskSpec_return(TaskSpec *spec, int64_t return_index) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(*message->returns()->Get(return_index));
}

double TaskSpec_get_required_resource(const TaskSpec *spec,
                                      const std::string &resource_name) {
  // This is a bit ugly. However it shouldn't be much of a performance issue
  // because there shouldn't be many distinct resources in a single task spec.
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  for (size_t i = 0; i < message->required_resources()->size(); i++) {
    const ResourcePair *resource_pair = message->required_resources()->Get(i);
    if (string_from_flatbuf(*resource_pair->key()) == resource_name) {
      return resource_pair->value();
    }
  }
  return 0;
}

const std::unordered_map<std::string, double> TaskSpec_get_required_resources(
    const TaskSpec *spec) {
  RAY_CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return map_from_flatbuf(*message->required_resources());
}

TaskSpec *TaskSpec_copy(TaskSpec *spec, int64_t task_spec_size) {
  TaskSpec *copy = (TaskSpec *) malloc(task_spec_size);
  memcpy(copy, spec, task_spec_size);
  return copy;
}

void TaskSpec_free(TaskSpec *spec) {
  free(spec);
}

TaskExecutionSpec::TaskExecutionSpec(
    const std::vector<ObjectID> &execution_dependencies,
    const TaskSpec *spec,
    int64_t task_spec_size,
    int spillback_count)
    : execution_dependencies_(execution_dependencies),
      task_spec_size_(task_spec_size),
      last_timestamp_(0),
      spillback_count_(spillback_count) {
  TaskSpec *spec_copy = new TaskSpec[task_spec_size_];
  memcpy(spec_copy, spec, task_spec_size);
  spec_ = std::unique_ptr<TaskSpec[]>(spec_copy);
}

TaskExecutionSpec::TaskExecutionSpec(
    const std::vector<ObjectID> &execution_dependencies,
    const TaskSpec *spec,
    int64_t task_spec_size)
    : TaskExecutionSpec(execution_dependencies, spec, task_spec_size, 0) {}

TaskExecutionSpec::TaskExecutionSpec(TaskExecutionSpec *other)
    : execution_dependencies_(other->execution_dependencies_),
      task_spec_size_(other->task_spec_size_),
      last_timestamp_(other->last_timestamp_),
      spillback_count_(other->spillback_count_) {
  TaskSpec *spec_copy = new TaskSpec[task_spec_size_];
  memcpy(spec_copy, other->spec_.get(), task_spec_size_);
  spec_ = std::unique_ptr<TaskSpec[]>(spec_copy);
}

const std::vector<ObjectID> &TaskExecutionSpec::ExecutionDependencies() const {
  return execution_dependencies_;
}

void TaskExecutionSpec::SetExecutionDependencies(
    const std::vector<ObjectID> &dependencies) {
  execution_dependencies_ = dependencies;
}

int64_t TaskExecutionSpec::SpecSize() const {
  return task_spec_size_;
}

int TaskExecutionSpec::SpillbackCount() const {
  return spillback_count_;
}

void TaskExecutionSpec::IncrementSpillbackCount() {
  ++spillback_count_;
}

int64_t TaskExecutionSpec::LastTimeStamp() const {
  return last_timestamp_;
}

void TaskExecutionSpec::SetLastTimeStamp(int64_t new_timestamp) {
  last_timestamp_ = new_timestamp;
}

TaskSpec *TaskExecutionSpec::Spec() const {
  return spec_.get();
}

int64_t TaskExecutionSpec::NumDependencies() const {
  TaskSpec *spec = Spec();
  int64_t num_dependencies = TaskSpec_num_args(spec);
  num_dependencies += execution_dependencies_.size();
  return num_dependencies;
}

int TaskExecutionSpec::DependencyIdCount(int64_t dependency_index) const {
  TaskSpec *spec = Spec();
  /* The first dependencies are the arguments of the task itself, followed by
   * the execution dependencies. Find the total number of task arguments so
   * that we can index into the correct list. */
  int64_t num_args = TaskSpec_num_args(spec);
  if (dependency_index < num_args) {
    /* Index into the task arguments. */
    return TaskSpec_arg_id_count(spec, dependency_index);
  } else {
    /* Index into the execution dependencies. */
    dependency_index -= num_args;
    RAY_CHECK((size_t) dependency_index < execution_dependencies_.size());
    /* All elements in the execution dependency list have exactly one ID. */
    return 1;
  }
}

ObjectID TaskExecutionSpec::DependencyId(int64_t dependency_index,
                                         int64_t id_index) const {
  TaskSpec *spec = Spec();
  /* The first dependencies are the arguments of the task itself, followed by
   * the execution dependencies. Find the total number of task arguments so
   * that we can index into the correct list. */
  int64_t num_args = TaskSpec_num_args(spec);
  if (dependency_index < num_args) {
    /* Index into the task arguments. */
    return TaskSpec_arg_id(spec, dependency_index, id_index);
  } else {
    /* Index into the execution dependencies. */
    dependency_index -= num_args;
    RAY_CHECK((size_t) dependency_index < execution_dependencies_.size());
    return execution_dependencies_[dependency_index];
  }
}

bool TaskExecutionSpec::DependsOn(ObjectID object_id) const {
  // Iterate through the task arguments to see if it contains object_id.
  TaskSpec *spec = Spec();
  int64_t num_args = TaskSpec_num_args(spec);
  for (int i = 0; i < num_args; ++i) {
    int count = TaskSpec_arg_id_count(spec, i);
    for (int j = 0; j < count; j++) {
      ObjectID arg_id = TaskSpec_arg_id(spec, i, j);
      if (arg_id == object_id) {
        return true;
      }
    }
  }
  // Iterate through the execution dependencies to see if it contains object_id.
  for (auto dependency_id : execution_dependencies_) {
    if (dependency_id == object_id) {
      return true;
    }
  }
  // The requested object ID was not a task argument or an execution dependency.
  // This task is not dependent on it.
  return false;
}

bool TaskExecutionSpec::IsStaticDependency(int64_t dependency_index) const {
  TaskSpec *spec = Spec();
  /* The first dependencies are the arguments of the task itself, followed by
   * the execution dependencies. If the requested dependency index is a task
   * argument, then it is a task dependency. */
  int64_t num_args = TaskSpec_num_args(spec);
  return (dependency_index < num_args);
}

/* TASK INSTANCES */

Task *Task_alloc(const TaskSpec *spec,
                 int64_t task_spec_size,
                 TaskStatus state,
                 DBClientID local_scheduler_id,
                 const std::vector<ObjectID> &execution_dependencies) {
  Task *result = new Task();
  auto execution_spec =
      new TaskExecutionSpec(execution_dependencies, spec, task_spec_size);
  result->execution_spec = std::unique_ptr<TaskExecutionSpec>(execution_spec);
  result->state = state;
  result->local_scheduler_id = local_scheduler_id;
  return result;
}

Task *Task_alloc(TaskExecutionSpec &execution_spec,
                 TaskStatus state,
                 DBClientID local_scheduler_id) {
  Task *result = new Task();
  result->execution_spec = std::unique_ptr<TaskExecutionSpec>(
      new TaskExecutionSpec(&execution_spec));
  result->state = state;
  result->local_scheduler_id = local_scheduler_id;
  return result;
}

Task *Task_copy(Task *other) {
  return Task_alloc(*Task_task_execution_spec(other), other->state,
                    other->local_scheduler_id);
}

int64_t Task_size(Task *task_arg) {
  return sizeof(Task) - sizeof(TaskSpec) + task_arg->execution_spec->SpecSize();
}

TaskStatus Task_state(Task *task) {
  return task->state;
}

void Task_set_state(Task *task, TaskStatus state) {
  task->state = state;
}

DBClientID Task_local_scheduler(Task *task) {
  return task->local_scheduler_id;
}

void Task_set_local_scheduler(Task *task, DBClientID local_scheduler_id) {
  task->local_scheduler_id = local_scheduler_id;
}

TaskExecutionSpec *Task_task_execution_spec(Task *task) {
  return task->execution_spec.get();
}

TaskID Task_task_id(Task *task) {
  TaskExecutionSpec *execution_spec = Task_task_execution_spec(task);
  TaskSpec *spec = execution_spec->Spec();
  return TaskSpec_task_id(spec);
}

void Task_free(Task *task) {
  delete task;
}
