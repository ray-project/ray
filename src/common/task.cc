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
             ActorID actor_id,
             int64_t actor_counter,
             bool is_actor_checkpoint_method,
             FunctionID function_id,
             int64_t num_returns) {
    driver_id_ = driver_id;
    parent_task_id_ = parent_task_id;
    parent_counter_ = parent_counter;
    actor_id_ = actor_id;
    actor_counter_ = actor_counter;
    is_actor_checkpoint_method_ = is_actor_checkpoint_method;
    function_id_ = function_id;
    num_returns_ = num_returns;

    /* Compute hashes. */
    sha256_init(&ctx);
    sha256_update(&ctx, (BYTE *) &driver_id, sizeof(driver_id));
    sha256_update(&ctx, (BYTE *) &parent_task_id, sizeof(parent_task_id));
    sha256_update(&ctx, (BYTE *) &parent_counter, sizeof(parent_counter));
    sha256_update(&ctx, (BYTE *) &actor_id, sizeof(actor_id));
    sha256_update(&ctx, (BYTE *) &actor_counter, sizeof(actor_counter));
    sha256_update(&ctx, (BYTE *) &is_actor_checkpoint_method,
                  sizeof(is_actor_checkpoint_method));
    sha256_update(&ctx, (BYTE *) &function_id, sizeof(function_id));
  }

  void NextReferenceArgument(ObjectID object_id) {
    args.push_back(CreateArg(fbb, to_flatbuf(fbb, object_id)));
    sha256_update(&ctx, (BYTE *) &object_id, sizeof(object_id));
  }

  void NextValueArgument(uint8_t *value, int64_t length) {
    auto arg = fbb.CreateString((const char *) value, length);
    auto empty_id = fbb.CreateString("", 0);
    args.push_back(CreateArg(fbb, empty_id, arg));
    sha256_update(&ctx, (BYTE *) value, length);
  }

  void SetRequiredResource(int64_t resource_index, double value) {
    if (static_cast<size_t>(resource_index) >= resource_vector_.size()) {
      /* Make sure the resource vector is constructed entry by entry,
       * in order. */
      CHECK(static_cast<size_t>(resource_index) == resource_vector_.size());
      resource_vector_.resize(resource_index + 1);
    }
    resource_vector_[resource_index] = value;
  }

  uint8_t *Finish(int64_t *size) {
    /* Add arguments. */
    auto arguments = fbb.CreateVector(args);
    /* Update hash. */
    BYTE buff[DIGEST_SIZE];
    sha256_final(&ctx, buff);
    TaskID task_id;
    CHECK(sizeof(task_id) <= DIGEST_SIZE);
    memcpy(&task_id, buff, sizeof(task_id));
    /* Add return object IDs. */
    std::vector<flatbuffers::Offset<flatbuffers::String>> returns;
    for (int64_t i = 0; i < num_returns_; i++) {
      ObjectID return_id = task_compute_return_id(task_id, i);
      returns.push_back(to_flatbuf(fbb, return_id));
    }
    /* Create TaskInfo. */
    for (int64_t i = resource_vector_.size(); i < ResourceIndex_MAX; ++i) {
      resource_vector_.push_back(0.0);
    }
    auto message = CreateTaskInfo(
        fbb, to_flatbuf(fbb, driver_id_), to_flatbuf(fbb, task_id),
        to_flatbuf(fbb, parent_task_id_), parent_counter_,
        to_flatbuf(fbb, actor_id_), actor_counter_, is_actor_checkpoint_method_,
        to_flatbuf(fbb, function_id_), arguments, fbb.CreateVector(returns),
        fbb.CreateVector(resource_vector_));
    /* Finish the TaskInfo. */
    fbb.Finish(message);
    *size = fbb.GetSize();
    uint8_t *result = (uint8_t *) malloc(*size);
    memcpy(result, fbb.GetBufferPointer(), *size);
    fbb.Clear();
    args.clear();
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
  ActorID actor_id_;
  int64_t actor_counter_;
  bool is_actor_checkpoint_method_;
  FunctionID function_id_;
  int64_t num_returns_;
  std::vector<double> resource_vector_;
};

TaskBuilder *make_task_builder(void) {
  return new TaskBuilder();
}

void free_task_builder(TaskBuilder *builder) {
  delete builder;
}

bool TaskID_equal(TaskID first_id, TaskID second_id) {
  return UNIQUE_ID_EQ(first_id, second_id);
}

bool TaskID_is_nil(TaskID id) {
  return TaskID_equal(id, NIL_TASK_ID);
}

bool ActorID_equal(ActorID first_id, ActorID second_id) {
  return UNIQUE_ID_EQ(first_id, second_id);
}

bool FunctionID_equal(FunctionID first_id, FunctionID second_id) {
  return UNIQUE_ID_EQ(first_id, second_id);
}

bool FunctionID_is_nil(FunctionID id) {
  return FunctionID_equal(id, NIL_FUNCTION_ID);
}

/* Functions for building tasks. */

void TaskSpec_start_construct(TaskBuilder *builder,
                              UniqueID driver_id,
                              TaskID parent_task_id,
                              int64_t parent_counter,
                              ActorID actor_id,
                              int64_t actor_counter,
                              bool is_actor_checkpoint_method,
                              FunctionID function_id,
                              int64_t num_returns) {
  builder->Start(driver_id, parent_task_id, parent_counter, actor_id,
                 actor_counter, is_actor_checkpoint_method, function_id,
                 num_returns);
}

uint8_t *TaskSpec_finish_construct(TaskBuilder *builder, int64_t *size) {
  return builder->Finish(size);
}

void TaskSpec_args_add_ref(TaskBuilder *builder, ObjectID object_id) {
  builder->NextReferenceArgument(object_id);
}

void TaskSpec_args_add_val(TaskBuilder *builder,
                           uint8_t *value,
                           int64_t length) {
  builder->NextValueArgument(value, length);
}

void TaskSpec_set_required_resource(TaskBuilder *builder,
                                    int64_t resource_index,
                                    double value) {
  builder->SetRequiredResource(resource_index, value);
}

/* Functions for reading tasks. */

TaskID TaskSpec_task_id(TaskSpec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(message->task_id());
}

FunctionID TaskSpec_function(TaskSpec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(message->function_id());
}

ActorID TaskSpec_actor_id(TaskSpec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(message->actor_id());
}

bool TaskSpec_is_actor_task(TaskSpec *spec) {
  return !ActorID_equal(TaskSpec_actor_id(spec), NIL_ACTOR_ID);
}

int64_t TaskSpec_actor_counter(TaskSpec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return std::abs(message->actor_counter());
}

bool TaskSpec_is_actor_checkpoint_method(TaskSpec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->is_actor_checkpoint_method();
}

bool TaskSpec_arg_is_actor_dummy_object(TaskSpec *spec, int64_t arg_index) {
  if (TaskSpec_actor_counter(spec) == 0) {
    /* The first task does not have any dependencies. */
    return false;
  } else if (TaskSpec_is_actor_checkpoint_method(spec)) {
    /* Checkpoint tasks do not have any dependencies. */
    return false;
  } else {
    /* For all other tasks, the last argument is the dummy object. */
    return arg_index == (TaskSpec_num_args(spec) - 1);
  }
}

UniqueID TaskSpec_driver_id(TaskSpec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(message->driver_id());
}

TaskID TaskSpec_parent_task_id(TaskSpec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(message->parent_task_id());
}

int64_t TaskSpec_parent_counter(TaskSpec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->parent_counter();
}

int64_t TaskSpec_num_args(TaskSpec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->args()->size();
}

ObjectID TaskSpec_arg_id(TaskSpec *spec, int64_t arg_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(message->args()->Get(arg_index)->object_id());
}

const uint8_t *TaskSpec_arg_val(TaskSpec *spec, int64_t arg_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return (uint8_t *) message->args()->Get(arg_index)->data()->c_str();
}

int64_t TaskSpec_arg_length(TaskSpec *spec, int64_t arg_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->args()->Get(arg_index)->data()->size();
}

int64_t TaskSpec_num_returns(TaskSpec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->returns()->size();
}

bool TaskSpec_arg_by_ref(TaskSpec *spec, int64_t arg_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->args()->Get(arg_index)->object_id()->size() != 0;
}

ObjectID TaskSpec_return(TaskSpec *spec, int64_t return_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return from_flatbuf(message->returns()->Get(return_index));
}

double TaskSpec_get_required_resource(const TaskSpec *spec,
                                      int64_t resource_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskInfo>(spec);
  return message->required_resources()->Get(resource_index);
}

bool TaskSpec_is_dependent_on(TaskSpec *spec, ObjectID object_id) {
  int64_t num_args = TaskSpec_num_args(spec);
  for (int i = 0; i < num_args; ++i) {
    if (TaskSpec_arg_by_ref(spec, i)) {
      ObjectID arg_id = TaskSpec_arg_id(spec, i);
      if (ObjectID_equal(arg_id, object_id)) {
        return true;
      }
    }
  }
  return false;
}

TaskSpec *TaskSpec_copy(TaskSpec *spec, int64_t task_spec_size) {
  TaskSpec *copy = (TaskSpec *) malloc(task_spec_size);
  memcpy(copy, spec, task_spec_size);
  return copy;
}

void TaskSpec_free(TaskSpec *spec) {
  free(spec);
}

/* TASK INSTANCES */

Task *Task_alloc(TaskSpec *spec,
                 int64_t task_spec_size,
                 int state,
                 DBClientID local_scheduler_id) {
  int64_t size = sizeof(Task) - sizeof(TaskSpec) + task_spec_size;
  Task *result = (Task *) malloc(size);
  memset(result, 0, size);
  result->state = state;
  result->local_scheduler_id = local_scheduler_id;
  result->task_spec_size = task_spec_size;
  memcpy(&result->spec, spec, task_spec_size);
  return result;
}

Task *Task_copy(Task *other) {
  int64_t size = Task_size(other);
  Task *copy = (Task *) malloc(size);
  CHECK(copy != NULL);
  memcpy(copy, other, size);
  return copy;
}

int64_t Task_size(Task *task_arg) {
  return sizeof(Task) - sizeof(TaskSpec) + task_arg->task_spec_size;
}

int Task_state(Task *task) {
  return task->state;
}

void Task_set_state(Task *task, int state) {
  task->state = state;
}

DBClientID Task_local_scheduler(Task *task) {
  return task->local_scheduler_id;
}

void Task_set_local_scheduler(Task *task, DBClientID local_scheduler_id) {
  task->local_scheduler_id = local_scheduler_id;
}

TaskSpec *Task_task_spec(Task *task) {
  return &task->spec;
}

int64_t Task_task_spec_size(Task *task) {
  return task->task_spec_size;
}

TaskID Task_task_id(Task *task) {
  TaskSpec *spec = Task_task_spec(task);
  return TaskSpec_task_id(spec);
}

void Task_free(Task *task) {
  free(task);
}
