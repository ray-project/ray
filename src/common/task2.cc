#include <limits.h>

#include "common_protocol.h"

#include "task2.h"

extern "C" {
#include "sha256.h"
}

ObjectID task_compute_return_id(TaskID task_id, int64_t return_index) {
  /* Here, return_indices need to be >= 0, so we can use negative
   * indices for put. */
  DCHECK(return_index >= 0);
  /* TODO(rkn): This line requires object and task IDs to be the same size. */
  ObjectID return_id = task_id;
  int64_t *first_bytes = (int64_t *) &return_id;
  /* XOR the first bytes of the object ID with the return index. We add one so
   * the first return ID is not the same as the task ID. */
  *first_bytes = *first_bytes ^ (return_index + 1);
  return return_id;
}

class TaskBuilder {
public:
  void Start(UniqueID driver_id, TaskID parent_task_id, int64_t parent_counter,
             ActorID actor_id, int64_t actor_counter, FunctionID function_id, int64_t num_returns) {
    driver_id_ = driver_id;
    parent_task_id_ = parent_task_id;
    parent_counter_ = parent_counter;
    actor_id_ = actor_id;
    actor_counter_ = actor_counter;
    function_id_ = function_id;
    num_returns_ = num_returns;

    /* Compute hashes. */
    sha256_init(&ctx);
    sha256_update(&ctx, (BYTE*) &driver_id, sizeof(driver_id));
    sha256_update(&ctx, (BYTE*) &parent_task_id, sizeof(parent_task_id));
    sha256_update(&ctx, (BYTE*) &parent_counter, sizeof(parent_counter));
    sha256_update(&ctx, (BYTE*) &actor_id, sizeof(actor_id));
    sha256_update(&ctx, (BYTE*) &actor_counter, sizeof(actor_counter));
    sha256_update(&ctx, (BYTE*) &function_id, sizeof(function_id));
  }
  
  void NextReferenceArgument(ObjectID object_id) {
    args.push_back(CreateArg(fbb, to_flat(fbb, object_id)));
    sha256_update(&ctx, (BYTE*) &object_id, sizeof(object_id));
  }

  void NextValueArgument(uint8_t *value, int64_t length) {
    auto arg = fbb.CreateString((const char *) value, length);
    auto empty_id = fbb.CreateString("", 0);
    args.push_back(CreateArg(fbb, empty_id, arg));
    sha256_update(&ctx, (BYTE*) &value, length);
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
      returns.push_back(to_flat(fbb, return_id));
    }
    /* Create TaskSpec. */
    auto message = CreateTaskSpec(fbb,
      to_flat(fbb, driver_id_), to_flat(fbb, task_id),
      to_flat(fbb, parent_task_id_), parent_counter_,
      to_flat(fbb, actor_id_), actor_counter_, to_flat(fbb, function_id_),
      arguments, fbb.CreateVector(returns));
    /* Finish the TaskSpec. */
    fbb.Finish(message);
    *size = fbb.GetSize();
    uint8_t *result = (uint8_t *) malloc(*size);
    memcpy(result, fbb.GetBufferPointer(), *size);
    fbb.Clear();
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
  FunctionID function_id_;
  int64_t num_returns_;
};

TaskBuilder *make_task_builder(void) {
  return new TaskBuilder();
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

void start_construct_task_spec(TaskBuilder *builder,
                               UniqueID driver_id,
                               TaskID parent_task_id,
                               int64_t parent_counter,
                               ActorID actor_id,
                               int64_t actor_counter,
                               FunctionID function_id,
                               int64_t num_returns) {
  builder->Start(driver_id, parent_task_id, parent_counter, actor_id, actor_counter, function_id, num_returns);
}

uint8_t *finish_construct_task_spec(TaskBuilder *builder, int64_t *size) {
  return builder->Finish(size);
}

void task_args_add_ref(TaskBuilder *builder, ObjectID object_id) {
  builder->NextReferenceArgument(object_id);
}

void task_args_add_val(TaskBuilder *builder, uint8_t *value, int64_t length) {
  builder->NextValueArgument(value, length);
}

void task_spec_set_required_resource(TaskBuilder *builder,
                                     int64_t resource_index,
                                     double value) {
  
  /*
  CHECK(resource_index < Task_required_resources_reserved_len(builder->B));
  double *resource_vector = Task_required_resources_edit(builder->B);
  resource_vector[resource_index] = value;
  */
}

/* Functions for reading tasks. */

TaskID task_spec_id(uint8_t *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return from_flat(message->task_id());
}

FunctionID task_function(uint8_t *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return from_flat(message->function_id());
}

ActorID task_spec_actor_id(task_spec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return from_flat(message->actor_id());
}

int64_t task_spec_actor_counter(task_spec *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return message->actor_counter();
}

int64_t task_num_args(uint8_t *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return message->args()->size();
}

ObjectID task_arg_id(uint8_t *spec, int64_t arg_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return from_flat(message->args()->Get(arg_index)->object_id());
  return NIL_ID;
}

const uint8_t *task_arg_val(uint8_t *spec, int64_t arg_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return (uint8_t *) message->args()->Get(arg_index)->data()->c_str();
}

int64_t task_arg_length(uint8_t *spec, int64_t arg_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return message->args()->Get(arg_index)->data()->size();
}

int64_t task_num_returns(uint8_t *spec) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return message->returns()->size();
}

bool task_arg_by_ref(uint8_t *spec, int64_t arg_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return message->args()->Get(arg_index)->object_id()->size() != 0;
}

ObjectID task_return(uint8_t *spec, int64_t return_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return from_flat(message->returns()->Get(return_index));
}

double task_spec_get_required_resource(const task_spec *spec,
                                       int64_t resource_index) {
  CHECK(spec);
  auto message = flatbuffers::GetRoot<TaskSpec>(spec);
  return message->required_resources()->Get(resource_index);
}

/* TASK INSTANCES */

Task *Task_alloc(task_spec *spec, int64_t task_spec_size, int state, DBClientID local_scheduler_id) {
  int64_t size = sizeof(Task) - sizeof(task_spec) + task_spec_size;
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
  return sizeof(Task) - sizeof(task_spec) + task_arg->task_spec_size;
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

task_spec *Task_task_spec(Task *task) {
  return &task->spec;
}

int64_t Task_task_spec_size(Task *task) {
  return task->task_spec_size;
}

TaskID Task_task_id(Task *task) {
  task_spec *spec = Task_task_spec(task);
  return task_spec_id(spec);
}

void Task_free(Task *task) {
  free(task);
}
