#include "greatest.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "common.h"
#include "test_common.h"
#include "task.h"
#include "io.h"

SUITE(task_tests);

TEST task_test(void) {
  TaskID parent_task_id = TaskID::from_random();
  FunctionID func_id = FunctionID::from_random();
  TaskBuilder *builder = make_task_builder();
  TaskSpec_start_construct(builder, DriverID::nil(), parent_task_id, 0,
                           ActorID::nil(), ObjectID::nil(), ActorID::nil(),
                           ActorID::nil(), 0, false, func_id, 2);

  UniqueID arg1 = UniqueID::from_random();
  TaskSpec_args_add_ref(builder, &arg1, 1);
  TaskSpec_args_add_val(builder, (uint8_t *) "hello", 5);
  UniqueID arg2 = UniqueID::from_random();
  TaskSpec_args_add_ref(builder, &arg2, 1);
  TaskSpec_args_add_val(builder, (uint8_t *) "world", 5);
  /* Finish constructing the spec. This constructs the task ID and the
   * return IDs. */
  int64_t size;
  TaskSpec *spec = TaskSpec_finish_construct(builder, &size);

  /* Check that the spec was constructed as expected. */
  ASSERT(TaskSpec_num_args(spec) == 4);
  ASSERT(TaskSpec_num_returns(spec) == 2);
  ASSERT(FunctionID_equal(TaskSpec_function(spec), func_id));
  ASSERT(TaskSpec_arg_id(spec, 0, 0) == arg1);
  ASSERT(memcmp(TaskSpec_arg_val(spec, 1), (uint8_t *) "hello",
                TaskSpec_arg_length(spec, 1)) == 0);
  ASSERT(TaskSpec_arg_id(spec, 2, 0) == arg2);
  ASSERT(memcmp(TaskSpec_arg_val(spec, 3), (uint8_t *) "world",
                TaskSpec_arg_length(spec, 3)) == 0);

  TaskSpec_free(spec);
  free_task_builder(builder);
  PASS();
}

TEST deterministic_ids_test(void) {
  TaskBuilder *builder = make_task_builder();
  /* Define the inputs to the task construction. */
  TaskID parent_task_id = TaskID::from_random();
  FunctionID func_id = FunctionID::from_random();
  UniqueID arg1 = UniqueID::from_random();
  uint8_t *arg2 = (uint8_t *) "hello world";

  /* Construct a first task. */
  TaskSpec_start_construct(builder, DriverID::nil(), parent_task_id, 0,
                           ActorID::nil(), ObjectID::nil(), ActorID::nil(),
                           ActorID::nil(), 0, false, func_id, 3);
  TaskSpec_args_add_ref(builder, &arg1, 1);
  TaskSpec_args_add_val(builder, arg2, 11);
  int64_t size1;
  TaskSpec *spec1 = TaskSpec_finish_construct(builder, &size1);

  /* Construct a second identical task. */
  TaskSpec_start_construct(builder, DriverID::nil(), parent_task_id, 0,
                           ActorID::nil(), ObjectID::nil(), ActorID::nil(),
                           ActorID::nil(), 0, false, func_id, 3);
  TaskSpec_args_add_ref(builder, &arg1, 1);
  TaskSpec_args_add_val(builder, arg2, 11);
  int64_t size2;
  TaskSpec *spec2 = TaskSpec_finish_construct(builder, &size2);

  /* Check that these tasks have the same task IDs and the same return IDs. */
  ASSERT(TaskID_equal(TaskSpec_task_id(spec1), TaskSpec_task_id(spec2)));
  ASSERT(TaskSpec_return(spec1, 0) == TaskSpec_return(spec2, 0));
  ASSERT(TaskSpec_return(spec1, 1) == TaskSpec_return(spec2, 1));
  ASSERT(TaskSpec_return(spec1, 2) == TaskSpec_return(spec2, 2));
  /* Check that the return IDs are all distinct. */
  ASSERT(!(TaskSpec_return(spec1, 0) == TaskSpec_return(spec2, 1)));
  ASSERT(!(TaskSpec_return(spec1, 0) == TaskSpec_return(spec2, 2)));
  ASSERT(!(TaskSpec_return(spec1, 1) == TaskSpec_return(spec2, 2)));

  /* Create more tasks that are only mildly different. */

  /* Construct a task with a different parent task ID. */
  TaskSpec_start_construct(builder, DriverID::nil(), TaskID::from_random(), 0,
                           ActorID::nil(), ObjectID::nil(), ActorID::nil(),
                           ActorID::nil(), 0, false, func_id, 3);
  TaskSpec_args_add_ref(builder, &arg1, 1);
  TaskSpec_args_add_val(builder, arg2, 11);
  int64_t size3;
  TaskSpec *spec3 = TaskSpec_finish_construct(builder, &size3);

  /* Construct a task with a different parent counter. */
  TaskSpec_start_construct(builder, DriverID::nil(), parent_task_id, 1,
                           ActorID::nil(), ObjectID::nil(), ActorID::nil(),
                           ActorID::nil(), 0, false, func_id, 3);
  TaskSpec_args_add_ref(builder, &arg1, 1);
  TaskSpec_args_add_val(builder, arg2, 11);
  int64_t size4;
  TaskSpec *spec4 = TaskSpec_finish_construct(builder, &size4);

  /* Construct a task with a different function ID. */
  TaskSpec_start_construct(builder, DriverID::nil(), parent_task_id, 0,
                           ActorID::nil(), ObjectID::nil(), ActorID::nil(),
                           ActorID::nil(), 0, false, FunctionID::from_random(),
                           3);
  TaskSpec_args_add_ref(builder, &arg1, 1);
  TaskSpec_args_add_val(builder, arg2, 11);
  int64_t size5;
  TaskSpec *spec5 = TaskSpec_finish_construct(builder, &size5);

  /* Construct a task with a different object ID argument. */
  TaskSpec_start_construct(builder, DriverID::nil(), parent_task_id, 0,
                           ActorID::nil(), ObjectID::nil(), ActorID::nil(),
                           ActorID::nil(), 0, false, func_id, 3);
  ObjectID object_id = ObjectID::from_random();
  TaskSpec_args_add_ref(builder, &object_id, 1);
  TaskSpec_args_add_val(builder, arg2, 11);
  int64_t size6;
  TaskSpec *spec6 = TaskSpec_finish_construct(builder, &size6);

  /* Construct a task with a different value argument. */
  TaskSpec_start_construct(builder, DriverID::nil(), parent_task_id, 0,
                           ActorID::nil(), ObjectID::nil(), ActorID::nil(),
                           ActorID::nil(), 0, false, func_id, 3);
  TaskSpec_args_add_ref(builder, &arg1, 1);
  TaskSpec_args_add_val(builder, (uint8_t *) "hello_world", 11);
  int64_t size7;
  TaskSpec *spec7 = TaskSpec_finish_construct(builder, &size7);

  /* Check that the task IDs are all distinct from the original. */
  ASSERT(!TaskID_equal(TaskSpec_task_id(spec1), TaskSpec_task_id(spec3)));
  ASSERT(!TaskID_equal(TaskSpec_task_id(spec1), TaskSpec_task_id(spec4)));
  ASSERT(!TaskID_equal(TaskSpec_task_id(spec1), TaskSpec_task_id(spec5)));
  ASSERT(!TaskID_equal(TaskSpec_task_id(spec1), TaskSpec_task_id(spec6)));
  ASSERT(!TaskID_equal(TaskSpec_task_id(spec1), TaskSpec_task_id(spec7)));

  /* Check that the return object IDs are distinct from the originals. */
  TaskSpec *specs[6] = {spec1, spec3, spec4, spec5, spec6, spec7};
  for (int task_index1 = 0; task_index1 < 6; ++task_index1) {
    for (int return_index1 = 0; return_index1 < 3; ++return_index1) {
      for (int task_index2 = 0; task_index2 < 6; ++task_index2) {
        for (int return_index2 = 0; return_index2 < 3; ++return_index2) {
          if (task_index1 != task_index2 && return_index1 != return_index2) {
            ASSERT(!(TaskSpec_return(specs[task_index1], return_index1) ==
                     TaskSpec_return(specs[task_index2], return_index2)));
          }
        }
      }
    }
  }

  TaskSpec_free(spec1);
  TaskSpec_free(spec2);
  TaskSpec_free(spec3);
  TaskSpec_free(spec4);
  TaskSpec_free(spec5);
  TaskSpec_free(spec6);
  TaskSpec_free(spec7);
  free_task_builder(builder);
  PASS();
}

TEST send_task(void) {
  TaskBuilder *builder = make_task_builder();
  TaskID parent_task_id = TaskID::from_random();
  FunctionID func_id = FunctionID::from_random();
  TaskSpec_start_construct(builder, DriverID::nil(), parent_task_id, 0,
                           ActorID::nil(), ObjectID::nil(), ActorID::nil(),
                           ActorID::nil(), 0, false, func_id, 2);
  ObjectID object_id = ObjectID::from_random();
  TaskSpec_args_add_ref(builder, &object_id, 1);
  TaskSpec_args_add_val(builder, (uint8_t *) "Hello", 5);
  TaskSpec_args_add_val(builder, (uint8_t *) "World", 5);
  object_id = ObjectID::from_random();
  TaskSpec_args_add_ref(builder, &object_id, 1);
  int64_t size;
  TaskSpec *spec = TaskSpec_finish_construct(builder, &size);
  int fd[2];
  socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
  write_message(fd[0], static_cast<int64_t>(CommonMessageType::SUBMIT_TASK),
                size, (uint8_t *) spec);
  int64_t type;
  int64_t length;
  uint8_t *message;
  read_message(fd[1], &type, &length, &message);
  TaskSpec *result = (TaskSpec *) message;
  ASSERT(static_cast<CommonMessageType>(type) ==
         CommonMessageType::SUBMIT_TASK);
  ASSERT(memcmp(spec, result, size) == 0);
  TaskSpec_free(spec);
  free(result);
  free_task_builder(builder);
  PASS();
}

SUITE(task_tests) {
  RUN_TEST(task_test);
  RUN_TEST(deterministic_ids_test);
  RUN_TEST(send_task);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(task_tests);
  GREATEST_MAIN_END();
}
