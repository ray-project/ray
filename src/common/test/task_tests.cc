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
  TaskID parent_task_id = globally_unique_id();
  FunctionID func_id = globally_unique_id();
  TaskBuilder *builder = make_task_builder();
  start_construct_task_spec(
      builder, NIL_ID, parent_task_id, 0, NIL_ACTOR_ID, 0, func_id, 2);
  ASSERT(task_num_args(spec) == 4);
  ASSERT(task_num_returns(spec) == 2);

  UniqueID arg1 = globally_unique_id();
  task_args_add_ref(builder, arg1);
  task_args_add_val(builder, (uint8_t *) "hello", 5);
  UniqueID arg2 = globally_unique_id();
  task_args_add_ref(builder, arg2);
  task_args_add_val(builder, (uint8_t *) "world", 5);
  /* Finish constructing the spec. This constructs the task ID and the
   * return IDs. */
  int64_t size;
  uint8_t *spec = finish_construct_task_spec(builder, &size);

  /* Check that the spec was constructed as expected. */
  ASSERT(task_num_args(spec) == 4);
  ASSERT(task_num_returns(spec) == 2);
  ASSERT(FunctionID_equal(task_function(spec), func_id));
  ASSERT(ObjectID_equal(task_arg_id(spec, 0), arg1));
  ASSERT(memcmp(task_arg_val(spec, 1), (uint8_t *) "hello",
                task_arg_length(spec, 1)) == 0);
  ASSERT(ObjectID_equal(task_arg_id(spec, 2), arg2));
  ASSERT(memcmp(task_arg_val(spec, 3), (uint8_t *) "world",
                task_arg_length(spec, 3)) == 0);

  free(spec);
  PASS();
}

TEST deterministic_ids_test(void) {
  TaskBuilder *builder = make_task_builder();
  /* Define the inputs to the task construction. */
  TaskID parent_task_id = globally_unique_id();
  FunctionID func_id = globally_unique_id();
  UniqueID arg1 = globally_unique_id();
  uint8_t *arg2 = (uint8_t *) "hello world";

  /* Construct a first task. */
  start_construct_task_spec(
      builder, NIL_ID, parent_task_id, 0, NIL_ACTOR_ID, 0, func_id, 3);
  task_args_add_ref(builder, arg1);
  task_args_add_val(builder, arg2, 11);
  int64_t size1;
  task_spec *spec1 = finish_construct_task_spec(builder, &size1);

  /* Construct a second identical task. */
  start_construct_task_spec(
      builder, NIL_ID, parent_task_id, 0, NIL_ACTOR_ID, 0, func_id, 3);
  task_args_add_ref(builder, arg1);
  task_args_add_val(builder, arg2, 11);
  int64_t size2;
  task_spec *spec2 = finish_construct_task_spec(builder, &size2);

  /* Check that these tasks have the same task IDs and the same return IDs.*/
  ASSERT(TaskID_equal(task_spec_id(spec1), task_spec_id(spec2)));
  ASSERT(ObjectID_equal(task_return(spec1, 0), task_return(spec2, 0)));
  ASSERT(ObjectID_equal(task_return(spec1, 1), task_return(spec2, 1)));
  ASSERT(ObjectID_equal(task_return(spec1, 2), task_return(spec2, 2)));
  /* Check that the return IDs are all distinct. */
  ASSERT(!ObjectID_equal(task_return(spec1, 0), task_return(spec2, 1)));
  ASSERT(!ObjectID_equal(task_return(spec1, 0), task_return(spec2, 2)));
  ASSERT(!ObjectID_equal(task_return(spec1, 1), task_return(spec2, 2)));

  /* Create more tasks that are only mildly different. */

  /* Construct a task with a different parent task ID. */
  start_construct_task_spec(
      builder, NIL_ID, globally_unique_id(), 0, NIL_ACTOR_ID, 0, func_id, 3);
  task_args_add_ref(builder, arg1);
  task_args_add_val(builder, arg2, 11);
  int64_t size3;
  task_spec *spec3 = finish_construct_task_spec(builder, &size3);

  /* Construct a task with a different parent counter. */
  start_construct_task_spec(
      builder, NIL_ID, parent_task_id, 1, NIL_ACTOR_ID, 0, func_id, 3);
  task_args_add_ref(builder, arg1);
  task_args_add_val(builder, arg2, 11);
  int64_t size4;
  task_spec *spec4 = finish_construct_task_spec(builder, &size4);

  /* Construct a task with a different function ID. */
  start_construct_task_spec(builder, NIL_ID, parent_task_id, 0, NIL_ACTOR_ID, 0,
                            globally_unique_id(), 3);
  task_args_add_ref(builder, arg1);
  task_args_add_val(builder, arg2, 11);
  int64_t size5;
  task_spec *spec5 = finish_construct_task_spec(builder, &size5);

  /* Construct a task with a different object ID argument. */
  start_construct_task_spec(
      builder, NIL_ID, parent_task_id, 0, NIL_ACTOR_ID, 0, func_id, 3);
  task_args_add_ref(builder, globally_unique_id());
  task_args_add_val(builder, arg2, 11);
  int64_t size6;
  task_spec *spec6 = finish_construct_task_spec(builder, &size6);

  /* Construct a task with a different value argument. */
  start_construct_task_spec(
      builder, NIL_ID, parent_task_id, 0, NIL_ACTOR_ID, 0, func_id, 3);
  task_args_add_ref(builder, arg1);
  task_args_add_val(builder, (uint8_t *) "hello_world", 11);
  int64_t size7;
  task_spec *spec7 = finish_construct_task_spec(builder, &size7);

  /* Check that the task IDs are all distinct from the original. */
  ASSERT(!TaskID_equal(task_spec_id(spec1), task_spec_id(spec3)));
  ASSERT(!TaskID_equal(task_spec_id(spec1), task_spec_id(spec4)));
  ASSERT(!TaskID_equal(task_spec_id(spec1), task_spec_id(spec5)));
  ASSERT(!TaskID_equal(task_spec_id(spec1), task_spec_id(spec6)));
  ASSERT(!TaskID_equal(task_spec_id(spec1), task_spec_id(spec7)));

  /* Check that the return object IDs are distinct from the originals. */
  task_spec *specs[6] = {spec1, spec3, spec4, spec5, spec6, spec7};
  for (int task_index1 = 0; task_index1 < 6; ++task_index1) {
    for (int return_index1 = 0; return_index1 < 3; ++return_index1) {
      for (int task_index2 = 0; task_index2 < 6; ++task_index2) {
        for (int return_index2 = 0; return_index2 < 3; ++return_index2) {
          if (task_index1 != task_index2 && return_index1 != return_index2) {
            ASSERT(!ObjectID_equal(
                task_return(specs[task_index1], return_index1),
                task_return(specs[task_index2], return_index2)));
          }
        }
      }
    }
  }

  free(spec1);
  free(spec2);
  free(spec3);
  free(spec4);
  free(spec5);
  free(spec6);
  free(spec7);
  PASS();
}

TEST send_task(void) {
  TaskBuilder *builder = make_task_builder();
  TaskID parent_task_id = globally_unique_id();
  FunctionID func_id = globally_unique_id();
  start_construct_task_spec(
      builder, NIL_ID, parent_task_id, 0, NIL_ACTOR_ID, 0, func_id, 2);
  task_args_add_ref(builder, globally_unique_id());
  task_args_add_val(builder, (uint8_t *) "Hello", 5);
  task_args_add_val(builder, (uint8_t *) "World", 5);
  task_args_add_ref(builder, globally_unique_id());
  int64_t size;
  task_spec *spec = finish_construct_task_spec(builder, &size);
  int fd[2];
  socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
  write_message(fd[0], SUBMIT_TASK, size, (uint8_t *) spec);
  int64_t type;
  int64_t length;
  uint8_t *message;
  read_message(fd[1], &type, &length, &message);
  task_spec *result = (task_spec *) message;
  ASSERT(type == SUBMIT_TASK);
  ASSERT(memcmp(spec, result, size) == 0);
  free(spec);
  free(result);
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
