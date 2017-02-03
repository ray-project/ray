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
  task_id parent_task_id = globally_unique_id();
  function_id func_id = globally_unique_id();
  task_spec *spec =
      start_construct_task_spec(NIL_ID, parent_task_id, 0, NIL_ID, 0, func_id, 4, 2, 10);
  ASSERT(task_num_args(spec) == 4);
  ASSERT(task_num_returns(spec) == 2);

  unique_id arg1 = globally_unique_id();
  ASSERT(task_args_add_ref(spec, arg1) == 0);
  ASSERT(task_args_add_val(spec, (uint8_t *) "hello", 5) == 1);
  unique_id arg2 = globally_unique_id();
  ASSERT(task_args_add_ref(spec, arg2) == 2);
  ASSERT(task_args_add_val(spec, (uint8_t *) "world", 5) == 3);
  /* Finish constructing the spec. This constructs the task ID and the
   * return IDs. */
  finish_construct_task_spec(spec);

  /* Check that the spec was constructed as expected. */
  ASSERT(task_num_args(spec) == 4);
  ASSERT(task_num_returns(spec) == 2);
  ASSERT(function_ids_equal(task_function(spec), func_id));
  ASSERT(object_ids_equal(task_arg_id(spec, 0), arg1));
  ASSERT(memcmp(task_arg_val(spec, 1), (uint8_t *) "hello",
                task_arg_length(spec, 1)) == 0);
  ASSERT(object_ids_equal(task_arg_id(spec, 2), arg2));
  ASSERT(memcmp(task_arg_val(spec, 3), (uint8_t *) "world",
                task_arg_length(spec, 3)) == 0);

  free_task_spec(spec);
  PASS();
}

TEST deterministic_ids_test(void) {
  /* Define the inputs to the task construction. */
  task_id parent_task_id = globally_unique_id();
  function_id func_id = globally_unique_id();
  unique_id arg1 = globally_unique_id();
  uint8_t *arg2 = (uint8_t *) "hello world";

  /* Construct a first task. */
  task_spec *spec1 =
      start_construct_task_spec(NIL_ID, parent_task_id, 0, NIL_ID, 0, func_id, 2, 3, 11);
  task_args_add_ref(spec1, arg1);
  task_args_add_val(spec1, arg2, 11);
  finish_construct_task_spec(spec1);

  /* Construct a second identical task. */
  task_spec *spec2 =
      start_construct_task_spec(NIL_ID, parent_task_id, 0, NIL_ID, 0, func_id, 2, 3, 11);
  task_args_add_ref(spec2, arg1);
  task_args_add_val(spec2, arg2, 11);
  finish_construct_task_spec(spec2);

  /* Check that these tasks have the same task IDs and the same return IDs.*/
  ASSERT(task_ids_equal(task_spec_id(spec1), task_spec_id(spec2)));
  ASSERT(object_ids_equal(task_return(spec1, 0), task_return(spec2, 0)));
  ASSERT(object_ids_equal(task_return(spec1, 1), task_return(spec2, 1)));
  ASSERT(object_ids_equal(task_return(spec1, 2), task_return(spec2, 2)));
  /* Check that the return IDs are all distinct. */
  ASSERT(!object_ids_equal(task_return(spec1, 0), task_return(spec2, 1)));
  ASSERT(!object_ids_equal(task_return(spec1, 0), task_return(spec2, 2)));
  ASSERT(!object_ids_equal(task_return(spec1, 1), task_return(spec2, 2)));

  /* Create more tasks that are only mildly different. */

  /* Construct a task with a different parent task ID. */
  task_spec *spec3 = start_construct_task_spec(NIL_ID, globally_unique_id(), 0, NIL_ID, 0,
                                               func_id, 2, 3, 11);
  task_args_add_ref(spec3, arg1);
  task_args_add_val(spec3, arg2, 11);
  finish_construct_task_spec(spec3);

  /* Construct a task with a different parent counter. */
  task_spec *spec4 =
      start_construct_task_spec(NIL_ID, parent_task_id, 1, NIL_ID, 0, func_id, 2, 3, 11);
  task_args_add_ref(spec4, arg1);
  task_args_add_val(spec4, arg2, 11);
  finish_construct_task_spec(spec4);

  /* Construct a task with a different function ID. */
  task_spec *spec5 = start_construct_task_spec(NIL_ID, parent_task_id, 0, NIL_ID, 0,
                                               globally_unique_id(), 2, 3, 11);
  task_args_add_ref(spec5, arg1);
  task_args_add_val(spec5, arg2, 11);
  finish_construct_task_spec(spec5);

  /* Construct a task with a different object ID argument. */
  task_spec *spec6 =
      start_construct_task_spec(NIL_ID, parent_task_id, 0, NIL_ID, 0, func_id, 2, 3, 11);
  task_args_add_ref(spec6, globally_unique_id());
  task_args_add_val(spec6, arg2, 11);
  finish_construct_task_spec(spec6);

  /* Construct a task with a different value argument. */
  task_spec *spec7 =
      start_construct_task_spec(NIL_ID, parent_task_id, 0, NIL_ID, 0, func_id, 2, 3, 11);
  task_args_add_ref(spec7, arg1);
  task_args_add_val(spec7, (uint8_t *) "hello_world", 11);
  finish_construct_task_spec(spec7);

  /* Check that the task IDs are all distinct from the original. */
  ASSERT(!task_ids_equal(task_spec_id(spec1), task_spec_id(spec3)));
  ASSERT(!task_ids_equal(task_spec_id(spec1), task_spec_id(spec4)));
  ASSERT(!task_ids_equal(task_spec_id(spec1), task_spec_id(spec5)));
  ASSERT(!task_ids_equal(task_spec_id(spec1), task_spec_id(spec6)));
  ASSERT(!task_ids_equal(task_spec_id(spec1), task_spec_id(spec7)));

  /* Check that the return object IDs are distinct from the originals. */
  task_spec *specs[6] = {spec1, spec3, spec4, spec5, spec6, spec7};
  for (int task_index1 = 0; task_index1 < 6; ++task_index1) {
    for (int return_index1 = 0; return_index1 < 3; ++return_index1) {
      for (int task_index2 = 0; task_index2 < 6; ++task_index2) {
        for (int return_index2 = 0; return_index2 < 3; ++return_index2) {
          if (task_index1 != task_index2 && return_index1 != return_index2) {
            ASSERT(!object_ids_equal(
                task_return(specs[task_index1], return_index1),
                task_return(specs[task_index2], return_index2)));
          }
        }
      }
    }
  }

  free_task_spec(spec1);
  free_task_spec(spec2);
  free_task_spec(spec3);
  free_task_spec(spec4);
  free_task_spec(spec5);
  free_task_spec(spec6);
  free_task_spec(spec7);
  PASS();
}

TEST send_task(void) {
  task_id parent_task_id = globally_unique_id();
  function_id func_id = globally_unique_id();
  task_spec *spec =
      start_construct_task_spec(NIL_ID, parent_task_id, 0, NIL_ID, 0, func_id, 4, 2, 10);
  task_args_add_ref(spec, globally_unique_id());
  task_args_add_val(spec, (uint8_t *) "Hello", 5);
  task_args_add_val(spec, (uint8_t *) "World", 5);
  task_args_add_ref(spec, globally_unique_id());
  finish_construct_task_spec(spec);
  int fd[2];
  socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
  write_message(fd[0], SUBMIT_TASK, task_spec_size(spec), (uint8_t *) spec);
  int64_t type;
  int64_t length;
  uint8_t *message;
  read_message(fd[1], &type, &length, &message);
  task_spec *result = (task_spec *) message;
  ASSERT(type == SUBMIT_TASK);
  ASSERT(memcmp(spec, result, task_spec_size(spec)) == 0);
  ASSERT(memcmp(spec, result, task_spec_size(result)) == 0);
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
