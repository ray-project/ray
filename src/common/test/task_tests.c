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
  task_spec *task =
      start_construct_task_spec(parent_task_id, 0, func_id, 4, 2, 10);
  ASSERT(task_num_args(task) == 4);
  ASSERT(task_num_returns(task) == 2);

  unique_id arg1 = globally_unique_id();
  ASSERT(task_args_add_ref(task, arg1) == 0);
  ASSERT(task_args_add_val(task, (uint8_t *) "hello", 5) == 1);
  unique_id arg2 = globally_unique_id();
  ASSERT(task_args_add_ref(task, arg2) == 2);
  ASSERT(task_args_add_val(task, (uint8_t *) "world", 5) == 3);
  /* Finish constructing the task. This constructs the task ID and the task
   * return IDs. */
  finish_construct_task_spec(task);

  /* Check that the task was constructed as expected. */
  ASSERT(task_num_args(task) == 4);
  ASSERT(task_num_returns(task) == 2);
  ASSERT(function_ids_equal(task_function(task), func_id));
  ASSERT(object_ids_equal(task_arg_id(task, 0), arg1));
  ASSERT(memcmp(task_arg_val(task, 1), (uint8_t *) "hello",
                task_arg_length(task, 1)) == 0);
  ASSERT(object_ids_equal(task_arg_id(task, 2), arg2));
  ASSERT(memcmp(task_arg_val(task, 3), (uint8_t *) "world",
                task_arg_length(task, 3)) == 0);

  free_task_spec(task);
  PASS();
}

TEST deterministic_ids_test(void) {
  /* Define the inputs to the task construction. */
  task_id parent_task_id = globally_unique_id();
  function_id func_id = globally_unique_id();
  unique_id arg1 = globally_unique_id();
  uint8_t *arg2 = (uint8_t *) "hello world";

  /* Construct a first task. */
  task_spec *task1 =
      start_construct_task_spec(parent_task_id, 0, func_id, 2, 3, 11);
  task_args_add_ref(task1, arg1);
  task_args_add_val(task1, arg2, 11);
  finish_construct_task_spec(task1);

  /* Construct a second identical task. */
  task_spec *task2 =
      start_construct_task_spec(parent_task_id, 0, func_id, 2, 3, 11);
  task_args_add_ref(task2, arg1);
  task_args_add_val(task2, arg2, 11);
  finish_construct_task_spec(task2);

  /* Check that these tasks have the same task IDs and the same return IDs.*/
  ASSERT(task_ids_equal(task_task_id(task1), task_task_id(task2)));
  ASSERT(object_ids_equal(task_return(task1, 0), task_return(task2, 0)));
  ASSERT(object_ids_equal(task_return(task1, 1), task_return(task2, 1)));
  ASSERT(object_ids_equal(task_return(task1, 2), task_return(task2, 2)));
  /* Check that the return IDs are all distinct. */
  ASSERT(!object_ids_equal(task_return(task1, 0), task_return(task2, 1)));
  ASSERT(!object_ids_equal(task_return(task1, 0), task_return(task2, 2)));
  ASSERT(!object_ids_equal(task_return(task1, 1), task_return(task2, 2)));

  /* Create more tasks that are only mildly different. */

  /* Construct a task with a different parent task ID. */
  task_spec *task3 =
      start_construct_task_spec(globally_unique_id(), 0, func_id, 2, 3, 11);
  task_args_add_ref(task3, arg1);
  task_args_add_val(task3, arg2, 11);
  finish_construct_task_spec(task3);

  /* Construct a task with a different parent counter. */
  task_spec *task4 =
      start_construct_task_spec(parent_task_id, 1, func_id, 2, 3, 11);
  task_args_add_ref(task4, arg1);
  task_args_add_val(task4, arg2, 11);
  finish_construct_task_spec(task4);

  /* Construct a task with a different function ID. */
  task_spec *task5 = start_construct_task_spec(parent_task_id, 0,
                                               globally_unique_id(), 2, 3, 11);
  task_args_add_ref(task5, arg1);
  task_args_add_val(task5, arg2, 11);
  finish_construct_task_spec(task5);

  /* Construct a task with a different object ID argument. */
  task_spec *task6 =
      start_construct_task_spec(parent_task_id, 0, func_id, 2, 3, 11);
  task_args_add_ref(task6, globally_unique_id());
  task_args_add_val(task6, arg2, 11);
  finish_construct_task_spec(task6);

  /* Construct a task with a different value argument. */
  task_spec *task7 =
      start_construct_task_spec(parent_task_id, 0, func_id, 2, 3, 11);
  task_args_add_ref(task7, arg1);
  task_args_add_val(task7, (uint8_t *) "hello_world", 11);
  finish_construct_task_spec(task7);

  /* Check that the task IDs are all distinct from the original. */
  ASSERT(!task_ids_equal(task_task_id(task1), task_task_id(task3)));
  ASSERT(!task_ids_equal(task_task_id(task1), task_task_id(task4)));
  ASSERT(!task_ids_equal(task_task_id(task1), task_task_id(task5)));
  ASSERT(!task_ids_equal(task_task_id(task1), task_task_id(task6)));
  ASSERT(!task_ids_equal(task_task_id(task1), task_task_id(task7)));

  /* Check that the return object IDs are distinct from the originals. */
  task_spec *tasks[6] = {task1, task3, task4, task5, task6, task7};
  for (int task_index1 = 0; task_index1 < 6; ++task_index1) {
    for (int return_index1 = 0; return_index1 < 3; ++return_index1) {
      for (int task_index2 = 0; task_index2 < 6; ++task_index2) {
        for (int return_index2 = 0; return_index2 < 3; ++return_index2) {
          if (task_index1 != task_index2 && return_index1 != return_index2) {
            ASSERT(!object_ids_equal(
                task_return(tasks[task_index1], return_index1),
                task_return(tasks[task_index2], return_index2)));
          }
        }
      }
    }
  }

  free_task_spec(task1);
  free_task_spec(task2);
  free_task_spec(task3);
  free_task_spec(task4);
  free_task_spec(task5);
  free_task_spec(task6);
  free_task_spec(task7);
  PASS();
}

TEST send_task(void) {
  task_id parent_task_id = globally_unique_id();
  function_id func_id = globally_unique_id();
  task_spec *task =
      start_construct_task_spec(parent_task_id, 0, func_id, 4, 2, 10);
  task_args_add_ref(task, globally_unique_id());
  task_args_add_val(task, (uint8_t *) "Hello", 5);
  task_args_add_val(task, (uint8_t *) "World", 5);
  task_args_add_ref(task, globally_unique_id());
  finish_construct_task_spec(task);
  int fd[2];
  socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
  write_message(fd[0], SUBMIT_TASK, task_size(task), (uint8_t *) task);
  int64_t type;
  int64_t length;
  uint8_t *message;
  read_message(fd[1], &type, &length, &message);
  task_spec *result = (task_spec *) message;
  ASSERT(type == SUBMIT_TASK);
  ASSERT(memcmp(task, result, task_size(task)) == 0);
  ASSERT(memcmp(task, result, task_size(result)) == 0);
  free(task);
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
