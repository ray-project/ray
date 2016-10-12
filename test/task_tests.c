#include "greatest.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "common.h"
#include "test/example_task.h"
#include "task.h"
#include "io.h"

SUITE(task_tests);

TEST task_test(void) {
  function_id func_id = globally_unique_id();
  task_spec *task = alloc_task_spec(func_id, 4, 2, 10);
  ASSERT(task_num_args(task) == 4);
  ASSERT(task_num_returns(task) == 2);

  unique_id arg1 = globally_unique_id();
  ASSERT(task_args_add_ref(task, arg1) == 0);
  ASSERT(task_args_add_val(task, (uint8_t *) "hello", 5) == 1);
  unique_id arg2 = globally_unique_id();
  ASSERT(task_args_add_ref(task, arg2) == 2);
  ASSERT(task_args_add_val(task, (uint8_t *) "world", 5) == 3);

  unique_id ret0 = globally_unique_id();
  unique_id ret1 = globally_unique_id();
  memcpy(task_return(task, 0), &ret0, sizeof(ret0));
  memcpy(task_return(task, 1), &ret1, sizeof(ret1));

  ASSERT(memcmp(task_arg_id(task, 0), &arg1, sizeof(arg1)) == 0);
  ASSERT(memcmp(task_arg_val(task, 1), (uint8_t *) "hello",
                task_arg_length(task, 1)) == 0);
  ASSERT(memcmp(task_arg_id(task, 2), &arg2, sizeof(arg2)) == 0);
  ASSERT(memcmp(task_arg_val(task, 3), (uint8_t *) "world",
                task_arg_length(task, 3)) == 0);

  ASSERT(memcmp(task_return(task, 0), &ret0, sizeof(unique_id)) == 0);
  ASSERT(memcmp(task_return(task, 1), &ret1, sizeof(unique_id)) == 0);

  free_task_spec(task);
  PASS();
}

TEST send_task(void) {
  function_id func_id = globally_unique_id();
  task_spec *task = alloc_task_spec(func_id, 4, 2, 10);
  *task_return(task, 1) = globally_unique_id();
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
  RUN_TEST(send_task);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(task_tests);
  GREATEST_MAIN_END();
}
