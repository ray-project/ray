cmake_minimum_required(VERSION 2.8)

project(common)

include(${CMAKE_CURRENT_LIST_DIR}/cmake/Common.cmake)

include_directories(. thirdparty thirdparty/ae "${FLATBUFFERS_INCLUDE_DIR}")

add_custom_target(
   hiredis
   COMMAND make
   WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/thirdparty/hiredis
)

add_library(common STATIC
  event_loop.c
  common.c
  task.c
  io.c
  net.c
  logging.c
  state/redis.c
  state/table.c
  state/object_table.c
  state/task_table.c
  state/db_client_table.c
  state/local_scheduler_table.c
  thirdparty/ae/ae.c
  thirdparty/sha256.c)

target_link_libraries(common "${CMAKE_CURRENT_LIST_DIR}/thirdparty/hiredis/libhiredis.a")

function(define_test test_name)
  add_executable(${test_name} test/${test_name}.c ${file_list} ${ARGN})
  add_dependencies(${test_name} hiredis)
  target_link_libraries(${test_name} common ${FLATBUFFERS_STATIC_LIB})
endfunction()

define_test(common_tests)
define_test(db_tests)
define_test(io_tests)
define_test(task_tests)
define_test(redis_tests)
define_test(task_table_tests)
define_test(object_table_tests)
