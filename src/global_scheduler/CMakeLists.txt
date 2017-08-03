cmake_minimum_required(VERSION 3.4)

project(global_scheduler)

include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

add_executable(global_scheduler global_scheduler.cc global_scheduler_algorithm.cc)
target_link_libraries(global_scheduler common ${HIREDIS_LIB})
