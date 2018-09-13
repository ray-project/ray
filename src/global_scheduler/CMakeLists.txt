cmake_minimum_required(VERSION 3.4)

project(global_scheduler)

include_directories(${CMAKE_CURRENT_LIST_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall")

add_executable(global_scheduler global_scheduler.cc global_scheduler_algorithm.cc)

# Make sure ${HIREDIS_LIB} is ready before linking.
add_dependencies(global_scheduler hiredis common)

target_link_libraries(global_scheduler common ${HIREDIS_LIB} ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} ${Boost_SYSTEM_LIBRARY} pthread)
