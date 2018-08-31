cmake_minimum_required(VERSION 3.4)

project(global_scheduler)

set(global_scheduler_header_files global_scheduler.h global_scheduler_algorithm.h)
set(global_scheduler_source_files global_scheduler_algorithm.cc global_scheduler.cc)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall")

add_executable(global_scheduler ${global_scheduler_header_files} ${global_scheduler_source_files})

# Make sure ${HIREDIS_LIB} is ready before linking.
add_dependencies(global_scheduler hiredis)

target_link_libraries(global_scheduler common ${HIREDIS_LIB} ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} ${Boost_SYSTEM_LIBRARY} pthread)
