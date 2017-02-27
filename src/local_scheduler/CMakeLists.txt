cmake_minimum_required(VERSION 2.8)

project(local_scheduler)

# Recursively include common
include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

include_directories("${PYTHON_INCLUDE_DIRS}")

# set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} --std=c99 -Werror")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

include_directories("${CMAKE_CURRENT_LIST_DIR}/")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../plasma/")

add_library(local_scheduler_library SHARED
  local_scheduler_extension.c
  ../common/lib/python/common_extension.c)

get_filename_component(PYTHON_SHARED_LIBRARY ${PYTHON_LIBRARIES} NAME)
if(APPLE)
  add_custom_command(TARGET local_scheduler_library
                     POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change ${PYTHON_SHARED_LIBRARY} ${PYTHON_LIBRARIES} liblocal_scheduler_library.so)
endif(APPLE)

add_library(local_scheduler_client STATIC local_scheduler_client.c)

target_link_libraries(local_scheduler_library local_scheduler_client ${COMMON_LIB} ${PYTHON_LIBRARIES})

add_executable(local_scheduler local_scheduler.c local_scheduler_algorithm.c)
target_link_libraries(local_scheduler local_scheduler_client common ${HIREDIS_LIB} plasma_lib)

add_executable(local_scheduler_tests test/local_scheduler_tests.c local_scheduler.c local_scheduler_algorithm.c )
target_link_libraries(local_scheduler_tests local_scheduler_client common ${HIREDIS_LIB} plasma_lib)
target_compile_options(local_scheduler_tests PUBLIC "-DLOCAL_SCHEDULER_TEST")

install(TARGETS local_scheduler_library DESTINATION ${CMAKE_SOURCE_DIR}/local_scheduler)
