cmake_minimum_required(VERSION 2.8)

project(local_scheduler)

# Recursively include common
include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

add_definitions(-fPIC)

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
include_directories("${CMAKE_CURRENT_LIST_DIR}/../common/format/")

# Compile flatbuffers

set(LOCAL_SCHEDULER_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/local_scheduler.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

add_custom_target(gen_local_scheduler_fbs ALL)

add_custom_command(
  TARGET gen_local_scheduler_fbs
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${LOCAL_SCHEDULER_FBS_SRC}
  DEPENDS ${FBS_DEPENDS}
  COMMENT "Running flatc compiler on ${LOCAL_SCHEDULER_FBS_SRC}"
  VERBATIM)

add_dependencies(gen_local_scheduler_fbs flatbuffers_ep)

add_library(local_scheduler_library SHARED
  local_scheduler_extension.cc
  ../common/lib/python/common_extension.cc)

get_filename_component(PYTHON_SHARED_LIBRARY ${PYTHON_LIBRARIES} NAME)
if(APPLE)
  add_custom_command(TARGET local_scheduler_library
                     POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change ${PYTHON_SHARED_LIBRARY} ${PYTHON_LIBRARIES} liblocal_scheduler_library.so)
endif(APPLE)

add_library(local_scheduler_client STATIC local_scheduler_client.cc)

add_dependencies(local_scheduler_client gen_local_scheduler_fbs)

target_link_libraries(local_scheduler_library local_scheduler_client common ${PYTHON_LIBRARIES})

add_dependencies(local_scheduler_library gen_local_scheduler_fbs)

add_executable(local_scheduler local_scheduler.cc local_scheduler_algorithm.cc)
target_link_libraries(local_scheduler local_scheduler_client common ${HIREDIS_LIB} plasma_lib)

# add_executable(local_scheduler_tests test/local_scheduler_tests.cc local_scheduler.cc local_scheduler_algorithm.cc)
# target_link_libraries(local_scheduler_tests local_scheduler_client common ${HIREDIS_LIB} plasma_lib)
# target_compile_options(local_scheduler_tests PUBLIC "-DLOCAL_SCHEDULER_TEST")

install(TARGETS local_scheduler_library DESTINATION ${CMAKE_SOURCE_DIR}/local_scheduler)
