cmake_minimum_required(VERSION 2.8)

project(plasma)

# Recursively include common
include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

include_directories("${PYTHON_INCLUDE_DIRS}" thirdparty)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -march=native -mtune=native -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++11 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -march=native -mtune=native -O3")

# Compile flatbuffers

set(PLASMA_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/plasma.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

set(PLASMA_FBS_OUTPUT_FILES
  "${OUTPUT_DIR}/plasma_generated.h")

add_custom_command(
  OUTPUT ${PLASMA_FBS_OUTPUT_FILES}
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${PLASMA_FBS_SRC}
  DEPENDS ${PLASMA_FBS_SRC}
  COMMENT "Running flatc compiler on ${PLASMA_FBS_SRC}"
  VERBATIM)

add_custom_target(gen_plasma_fbs DEPENDS ${PLASMA_FBS_OUTPUT_FILES})

add_dependencies(gen_plasma_fbs flatbuffers_ep)

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

include_directories("${CMAKE_CURRENT_LIST_DIR}/")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../")

add_library(plasma SHARED
  plasma.cc
  plasma_extension.cc
  plasma_protocol.cc
  plasma_client.cc
  thirdparty/xxhash.c
  fling.c)

add_dependencies(plasma gen_plasma_fbs)

if(APPLE)
  target_link_libraries(plasma plasma_lib "-undefined dynamic_lookup" -Wl,-force_load,${FLATBUFFERS_STATIC_LIB} ${PYTHON_LIBRARIES} ${FLATBUFFERS_STATIC_LIB} -lpthread)
else(APPLE)
  target_link_libraries(plasma plasma_lib -Wl,--whole-archive ${FLATBUFFERS_STATIC_LIB} -Wl,--no-whole-archive ${PYTHON_LIBRARIES} ${FLATBUFFERS_STATIC_LIB} -lpthread)
endif(APPLE)

include_directories("${FLATBUFFERS_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

set_source_files_properties(thirdparty/dlmalloc.c PROPERTIES COMPILE_FLAGS -Wno-all)

add_library(plasma_lib STATIC
  plasma_client.cc
  plasma.cc
  plasma_common.cc
  plasma_io.cc
  plasma_protocol.cc
  status.cc
  fling.c
  thirdparty/xxhash.c)

target_link_libraries(plasma_lib ${FLATBUFFERS_STATIC_LIB} -lpthread)
add_dependencies(plasma_lib gen_plasma_fbs)

add_executable(plasma_store
  plasma_store.cc
  thirdparty/ae/ae.c
  plasma.cc
  plasma_events.cc
  plasma_protocol.cc
  eviction_policy.cc
  fling.c
  malloc.cc)

add_dependencies(plasma_store hiredis gen_plasma_fbs)

target_link_libraries(plasma_store plasma_lib ${FLATBUFFERS_STATIC_LIB})

add_dependencies(plasma protocol_fbs)

 add_executable(plasma_manager
   plasma_manager.cc)

target_link_libraries(plasma_manager common plasma_lib ${FLATBUFFERS_STATIC_LIB})

add_library(plasma_client SHARED plasma_client.cc)
target_link_libraries(plasma_client ${FLATBUFFERS_STATIC_LIB})

target_link_libraries(plasma_client plasma_lib ${FLATBUFFERS_STATIC_LIB})

define_test(client_tests plasma_lib)
# define_test(manager_tests plasma_lib plasma_manager.cc)
define_test(serialization_tests plasma_lib)
