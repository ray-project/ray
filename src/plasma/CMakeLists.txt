cmake_minimum_required(VERSION 2.8)

project(plasma)

# Recursively include common
include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

include_directories("${PYTHON_INCLUDE_DIRS}" thirdparty)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++11 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -O3")

# Compile flatbuffers

set(PLASMA_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/plasma.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

set(PLASMA_FBS_OUTPUT_FILES
  "${OUTPUT_DIR}/plasma_generated.h")

add_custom_target(gen_plasma_fbs DEPENDS ${PLASMA_FBS_OUTPUT_FILES})

add_dependencies(gen_plasma_fbs flatbuffers_ep)

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

include_directories("${ARROW_DIR}/cpp/src/")
# include_directories("${CMAKE_CURRENT_LIST_DIR}/../")

include_directories("${FLATBUFFERS_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

add_executable(plasma_manager
  plasma_manager.cc)

target_link_libraries(plasma_manager common ${ARROW_DIR}/cpp/build/release/libplasma.a ${ARROW_DIR}/cpp/build/release/libarrow.a -lpthread)
