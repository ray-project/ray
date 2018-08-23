cmake_minimum_required(VERSION 3.4)

project(plasma)

# Recursively include common
include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --std=c99 -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++11 -O3 -Werror -Wall")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

include_directories("${ARROW_DIR}/cpp/src/")
# include_directories("${CMAKE_CURRENT_LIST_DIR}/../")

set(PLASMA_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/plasma.fbs" "${CMAKE_CURRENT_LIST_DIR}/format/common.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

set(PLASMA_FBS_OUTPUT_FILES
  "${OUTPUT_DIR}/plasma_generated.h"
  "${OUTPUT_DIR}/common_generated.h")

add_custom_target(gen_plasma_fbs DEPENDS ${PLASMA_FBS_OUTPUT_FILES})
add_dependencies(gen_plasma_fbs flatbuffers_ep)

# Copy the fbs files from Arrow project to local directory.
add_custom_command(
  OUTPUT ${PLASMA_FBS_SRC}
  COMMAND mkdir -p ${CMAKE_CURRENT_LIST_DIR}/format/
  COMMAND cp ${CMAKE_CURRENT_LIST_DIR}/../../thirdparty/build/arrow/cpp/src/plasma/format/plasma.fbs ${CMAKE_CURRENT_LIST_DIR}/format/
  COMMAND cp ${CMAKE_CURRENT_LIST_DIR}/../../thirdparty/build/arrow/cpp/src/plasma/format/common.fbs ${CMAKE_CURRENT_LIST_DIR}/format/
  COMMENT "Copying ${PLASMA_FBS_SRC} to local"
  VERBATIM)

# Compile flatbuffers
add_custom_command(
  OUTPUT ${PLASMA_FBS_OUTPUT_FILES}
  # The --gen-object-api flag generates a C++ class MessageT for each
  # flatbuffers message Message, which can be used to store deserialized
  # messages in data structures. This is currently used for ObjectInfo for
  # example.
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${PLASMA_FBS_SRC} --gen-object-api --scoped-enums
  DEPENDS ${PLASMA_FBS_SRC}
  COMMENT "Running flatc compiler on ${PLASMA_FBS_SRC}"
  VERBATIM)

include_directories("${FLATBUFFERS_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

add_executable(plasma_manager
  plasma_manager.cc)
add_dependencies(plasma_manager gen_plasma_fbs)

target_link_libraries(plasma_manager common ${PLASMA_STATIC_LIB} ray_static ${ARROW_STATIC_LIB} -lpthread ${Boost_SYSTEM_LIBRARY})

define_test(client_tests "")
define_test(manager_tests "" plasma_manager.cc)
target_link_libraries(manager_tests ${Boost_SYSTEM_LIBRARY})
add_dependencies(manager_tests gen_plasma_fbs)
