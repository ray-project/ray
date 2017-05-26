cmake_minimum_required(VERSION 2.8)

project(numbuf)

include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

option(HAS_PLASMA
  "Are we linking with the plasma object store? Recommended if numbuf is used as part of ray."
  ON)

if(HAS_PLASMA)
  add_definitions(-DHAS_PLASMA)
endif()

find_package(NumPy REQUIRED)

if(APPLE)
  set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

include_directories("${PYTHON_INCLUDE_DIRS}")
include_directories("${NUMPY_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -march=native -mtune=native -O3")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

set(ARROW_DIR "${CMAKE_CURRENT_LIST_DIR}/thirdparty/arrow/"
    CACHE STRING "Path of the arrow source directory")

set(ARROW_LIB "${CMAKE_CURRENT_LIST_DIR}/thirdparty/arrow/cpp/build/release/libarrow.a"
    CACHE STRING "Path to libarrow.a (needs to be changed if arrow is build in debug mode)")
set(ARROW_PYTHON_LIB "${CMAKE_CURRENT_LIST_DIR}/thirdparty/arrow/cpp/build/release/libarrow_python.a"
    CACHE STRING "Path to libarrow_python.a (needs to be changed if arrow is build in debug mode)")

include_directories("${ARROW_DIR}/cpp/src/")
include_directories("cpp/src/")
include_directories("python/src/")

if(HAS_PLASMA)
  include_directories("${CMAKE_CURRENT_LIST_DIR}/../plasma")
  include_directories("${CMAKE_CURRENT_LIST_DIR}/../common")
  include_directories("${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty")
  set(COMMON_EXTENSION ../common/lib/python/common_extension.cc)
endif()

add_definitions(-fPIC)

add_library(numbuf SHARED
  cpp/src/numbuf/dict.cc
  cpp/src/numbuf/sequence.cc
  python/src/pynumbuf/numbuf.cc
  python/src/pynumbuf/adapters/numpy.cc
  python/src/pynumbuf/adapters/python.cc
  ${COMMON_EXTENSION})


if(APPLE)
  target_link_libraries(numbuf "-undefined dynamic_lookup" ${ARROW_LIB} ${ARROW_PYTHON_LIB} -lpthread)
else()
  target_link_libraries(numbuf -Wl,--whole-archive ${ARROW_LIB} -Wl,--no-whole-archive ${ARROW_PYTHON_LIB} -lpthread)
endif()

if(HAS_PLASMA)
  target_link_libraries(numbuf plasma_lib common)
endif()

install(TARGETS numbuf DESTINATION ${CMAKE_SOURCE_DIR}/numbuf/)
