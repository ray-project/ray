cmake_minimum_required(VERSION 2.8)

project(numbuf)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

find_package(PythonInterp REQUIRED)
find_package(PythonLibs REQUIRED)
find_package(NumPy REQUIRED)

include_directories("${PYTHON_INCLUDE_DIRS}")
include_directories("${NUMPY_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

if (UNIX AND NOT APPLE)
   link_libraries(rt)
endif()

set(ARROW_DIR "${CMAKE_SOURCE_DIR}/../arrow/" CACHE STRING
  "Path of the arrow source directory")

set(ARROW_STATIC_LIB "${CMAKE_SOURCE_DIR}/../arrow/cpp/build/release/libarrow.so" CACHE STRING
  "Path to libarrow.a (needs to be changed if arrow is build in debug mode)")

include_directories("${ARROW_DIR}/cpp/src/")
include_directories("cpp/src/")
include_directories("python/src/")

add_definitions(-fPIC)

add_library(numbuf SHARED
  cpp/src/numbuf/tensor.cc
  cpp/src/numbuf/dict.cc
  cpp/src/numbuf/sequence.cc
  python/src/pynumbuf/numbuf.cc
  python/src/pynumbuf/adapters/numpy.cc
  python/src/pynumbuf/adapters/python.cc)

target_link_libraries(numbuf ${ARROW_STATIC_LIB} ${PYTHON_LIBRARIES})
