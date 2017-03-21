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

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

set(ARROW_DIR "${CMAKE_CURRENT_LIST_DIR}/thirdparty/arrow/"
    CACHE STRING "Path of the arrow source directory")

set(ARROW_LIB "${CMAKE_CURRENT_LIST_DIR}/thirdparty/arrow/cpp/build/release/libarrow.a"
    CACHE STRING "Path to libarrow.a (needs to be changed if arrow is build in debug mode)")
set(ARROW_IO_LIB "${CMAKE_CURRENT_LIST_DIR}/thirdparty/arrow/cpp/build/release/libarrow_io.a"
    CACHE STRING "Path to libarrow_io.a (needs to be changed if arrow is build in debug mode)")
set(ARROW_IPC_LIB "${CMAKE_CURRENT_LIST_DIR}/thirdparty/arrow/cpp/build/release/libarrow_ipc.a"
    CACHE STRING "Path to libarrow_ipc.a (needs to be changed if arrow is build in debug mode)")

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
  cpp/src/numbuf/tensor.cc
  cpp/src/numbuf/dict.cc
  cpp/src/numbuf/sequence.cc
  python/src/pynumbuf/numbuf.cc
  python/src/pynumbuf/adapters/numpy.cc
  python/src/pynumbuf/adapters/python.cc
  ${COMMON_EXTENSION})

get_filename_component(PYTHON_SHARED_LIBRARY ${PYTHON_LIBRARIES} NAME)
if(APPLE)
  add_custom_command(TARGET numbuf
      POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change ${PYTHON_SHARED_LIBRARY} ${PYTHON_LIBRARIES} libnumbuf.so)
endif(APPLE)

if(APPLE)
  target_link_libraries(numbuf ${ARROW_LIB} ${ARROW_IO_LIB} ${ARROW_IPC_LIB} ${PYTHON_LIBRARIES} -lpthread)
else()
  target_link_libraries(numbuf -Wl,--whole-archive ${ARROW_LIB} -Wl,--no-whole-archive ${ARROW_IO_LIB} ${ARROW_IPC_LIB} ${PYTHON_LIBRARIES} -lpthread)
endif()

if(HAS_PLASMA)
  target_link_libraries(numbuf ${ARROW_LIB} ${ARROW_IO_LIB} ${ARROW_IPC_LIB} ${PYTHON_LIBRARIES} plasma_lib common ${FLATBUFFERS_STATIC_LIB} -lpthread)
else()
  target_link_libraries(numbuf ${ARROW_LIB} ${ARROW_IO_LIB} ${ARROW_IPC_LIB} ${PYTHON_LIBRARIES} -lpthread)
endif()

install(TARGETS numbuf DESTINATION ${CMAKE_SOURCE_DIR}/numbuf/)
