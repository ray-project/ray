# Code for compiling flatbuffers

include(ExternalProject)
include(CMakeParseArguments)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Werror -Wall -Wno-error=unused-function -Wno-error=strict-aliasing")

# The rdynamic flag is needed to produce better backtraces on Linux.
if(UNIX AND NOT APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")
endif()

set(FLATBUFFERS_VERSION "1.7.1")

set(FLATBUFFERS_PREFIX "${CMAKE_BINARY_DIR}/flatbuffers_ep-prefix/src/flatbuffers_ep-install")
if (NOT TARGET flatbuffers_ep)
  ExternalProject_Add(flatbuffers_ep
    URL "https://github.com/google/flatbuffers/archive/v${FLATBUFFERS_VERSION}.tar.gz"
    CMAKE_ARGS
      "-DCMAKE_CXX_FLAGS=-fPIC"
      "-DCMAKE_INSTALL_PREFIX:PATH=${FLATBUFFERS_PREFIX}"
      "-DFLATBUFFERS_BUILD_TESTS=OFF")
endif()

set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_PREFIX}/include")
set(FLATBUFFERS_STATIC_LIB "${FLATBUFFERS_PREFIX}/lib/libflatbuffers.a")
set(FLATBUFFERS_COMPILER "${FLATBUFFERS_PREFIX}/bin/flatc")

message(STATUS "Flatbuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")
message(STATUS "Flatbuffers static library: ${FLATBUFFERS_STATIC_LIB}")
message(STATUS "Flatbuffers compiler: ${FLATBUFFERS_COMPILER}")
include_directories(SYSTEM ${FLATBUFFERS_INCLUDE_DIR})

# Custom CFLAGS

set(CMAKE_C_FLAGS "-g -Wall -Wextra -Werror=implicit-function-declaration -Wno-sign-compare -Wno-unused-parameter -Wno-type-limits -Wno-missing-field-initializers --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -fPIC -std=c99")

# Code for finding Python
find_package(PythonInterp REQUIRED)

# Now find the Python include directories.
execute_process(COMMAND ${PYTHON_EXECUTABLE} -c "from distutils.sysconfig import *; print(get_python_inc())"
                OUTPUT_VARIABLE PYTHON_INCLUDE_DIRS OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})

message(STATUS "Using PYTHON_EXECUTABLE: " ${PYTHON_EXECUTABLE})
message(STATUS "Using PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})

# Common libraries

set(COMMON_LIB "${CMAKE_BINARY_DIR}/src/common/libcommon.a"
    CACHE STRING "Path to libcommon.a")

include_directories("${CMAKE_CURRENT_LIST_DIR}/..")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../thirdparty/")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../lib/python")
