# Code for compiling flatbuffers

include(ExternalProject)
include(CMakeParseArguments)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(FLATBUFFERS_VERSION "1.6.0")

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

message(STATUS "Trying custom approach for finding Python.")
# Start off by figuring out which Python executable to use.
find_program(CUSTOM_PYTHON_EXECUTABLE python)
message(STATUS "Found Python program: ${CUSTOM_PYTHON_EXECUTABLE}")
execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c "import sys; print('python' + sys.version[0:3])"
                OUTPUT_VARIABLE PYTHON_LIBRARY_NAME OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "PYTHON_LIBRARY_NAME: " ${PYTHON_LIBRARY_NAME})
# Now find the Python include directories.
execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c "from distutils.sysconfig import *; print(get_python_inc())"
                OUTPUT_VARIABLE PYTHON_INCLUDE_DIRS OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})

# If we found the Python libraries and the include directories, then continue
# on. If not, then try find_package as a last resort, but it probably won't
# work.
if(PYTHON_INCLUDE_DIRS)
  message(STATUS "The custom approach for finding Python succeeded.")
  SET(PYTHONLIBS_FOUND TRUE)
else()
  message(WARNING "The custom approach for finding Python failed. Defaulting to find_package.")
  find_package(PythonInterp REQUIRED)
  find_package(PythonLibs ${PYTHON_VERSION_STRING} EXACT)
  set(CUSTOM_PYTHON_EXECUTABLE ${PYTHON_EXECUTABLE})
endif()

message(STATUS "Using CUSTOM_PYTHON_EXECUTABLE: " ${CUSTOM_PYTHON_EXECUTABLE})
message(STATUS "Using PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})

# Common libraries

set(COMMON_LIB "${CMAKE_BINARY_DIR}/src/common/libcommon.a"
    CACHE STRING "Path to libcommon.a")

include_directories("${CMAKE_CURRENT_LIST_DIR}/..")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../thirdparty/")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../lib/python")
