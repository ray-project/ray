# Code for compiling flatbuffers

include(ExternalProject)
include(CMakeParseArguments)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Werror -Wall -Wno-error=unused-function -Wno-error=strict-aliasing")

# The rdynamic flag is needed to produce better backtraces on Linux.
if(UNIX AND NOT APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")
endif()

# The following is needed because in CentOS, the lib directory is named lib64
if(EXISTS "/etc/redhat-release" AND CMAKE_SIZEOF_VOID_P EQUAL 8)
  set(LIB_SUFFIX 64)
endif()

set(FLATBUFFERS_VERSION "1.9.0")

set(FLATBUFFERS_PREFIX "${CMAKE_BINARY_DIR}/flatbuffers_ep-prefix/src/flatbuffers_ep-install")
if (NOT TARGET flatbuffers_ep)
  ExternalProject_Add(flatbuffers_ep
    URL "https://github.com/google/flatbuffers/archive/v${FLATBUFFERS_VERSION}.tar.gz"
    CMAKE_ARGS
      "-DCMAKE_CXX_FLAGS=-fPIC"
      "-DCMAKE_INSTALL_PREFIX:PATH=${FLATBUFFERS_PREFIX}"
      "-DFLATBUFFERS_BUILD_TESTS=OFF"
      "-DCMAKE_BUILD_TYPE=RELEASE")
endif()

set(FBS_DEPENDS flatbuffers_ep)

set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_PREFIX}/include")
set(FLATBUFFERS_STATIC_LIB "${FLATBUFFERS_PREFIX}/lib${LIB_SUFFIX}/libflatbuffers.a")
set(FLATBUFFERS_COMPILER "${FLATBUFFERS_PREFIX}/bin/flatc")

message(STATUS "Flatbuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")
message(STATUS "Flatbuffers static library: ${FLATBUFFERS_STATIC_LIB}")
message(STATUS "Flatbuffers compiler: ${FLATBUFFERS_COMPILER}")
include_directories(SYSTEM ${FLATBUFFERS_INCLUDE_DIR})

# Custom CFLAGS

set(CMAKE_C_FLAGS "-g -Wall -Wextra -Werror=implicit-function-declaration -Wno-sign-compare -Wno-unused-parameter -Wno-type-limits -Wno-missing-field-initializers --std=c99 -fPIC -std=c99")

# language-specific
if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
  # Code for finding Python
  find_package(PythonInterp REQUIRED)
  find_package(NumPy REQUIRED)

  # Now find the Python include directories.
  execute_process(COMMAND ${PYTHON_EXECUTABLE} -c "from distutils.sysconfig import *; print(get_python_inc())"
                  OUTPUT_VARIABLE PYTHON_INCLUDE_DIRS OUTPUT_STRIP_TRAILING_WHITESPACE)
  message(STATUS "PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})

  message(STATUS "Using PYTHON_EXECUTABLE: " ${PYTHON_EXECUTABLE})
  message(STATUS "Using PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})
endif ()

if ("${CMAKE_RAY_LANG_JAVA}" STREQUAL "YES")
  find_package(JNI REQUIRED)
  # add jni support
  include_directories(${JAVA_INCLUDE_PATH})
  include_directories(${JAVA_INCLUDE_PATH2})
  if (JNI_FOUND)
    message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
    message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
  else()
    message (WARNING "NOT FIND JNI")   
  endif()
endif()

# Common libraries

set(COMMON_LIB "${CMAKE_BINARY_DIR}/src/common/libcommon.a"
    CACHE STRING "Path to libcommon.a")

include_directories("${CMAKE_CURRENT_LIST_DIR}/..")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../thirdparty/")
if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../lib/python")
endif ()

