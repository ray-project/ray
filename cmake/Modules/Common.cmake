# Code for compiling flatbuffers

include(ExternalProject)
include(CMakeParseArguments)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Werror -Wall -Wno-error=unused-function -Wno-error=strict-aliasing")

# The rdynamic flag is needed to produce better backtraces on Linux.
if(UNIX AND NOT APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")
endif()

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

include_directories(${CMAKE_SOURCE_DIR}/src/common)
include_directories(${CMAKE_SOURCE_DIR}/src/common/thirdparty)
