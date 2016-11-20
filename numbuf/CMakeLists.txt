cmake_minimum_required(VERSION 2.8)

project(numbuf)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

# Make libnumbuf.so look for shared libraries in the folder libnumbuf.so is in.
set(CMAKE_INSTALL_RPATH "$ORIGIN/")
set(CMAKE_MACOSX_RPATH 1)

if(NOT APPLE)
  find_package(PythonInterp REQUIRED)
  find_package(PythonLibs ${PYTHON_VERSION_STRING} EXACT REQUIRED)
  set(CUSTOM_PYTHON_EXECUTABLE ${PYTHON_EXECUTABLE})
else()
  message(STATUS "Trying custom approach for finding Python.")
  # Start off by figuring out which Python executable to use.
  find_program(CUSTOM_PYTHON_EXECUTABLE python)
  message(STATUS "Found Python program: ${CUSTOM_PYTHON_EXECUTABLE}")
  execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c "import sys; print 'python' + sys.version[0:3]"
                  OUTPUT_VARIABLE PYTHON_LIBRARY_NAME OUTPUT_STRIP_TRAILING_WHITESPACE)
  message(STATUS "PYTHON_LIBRARY_NAME: " ${PYTHON_LIBRARY_NAME})
  # Now find the Python include directories.
  execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c "from distutils.sysconfig import *; print get_python_inc()"
                  OUTPUT_VARIABLE PYTHON_INCLUDE_DIRS OUTPUT_STRIP_TRAILING_WHITESPACE)
  message(STATUS "PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})
  # Now find the Python libraries. We'll start by looking near the Python
  # executable. If that fails, then we'll look near the Python include
  # directories.
  execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c "import sys; print sys.exec_prefix"
                  OUTPUT_VARIABLE PYTHON_PREFIX OUTPUT_STRIP_TRAILING_WHITESPACE)
  message(STATUS "PYTHON_PREFIX: " ${PYTHON_PREFIX})
  FIND_LIBRARY(PYTHON_LIBRARIES
               NAMES ${PYTHON_LIBRARY_NAME}
               HINTS "${PYTHON_PREFIX}"
               PATH_SUFFIXES "lib" "libs"
               NO_DEFAULT_PATH)
  message(STATUS "PYTHON_LIBRARIES: " ${PYTHON_LIBRARIES})
  # If that failed, perhaps because the user is in a virtualenv, search around
  # the Python include directories.
  if(NOT PYTHON_LIBRARIES)
    message(STATUS "Failed to find PYTHON_LIBRARIES near the Python executable, so now looking near the Python include directories.")
    FIND_LIBRARY(PYTHON_LIBRARIES
                 NAMES ${PYTHON_LIBRARY_NAME}
                 HINTS "${PYTHON_INCLUDE_DIRS}/../.."
                 PATH_SUFFIXES "lib" "libs"
                 NO_DEFAULT_PATH)
    message(STATUS "PYTHON_LIBRARIES: " ${PYTHON_LIBRARIES})
  endif()
  # If we found the Python libraries and the include directories, then continue
  # on. If not, then try find_package as a last resort, but it probably won't
  # work.
  if(PYTHON_LIBRARIES AND PYTHON_INCLUDE_DIRS)
    message(STATUS "The custom approach for finding Python succeeded.")
    SET(PYTHONLIBS_FOUND TRUE)
  else()
    message(WARNING "The custom approach for finding Python failed. Defaulting to find_package.")
    find_package(PythonInterp REQUIRED)
    find_package(PythonLibs ${PYTHON_VERSION_STRING} EXACT REQUIRED)
    set(CUSTOM_PYTHON_EXECUTABLE ${PYTHON_EXECUTABLE})
  endif()
endif()
message(STATUS "Using CUSTOM_PYTHON_EXECUTABLE: " ${CUSTOM_PYTHON_EXECUTABLE})
message(STATUS "Using PYTHON_LIBRARIES: " ${PYTHON_LIBRARIES})
message(STATUS "Using PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})

find_package(NumPy REQUIRED)

if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

include_directories("${PYTHON_INCLUDE_DIRS}")
include_directories("${NUMPY_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

set(ARROW_DIR "${CMAKE_SOURCE_DIR}/thirdparty/arrow/"
    CACHE STRING "Path of the arrow source directory")

set(ARROW_LIB "${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/build/release/libarrow.a"
    CACHE STRING "Path to libarrow.so (needs to be changed if arrow is build in debug mode)")
set(ARROW_IO_LIB "${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/build/release/libarrow_io.a"
    CACHE STRING "Path to libarrow_io.so (needs to be changed if arrow is build in debug mode)")
set(ARROW_IPC_LIB "${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/build/release/libarrow_ipc.a"
    CACHE STRING "Path to libarrow_ipc.so (needs to be changed if arrow is build in debug mode)")

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

get_filename_component(PYTHON_SHARED_LIBRARY ${PYTHON_LIBRARIES} NAME)
if(APPLE)
  add_custom_command(TARGET numbuf
      POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change ${PYTHON_SHARED_LIBRARY} ${PYTHON_LIBRARIES} libnumbuf.so)
endif(APPLE)

target_link_libraries(numbuf ${ARROW_LIB} ${ARROW_IO_LIB} ${ARROW_IPC_LIB} ${PYTHON_LIBRARIES})

install(TARGETS numbuf DESTINATION ${CMAKE_SOURCE_DIR}/numbuf/)
#install(FILES ${ARROW_LIB} ${ARROW_IO_LIB} ${ARROW_IPC_LIB} DESTINATION ${CMAKE_SOURCE_DIR}/numbuf/)
