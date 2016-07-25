cmake_minimum_required(VERSION 2.8)

project(numbuf)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

if(NOT APPLE)
  find_package(PythonInterp REQUIRED)
  find_package(PythonLibs REQUIRED)
  set(CUSTOM_PYTHON_EXECUTABLE ${PYTHON_EXECUTABLE})
else()
  find_program(CUSTOM_PYTHON_EXECUTABLE python)
  message("-- Found Python program: ${CUSTOM_PYTHON_EXECUTABLE}")
  execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "import sys; print 'python' + sys.version[0:3]"
                    OUTPUT_VARIABLE PYTHON_LIBRARY_NAME OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "import sys; print sys.exec_prefix"
                    OUTPUT_VARIABLE PYTHON_PREFIX OUTPUT_STRIP_TRAILING_WHITESPACE)
    FIND_LIBRARY(PYTHON_LIBRARIES
                 NAMES ${PYTHON_LIBRARY_NAME}
                 HINTS "${PYTHON_PREFIX}"
                 PATH_SUFFIXES "lib" "libs"
                 NO_DEFAULT_PATH)
    execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "from distutils.sysconfig import *; print get_python_inc()"
                    OUTPUT_VARIABLE PYTHON_INCLUDE_DIRS OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(PYTHON_LIBRARIES AND PYTHON_INCLUDE_DIRS)
        SET(PYTHONLIBS_FOUND TRUE)
        message("-- Found PythonLibs: " ${PYTHON_LIBRARIES})
        message("-- -- Used custom search path")
    else()
        find_package(PythonLibs REQUIRED)
        message("-- -- Used find_package(PythonLibs)")
    endif()
endif()

find_package(NumPy REQUIRED)

if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

include_directories("${PYTHON_INCLUDE_DIRS}")
include_directories("${NUMPY_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

if (UNIX AND NOT APPLE)
   link_libraries(rt)
endif()

set(ARROW_DIR "${CMAKE_SOURCE_DIR}/../arrow/" CACHE STRING
  "Path of the arrow source directory")

set(ARROW_STATIC_LIB "${CMAKE_SOURCE_DIR}/../arrow/cpp/build/release/libarrow.a" CACHE STRING
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

get_filename_component(PYTHON_SHARED_LIBRARY ${PYTHON_LIBRARIES} NAME)
if(APPLE)
  add_custom_command(TARGET numbuf
      POST_BUILD COMMAND
      ${CMAKE_INSTALL_NAME_TOOL} -change ${PYTHON_SHARED_LIBRARY} ${PYTHON_LIBRARIES} libnumbuf.so)
endif(APPLE)

target_link_libraries(numbuf ${ARROW_STATIC_LIB} ${PYTHON_LIBRARIES})
