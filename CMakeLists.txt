cmake_minimum_required(VERSION 2.8)

project(numbuf)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

# Make libnumbuf.so look for shared libraries in the folder libnumbuf.so is in
set(CMAKE_INSTALL_RPATH "$ORIGIN/")
set(CMAKE_MACOSX_RPATH 1)

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

set(ARROW_DIR "${CMAKE_SOURCE_DIR}/thirdparty/arrow/" CACHE STRING
  "Path of the arrow source directory")

if (APPLE)
  set(ARROW_LIB "${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/build/release/libarrow.dylib" CACHE STRING
    "Path to libarrow.dylib (needs to be changed if arrow is build in debug mode)")

  set(ARROW_IO_LIB "${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/build/release/libarrow_io.dylib" CACHE STRING
    "Path to libarrow_io.dylib (needs to be changed if arrow is build in debug mode)")

  set(ARROW_IPC_LIB "${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/build/release/libarrow_ipc.dylib" CACHE STRING
    "Path to libarrow_ipc.dylib (needs to be changed if arrow is build in debug mode)")
else()
  set(ARROW_LIB "${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/build/release/libarrow.so" CACHE STRING
    "Path to libarrow.so (needs to be changed if arrow is build in debug mode)")

  set(ARROW_IO_LIB "${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/build/release/libarrow_io.so" CACHE STRING
    "Path to libarrow_io.so (needs to be changed if arrow is build in debug mode)")

  set(ARROW_IPC_LIB "${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/build/release/libarrow_ipc.so" CACHE STRING
    "Path to libarrow_ipc.so (needs to be changed if arrow is build in debug mode)")
endif()

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
  add_custom_command(TARGET numbuf
      POST_BUILD COMMAND
      ${CMAKE_INSTALL_NAME_TOOL} -change "@rpath/libarrow.dylib" "@loader_path/libarrow.dylib" libnumbuf.so)
  add_custom_command(TARGET numbuf
      POST_BUILD COMMAND
      ${CMAKE_INSTALL_NAME_TOOL} -change "@rpath/libarrow_io.dylib" "@loader_path/libarrow_io.dylib" libnumbuf.so)
  add_custom_command(TARGET numbuf
      POST_BUILD COMMAND
      ${CMAKE_INSTALL_NAME_TOOL} -change "@rpath/libarrow_ipc.dylib" "@loader_path/libarrow_ipc.dylib" libnumbuf.so)
endif(APPLE)

target_link_libraries(numbuf ${ARROW_LIB} ${ARROW_IO_LIB} ${ARROW_IPC_LIB} ${PYTHON_LIBRARIES})

install(TARGETS numbuf DESTINATION ${CMAKE_SOURCE_DIR}/numbuf/)
install(FILES ${ARROW_LIB} ${ARROW_IO_LIB} ${ARROW_IPC_LIB} DESTINATION ${CMAKE_SOURCE_DIR}/numbuf/)
