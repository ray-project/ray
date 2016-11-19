cmake_minimum_required(VERSION 2.8)

project(plasma)

if(NOT APPLE)
  find_package(PythonInterp REQUIRED)
  find_package(PythonLibs REQUIRED)
  set(CUSTOM_PYTHON_EXECUTABLE ${PYTHON_EXECUTABLE})
else()
  message("-- -- Trying custom approach for finding Python.")
  find_program(CUSTOM_PYTHON_EXECUTABLE python)
  message("-- Found Python program: ${CUSTOM_PYTHON_EXECUTABLE}")
  execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "import sys; print 'python' + sys.version[0:3]"
                    OUTPUT_VARIABLE PYTHON_LIBRARY_NAME OUTPUT_STRIP_TRAILING_WHITESPACE)
  message("-- PYTHON_LIBRARY_NAME: " ${PYTHON_LIBRARY_NAME})
    execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "import sys; print sys.exec_prefix"
                    OUTPUT_VARIABLE PYTHON_PREFIX OUTPUT_STRIP_TRAILING_WHITESPACE)
    message("-- PYTHON_PREFIX: " ${PYTHON_PREFIX})
    FIND_LIBRARY(PYTHON_LIBRARIES
                 NAMES ${PYTHON_LIBRARY_NAME}
                 HINTS "${PYTHON_PREFIX}"
                 PATH_SUFFIXES "lib" "libs"
                 NO_DEFAULT_PATH)
    message("-- PYTHON_LIBRARIES: " ${PYTHON_LIBRARIES})
    execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "from distutils.sysconfig import *; print get_python_inc()"
                    OUTPUT_VARIABLE PYTHON_INCLUDE_DIRS OUTPUT_STRIP_TRAILING_WHITESPACE)
    message("-- PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})
    if(PYTHON_LIBRARIES AND PYTHON_INCLUDE_DIRS)
        message("-- -- The custom approach for finding Python succeeded.")
        SET(PYTHONLIBS_FOUND TRUE)
    else()
        message("-- -- The custom approach for finding Python failed. Defaulting to find_package.")
        find_package(PythonInterp REQUIRED)
        find_package(PythonLibs REQUIRED)
    endif()
endif()
message("-- Using PYTHON_LIBRARIES: " ${PYTHON_LIBRARIES})
message("-- Using PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})

if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

include_directories("${PYTHON_INCLUDE_DIRS}")

set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L")

if (UNIX AND NOT APPLE)
   link_libraries(rt)
endif()

set(COMMON_LIB "${CMAKE_SOURCE_DIR}/../common/build/libcommon.a" CACHE STRING
  "Path to libcommon.a")

include_directories("${CMAKE_SOURCE_DIR}/")
include_directories("${CMAKE_SOURCE_DIR}/../")
include_directories("${CMAKE_SOURCE_DIR}/../common/")
include_directories("${CMAKE_SOURCE_DIR}/../common/thirdparty/")
include_directories("${CMAKE_SOURCE_DIR}/../common/lib/python/")

add_library(plasma SHARED
  plasma_extension.c
  plasma_client.c
  fling.c)

get_filename_component(PYTHON_SHARED_LIBRARY ${PYTHON_LIBRARIES} NAME)
if(APPLE)
  add_custom_command(TARGET plasma
      POST_BUILD COMMAND
      ${CMAKE_INSTALL_NAME_TOOL} -change ${PYTHON_SHARED_LIBRARY} ${PYTHON_LIBRARIES} libplasma.so)
endif(APPLE)

target_link_libraries(plasma ${COMMON_LIB} ${PYTHON_LIBRARIES})

install(TARGETS plasma DESTINATION ${CMAKE_SOURCE_DIR}/lib/python)
