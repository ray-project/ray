cmake_minimum_required(VERSION 2.8)

project(plasma)

include(ExternalProject)
include(CMakeParseArguments)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(FLATBUFFERS_VERSION "1.3.0")

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

message(STATUS "Using CUSTOM_PYTHON_EXECUTABLE: " ${CUSTOM_PYTHON_EXECUTABLE})
message(STATUS "Using PYTHON_LIBRARIES: " ${PYTHON_LIBRARIES})
message(STATUS "Using PYTHON_INCLUDE_DIRS: " ${PYTHON_INCLUDE_DIRS})

if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)


include_directories("${PYTHON_INCLUDE_DIRS}")

############################################################
# Dependencies
############################################################
function(ADD_THIRDPARTY_LIB LIB_NAME)
  set(options)
  set(one_value_args SHARED_LIB STATIC_LIB)
  set(multi_value_args DEPS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(ARG_STATIC_LIB AND ARG_SHARED_LIB)
    if(NOT ARG_STATIC_LIB)
      message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
    endif()

    SET(AUG_LIB_NAME "${LIB_NAME}_static")
    add_library(${AUG_LIB_NAME} STATIC IMPORTED)
    set_target_properties(${AUG_LIB_NAME}
            PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")

    SET(AUG_LIB_NAME "${LIB_NAME}_shared")
    add_library(${AUG_LIB_NAME} SHARED IMPORTED)
    set_target_properties(${AUG_LIB_NAME}
            PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
  elseif(ARG_STATIC_LIB)
    add_library(${LIB_NAME} STATIC IMPORTED)
    set_target_properties(${LIB_NAME}
            PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")
  elseif(ARG_SHARED_LIB)
    add_library(${LIB_NAME} SHARED IMPORTED)
    set_target_properties(${LIB_NAME}
            PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
  else()
    message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
  endif()

  if(ARG_DEPS)
    set_target_properties(${LIB_NAME}
            PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
  endif()
endfunction()

############################################################
# Flatbuffers
############################################################

if("$ENV{FLATBUFFERS_HOME}" STREQUAL "")
  set(FLATBUFFERS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/flatbuffers_ep-prefix/src/flatbuffers_ep-install")
  ExternalProject_Add(flatbuffers_ep
    URL "https://github.com/google/flatbuffers/archive/v${FLATBUFFERS_VERSION}.tar.gz"
    CMAKE_ARGS
      "-DCMAKE_CXX_FLAGS=-fPIC"
      "-DCMAKE_INSTALL_PREFIX:PATH=${FLATBUFFERS_PREFIX}"
      "-DFLATBUFFERS_BUILD_TESTS=OFF")

  set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_PREFIX}/include")
  set(FLATBUFFERS_STATIC_LIB "${FLATBUFFERS_PREFIX}/lib/libflatbuffers.a")
  set(FLATBUFFERS_COMPILER "${FLATBUFFERS_PREFIX}/bin/flatc")
  set(FLATBUFFERS_VENDORED 1)
else()
  find_package(Flatbuffers REQUIRED)
  set(FLATBUFFERS_VENDORED 0)
endif()

message(STATUS "Flatbuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")
message(STATUS "Flatbuffers static library: ${FLATBUFFERS_STATIC_LIB}")
message(STATUS "Flatbuffers compiler: ${FLATBUFFERS_COMPILER}")
include_directories(SYSTEM ${FLATBUFFERS_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(flatbuffers
  STATIC_LIB ${FLATBUFFERS_STATIC_LIB})

if(FLATBUFFERS_VENDORED)
  add_dependencies(flatbuffers flatbuffers_ep)
endif()

set(FBS_SRC
  "${CMAKE_SOURCE_DIR}/format/plasma_protocol.fbs")

set(OUTPUT_DIR ${CMAKE_SOURCE_DIR}/format/)
set(FBS_OUTPUT_FILES "${OUTPUT_DIR}/Message_generated.h")

add_custom_command(
  OUTPUT ${FBS_OUTPUT_FILES}
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${FBS_SRC}
  DEPENDS ${FBS_DEPENDS}
  COMMENT "Running flatc compiler on ${FBS_SRC}"
  VERBATIM
)

add_custom_target(protocol_fbs DEPENDS ${FBS_OUTPUT_FILES})

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

set(COMMON_LIB "${CMAKE_SOURCE_DIR}/../common/build/libcommon.a"
    CACHE STRING "Path to libcommon.a")

include_directories("${CMAKE_SOURCE_DIR}/")
include_directories("${CMAKE_SOURCE_DIR}/../")
include_directories("${CMAKE_SOURCE_DIR}/../common/")
include_directories("${CMAKE_SOURCE_DIR}/../common/thirdparty/")
include_directories("${CMAKE_SOURCE_DIR}/../common/lib/python/")

add_library(plasma SHARED
  plasma_protocol.cc
  plasma.c
  plasma_extension.c
  plasma_client.c
  fling.c)

target_link_libraries(plasma -Wl,--whole-archive ${FLATBUFFERS_STATIC_LIB} -Wl,--no-whole-archive)

include_directories("${FLATBUFFERS_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static-libgcc -static-libstdc++ -fPIC")

add_library(plasmaprotocol STATIC
  plasma_protocol.cc)

target_link_libraries(plasmaprotocol ${FLATBUFFERS_STATIC_LIB})

add_library(plasma_client STATIC
  plasma_protocol.cc
  plasma.c
  plasma_client.c
  fling.c)

target_link_libraries(plasma_client ${FLATBUFFERS_STATIC_LIB})

get_filename_component(PYTHON_SHARED_LIBRARY ${PYTHON_LIBRARIES} NAME)
if(APPLE)
  add_custom_command(TARGET plasma
                     POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change ${PYTHON_SHARED_LIBRARY} ${PYTHON_LIBRARIES} libplasma.so)
endif(APPLE)

target_link_libraries(plasma ${COMMON_LIB} ${PYTHON_LIBRARIES})

add_dependencies(plasma protocol_fbs)

install(TARGETS plasma DESTINATION ${CMAKE_SOURCE_DIR}/lib/python)
