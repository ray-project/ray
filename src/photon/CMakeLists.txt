cmake_minimum_required(VERSION 2.8)

project(photon)

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
# Now find the Python libraries. We'll start by looking near the Python
# executable. If that fails, then we'll look near the Python include
# directories.
execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c "import sys; print(sys.exec_prefix)"
                OUTPUT_VARIABLE PYTHON_PREFIX OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "PYTHON_PREFIX: " ${PYTHON_PREFIX})
# The name ending in "m" is for miniconda.
FIND_LIBRARY(PYTHON_LIBRARIES
             NAMES "${PYTHON_LIBRARY_NAME}" "${PYTHON_LIBRARY_NAME}m"
             HINTS "${PYTHON_PREFIX}"
             PATH_SUFFIXES "lib" "libs"
             NO_DEFAULT_PATH)
message(STATUS "PYTHON_LIBRARIES: " ${PYTHON_LIBRARIES})
# If that failed, perhaps because the user is in a virtualenv, search around
# the Python include directories.
if(NOT PYTHON_LIBRARIES)
  message(STATUS "Failed to find PYTHON_LIBRARIES near the Python executable, so now looking near the Python include directories.")
  # The name ending in "m" is for miniconda.
  FIND_LIBRARY(PYTHON_LIBRARIES
               NAMES "${PYTHON_LIBRARY_NAME}" "${PYTHON_LIBRARY_NAME}m"
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

set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} --std=c99 -Werror")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

set(PHOTON_CLIENT_LIB "${CMAKE_SOURCE_DIR}/build/photon_client.a"
    CACHE STRING "Path to photon_client.a")

set(COMMON_LIB "${CMAKE_SOURCE_DIR}/../common/build/libcommon.a"
    CACHE STRING "Path to libcommon.a")

include_directories("${CMAKE_SOURCE_DIR}/")
include_directories("${CMAKE_SOURCE_DIR}/../")
include_directories("${CMAKE_SOURCE_DIR}/../common/")
include_directories("${CMAKE_SOURCE_DIR}/../common/thirdparty/")
include_directories("${CMAKE_SOURCE_DIR}/../common/lib/python/")
include_directories("${CMAKE_SOURCE_DIR}/../plasma/")

add_library(photon SHARED
  photon_extension.c
  ../common/lib/python/common_extension.c)

get_filename_component(PYTHON_SHARED_LIBRARY ${PYTHON_LIBRARIES} NAME)
if(APPLE)
  add_custom_command(TARGET photon
                     POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change ${PYTHON_SHARED_LIBRARY} ${PYTHON_LIBRARIES} libphoton.so)
endif(APPLE)

target_link_libraries(photon ${PHOTON_CLIENT_LIB} ${COMMON_LIB} ${PYTHON_LIBRARIES})

install(TARGETS photon DESTINATION ${CMAKE_SOURCE_DIR}/photon)
