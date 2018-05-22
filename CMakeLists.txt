cmake_minimum_required(VERSION 3.4)

project(ray)

set(CMAKE_RAY_LANG_PYTHON "NO")
set(CMAKE_RAY_LANG_JAVA "NO")
if ("${CMAKE_RAY_LANGUAGE}" STREQUAL "python")
  set(CMAKE_RAY_LANG_PYTHON "YES")
elseif ("${CMAKE_RAY_LANGUAGE}" STREQUAL "java")
  set(CMAKE_RAY_LANG_JAVA "YES")
elseif ("${CMAKE_RAY_LANGUAGE}" STREQUAL "")
  message(WARNING "Language is not set, choose Python as default.")
  set(CMAKE_RAY_LANG_PYTHON "YES")
else()
  message(FATAL_ERROR "Unrecognized language, use -DCMAKE_RAY_LANGUAGE=java|python. Abort.")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules")

include(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/scripts/thirdparty.cmake)

find_package(Arrow)
find_package(Plasma)

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 11)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(RAY_BUILD_STATIC
  "Build the libray static libraries"
  ON)

option(RAY_BUILD_SHARED
  "Build the libray shared libraries"
  ON)

option(RAY_BUILD_TESTS
  "Build the Ray googletest unit tests"
  ON)

option(RAY_USE_NEW_GCS
  "Use the new GCS implementation"
  OFF)

if (RAY_USE_NEW_GCS)
  add_definitions(-DRAY_USE_NEW_GCS)
endif()

include(ExternalProject)
include(GNUInstallDirs)
include(BuildUtils)
enable_testing()

include(ThirdpartyToolchain)

include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
include_directories(SYSTEM ${PLASMA_INCLUDE_DIR})
include_directories("${CMAKE_CURRENT_LIST_DIR}/src/")

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/ray/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/common/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/plasma/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/local_scheduler/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/global_scheduler/)
