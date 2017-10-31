cmake_minimum_required(VERSION 3.4)

project(ray)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules")

include(ExternalProject)
include(GNUInstallDirs)
include(BuildUtils)

set(ARROW_DIR "${CMAKE_CURRENT_LIST_DIR}/src/thirdparty/arrow/"
    CACHE STRING "Path of the arrow source directory")

include_directories("${ARROW_DIR}/cpp/src/")

option(RAY_BUILD_STATIC
  "Build the libarrow static libraries"
  ON)

option(RAY_BUILD_SHARED
  "Build the libarrow shared libraries"
  ON)

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/ray/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/common/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/plasma/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/local_scheduler/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/global_scheduler/)
