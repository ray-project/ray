cmake_minimum_required(VERSION 3.4)

project(ray)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules")

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/src/thirdparty/arrow/python/cmake_modules)

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
