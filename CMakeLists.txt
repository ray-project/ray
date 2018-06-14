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

if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
  # NOTE: The lists below must be kept in sync with ray/python/setup.py.

  set(ray_file_list
        "src/common/thirdparty/redis/src/redis-server"
        "src/common/redis_module/libray_redis_module.so"
        "src/plasma/plasma_manager"
        "src/local_scheduler/local_scheduler"
        "src/local_scheduler/liblocal_scheduler_library_python.so"
        "src/global_scheduler/global_scheduler"
        "src/ray/raylet/raylet_monitor"
        "src/ray/raylet/raylet")

  if (RAY_USE_NEW_GCS)
    list(APPEND ray_file_list "src/credis/build/src/libmember.so")
    list(APPEND ray_file_list "src/credis/build/src/libmaster.so")
    list(APPEND ray_file_list "src/credis/redis/src/redis-server")
  endif()

  if (DEFINED ENV{INCLUDE_UI} AND "$ENV{INCLUDE_UI}" STREQUAL "1")
    list(APPEND ray_file_list "src/catapult_files/index.html")
    list(APPEND ray_file_list "src/catapult_files/trace_viewer_full.html")
  endif()

  set(build_ray_file_list)
  foreach(file ${ray_file_list})
    list(APPEND build_ray_file_list ${CMAKE_BINARY_DIR}/${file})
  endforeach()

  add_custom_target(copy_ray ALL
      DEPENDS ${build_ray_file_list})

  # Make sure that the Python extensions are built before copying the files.
  if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
    get_local_scheduler_library("python" LOCAL_SCHEDULER_LIBRARY_LANG)
    add_dependencies(copy_ray ${LOCAL_SCHEDULER_LIBRARY_LANG})
  endif()
  if ("${CMAKE_RAY_LANG_JAVA}" STREQUAL "YES")
    get_local_scheduler_library("java" LOCAL_SCHEDULER_LIBRARY_LANG)
    add_dependencies(copy_ray ${LOCAL_SCHEDULER_LIBRARY_LANG})
  endif()

  add_dependencies(copy_ray ray_redis_module)

  foreach(file ${ray_file_list})
  add_custom_command(TARGET copy_ray POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E
                        copy ${CMAKE_BINARY_DIR}/${file}
                            ${CMAKE_BINARY_DIR}/../python/ray/core/${file})
  endforeach()
endif()
