cmake_minimum_required(VERSION 3.4)

project(ray)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules")


# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 11)

# Use old C++ ABI to be compatible with TensorFlow
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ray build options
option(RAY_BUILD_PYTHON
    "build python library"
    OFF)

option(RAY_BUILD_JAVA
    "build java library"
    OFF)

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

option(RAY_USE_GLOG
  "Build the logging system using glog"
  ON)

if (RAY_BUILD_PYTHON)
  set(CMAKE_RAY_LANG_PYTHON YES)
endif ()

if (RAY_BUILD_JAVA)
  set(CMAKE_RAY_LANG_JAVA YES)
endif ()

if ("${CMAKE_RAY_LANG_JAVA}" STREQUAL "NO" AND "${CMAKE_RAY_LANG_PYTHON}" STREQUAL "NO")
  message(FATAL_ERROR "Please specify which language support you want to build by passing \
   -DRAY_BUILD_PYTHON=on and/or -DRAY_BUILD_JAVA=on to CMake")
endif ()

if (RAY_USE_NEW_GCS)
  add_definitions(-DRAY_USE_NEW_GCS)
endif()

include(ExternalProject)
include(GNUInstallDirs)
include(BuildUtils)
enable_testing()

include(ThirdpartyToolchain)
include(Common)

# TODO(rkn): Fix all of this. This include is needed for the following
# reason. The local scheduler depends on tables.cc which depends on
# node_manager_generated.h which depends on gcs_generated.h. However,
# the include statement for gcs_generated.h doesn't include the file
# path, so we include the relevant directory here.
set(GCS_FBS_OUTPUT_DIRECTORY
        "${CMAKE_CURRENT_LIST_DIR}/src/ray/gcs/format")
include_directories(${GCS_FBS_OUTPUT_DIRECTORY})


include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
include_directories(SYSTEM ${PLASMA_INCLUDE_DIR})
include_directories("${CMAKE_CURRENT_LIST_DIR}/src/")

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/ray/)

# final target copy_ray
add_custom_target(copy_ray ALL)

# copy plasma_store_server
add_custom_command(TARGET copy_ray POST_BUILD
  COMMAND mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/src/plasma
  COMMAND ${CMAKE_COMMAND} -E
    copy ${ARROW_HOME}/bin/plasma_store_server ${CMAKE_CURRENT_BINARY_DIR}/src/plasma/)

if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
  # add pyarrow as the dependency
  add_dependencies(copy_ray pyarrow_ext)

  # NOTE: The lists below must be kept in sync with ray/python/setup.py.

  set(ray_file_list
        "src/ray/thirdparty/redis/src/redis-server"
        "src/ray/gcs/redis_module/libray_redis_module.so"
        "src/ray/raylet/raylet_monitor"
        "src/ray/raylet/raylet")

  if (RAY_USE_NEW_GCS)
    list(APPEND ray_file_list "src/credis/build/src/libmember.so")
    list(APPEND ray_file_list "src/credis/build/src/libmaster.so")
    list(APPEND ray_file_list "src/credis/redis/src/redis-server")
  endif()

  # The goal of the if statement below is to require the catapult files to be
  # present INCLUDE_UI=1 is set and to include the UI files if they are present.
  # This should match the logic in build_ui.sh.
  if (EXISTS "${CMAKE_BINARY_DIR}/src/catapult_files/index.html" OR "$ENV{INCLUDE_UI}" STREQUAL "1")
    list(APPEND ray_file_list "src/catapult_files/index.html")
    list(APPEND ray_file_list "src/catapult_files/trace_viewer_full.html")
  endif()

  set(build_ray_file_list)
  foreach(file ${ray_file_list})
    list(APPEND build_ray_file_list ${CMAKE_BINARY_DIR}/${file})
  endforeach()

  add_custom_target(copy_ray_files DEPENDS ${build_ray_file_list} copy_redis ray_redis_module)
  add_dependencies(copy_ray copy_ray_files)

  # Build Cython extensions
  add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/python)

  foreach(file ${ray_file_list})
  add_custom_command(TARGET copy_ray POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E
                        copy ${CMAKE_BINARY_DIR}/${file}
                            ${CMAKE_BINARY_DIR}/../python/ray/core/${file})
  endforeach()

  # copy plasma_store_server to python
  add_custom_command(TARGET copy_ray POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E
      copy ${ARROW_HOME}/bin/plasma_store_server ${CMAKE_SOURCE_DIR}/python/ray/core/src/plasma/)

  # copy Cython files
  add_custom_command(TARGET copy_ray POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/python/_raylet.so ${CMAKE_SOURCE_DIR}/python/ray/)

endif()

if ("${CMAKE_RAY_LANG_JAVA}" STREQUAL "YES")
  get_raylet_library("java" RAYLET_LIBRARY_JAVA)
  add_dependencies(copy_ray ${RAYLET_LIBRARY_JAVA})

  # Copy java native dependencies.
  add_custom_command(TARGET copy_ray POST_BUILD
    COMMAND mkdir -p ${CMAKE_SOURCE_DIR}/java/runtime/native_dependencies)
  set(java_native_dependencies
    "src/ray/thirdparty/redis/src/redis-server"
    "src/ray/gcs/redis_module/libray_redis_module.so"
    "src/ray/raylet/raylet"
    "src/ray/raylet/libraylet_library_java.*")
  foreach(file ${java_native_dependencies})
  add_custom_command(TARGET copy_ray POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/${file}
      ${CMAKE_SOURCE_DIR}/java/runtime/native_dependencies)
  endforeach()
add_custom_command(TARGET copy_ray POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy ${ARROW_HOME}/bin/plasma_store_server
    ${CMAKE_SOURCE_DIR}/java/runtime/native_dependencies)
  add_custom_command(TARGET copy_ray POST_BUILD
    COMMAND $(CMAKE_COMMAND) -E copy ${ARROW_LIBRARY_DIR}/libplasma_java.*
      ${CMAKE_SOURCE_DIR}/java/runtime/native_dependencies)
endif()
