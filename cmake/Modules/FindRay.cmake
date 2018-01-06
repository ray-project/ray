# - Find RAY
# This module defines
#  RAY_INCLUDE_DIR, directory containing headers
#  RAY_LIBS, directory containing ray libraries
#  RAY_STATIC_LIB, path to static library
#  RAY_SHARED_LIB, path to shared library
#  RAY_FOUND, whether ray has been found

include(FindPkgConfig)

if ("$ENV{RAY_HOME}" STREQUAL "")
  pkg_check_modules(RAY ray)
  if (RAY_FOUND)
    pkg_get_variable(RAY_ABI_VERSION ray abi_version)
    message(STATUS "Ray ABI version: ${RAY_ABI_VERSION}")
    pkg_get_variable(RAY_SO_VERSION ray so_version)
    message(STATUS "Ray SO version: ${RAY_SO_VERSION}")
    set(RAY_INCLUDE_DIR ${RAY_INCLUDE_DIRS})
    set(RAY_LIBS ${RAY_LIBRARY_DIRS})
    set(RAY_SEARCH_LIB_PATH ${RAY_LIBRARY_DIRS})
  endif()
else()
  set(RAY_HOME "$ENV{RAY_HOME}")

  set(RAY_SEARCH_HEADER_PATHS
    ${RAY_HOME}/include
    )

  set(RAY_SEARCH_LIB_PATH
    ${RAY_HOME}/lib
    )

  find_path(RAY_INCLUDE_DIR ray/gcs/client.h PATHS
    ${RAY_SEARCH_HEADER_PATHS}
    # make sure we don't accidentally pick up a different version
    NO_DEFAULT_PATH
    )
endif()

find_library(RAY_LIB_PATH NAMES ray
  PATHS
  ${RAY_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)
get_filename_component(RAY_LIBS ${RAY_LIB_PATH} DIRECTORY)

if (RAY_INCLUDE_DIR AND RAY_LIBS)
  set(RAY_FOUND TRUE)
  set(RAY_LIB_NAME ray)

  set(RAY_STATIC_LIB ${RAY_LIBS}/lib${RAY_LIB_NAME}.a)

  set(RAY_SHARED_LIB ${RAY_LIBS}/lib${RAY_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
endif()

if (RAY_FOUND)
  if (NOT Ray_FIND_QUIETLY)
    message(STATUS "Found the Ray core library: ${RAY_LIB_PATH}")
  endif ()
else ()
  if (NOT Ray_FIND_QUIETLY)
    set(RAY_ERR_MSG "Could not find the Ray library. Looked for headers")
    set(RAY_ERR_MSG "${RAY_ERR_MSG} in ${RAY_SEARCH_HEADER_PATHS}, and for libs")
    set(RAY_ERR_MSG "${RAY_ERR_MSG} in ${RAY_SEARCH_LIB_PATH}")
    if (Ray_FIND_REQUIRED)
      message(FATAL_ERROR "${RAY_ERR_MSG}")
    else (Ray_FIND_REQUIRED)
      message(STATUS "${RAY_ERR_MSG}")
    endif (Ray_FIND_REQUIRED)
  endif ()
  set(RAY_FOUND FALSE)
endif ()

mark_as_advanced(
  RAY_INCLUDE_DIR
  RAY_STATIC_LIB
  RAY_SHARED_LIB
)
