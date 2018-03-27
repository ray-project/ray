if ("${CMAKE_COMMON_RUNTIME}" STREQUAL "")
    set(CMAKE_COMMON_RUNTIME "dummy")
endif ()

if (NOT EXISTS ${CMAKE_CURRENT_LIST_DIR}/providers/${CMAKE_COMMON_RUNTIME})
    message (FATAL "Common runtime provider '${CMAKE_CURRENT_LIST_DIR}/providers/${CMAKE_COMMON_RUNTIME}' "
        "does not exist, please choose correct common runtime by -DCMAKE_COMMON_RUNTIME=xxx where "
        "'${CMAKE_CURRENT_LIST_DIR}/providers/xxx' does exist")
else ()
    message (INFO "Using common runtime provider '${CMAKE_CURRENT_LIST_DIR}/providers/${CMAKE_COMMON_RUNTIME}' ")
endif ()

set(PROJ_NAME common.runtime)
project(${PROJ_NAME} C CXX)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y" CACHE STRING "" FORCE)

include(${CMAKE_CURRENT_LIST_DIR}/../../../../thirdparty/scripts/thirdparty.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/providers/${CMAKE_COMMON_RUNTIME}/${CMAKE_COMMON_RUNTIME}.cmake)
include_directories(${CMAKE_CURRENT_LIST_DIR}/interface)
include_directories(${CMAKE_CURRENT_LIST_DIR}/providers/${CMAKE_COMMON_RUNTIME})

add_subdirectory(test)

set(PROJ_SRC "")
file(GLOB
    PROJ_SRC
    "${CMAKE_CURRENT_LIST_DIR}/providers/${CMAKE_COMMON_RUNTIME}/*.cc"
    "${CMAKE_CURRENT_LIST_DIR}/providers/${CMAKE_COMMON_RUNTIME}/*.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/providers/${CMAKE_COMMON_RUNTIME}/*.c"
    "${CMAKE_CURRENT_LIST_DIR}/providers/${CMAKE_COMMON_RUNTIME}/*.h"
    "${CMAKE_CURRENT_LIST_DIR}/providers/${CMAKE_COMMON_RUNTIME}/*.hpp"
    )

add_library(${PROJ_NAME} "STATIC" ${PROJ_SRC})

