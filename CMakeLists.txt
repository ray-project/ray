cmake_minimum_required(VERSION 3.4)

project(ray)

set(ARROW_DIR "${CMAKE_CURRENT_LIST_DIR}/src/thirdparty/arrow/"
    CACHE STRING "Path of the arrow source directory")

include_directories("${ARROW_DIR}/cpp/src/")

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/common/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/plasma/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/local_scheduler/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/global_scheduler/)
