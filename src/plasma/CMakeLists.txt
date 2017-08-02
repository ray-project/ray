cmake_minimum_required(VERSION 3.4)

project(plasma)

# Recursively include common
include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

# Include plasma
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/../thirdparty/arrow/python/cmake_modules)

find_package(Plasma)
include_directories(SYSTEM ${PLASMA_INCLUDE_DIR})

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++11 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -O3")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

include_directories("${ARROW_DIR}/cpp/src/")
# include_directories("${CMAKE_CURRENT_LIST_DIR}/../")

include_directories("${FLATBUFFERS_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

add_executable(plasma_manager
  plasma_manager.cc)

target_link_libraries(plasma_manager common ${PLASMA_STATIC_LIB} ${ARROW_DIR}/cpp/build/release/libarrow.a -lpthread)

define_test(client_tests ${PLASMA_STATIC_LIB})
define_test(manager_tests ${PLASMA_STATIC_LIB} plasma_manager.cc)
