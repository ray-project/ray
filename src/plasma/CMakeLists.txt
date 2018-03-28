cmake_minimum_required(VERSION 3.4)

project(plasma)

# Recursively include common
include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --std=c99 -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++11 -O3 -Werror -Wall")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

include_directories("${ARROW_DIR}/cpp/src/")
# include_directories("${CMAKE_CURRENT_LIST_DIR}/../")

include_directories("${FLATBUFFERS_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

add_executable(plasma_manager
  plasma_manager.cc)

target_link_libraries(plasma_manager common ${PLASMA_STATIC_LIB} ray_static ${ARROW_STATIC_LIB} -lpthread ${Boost_SYSTEM_LIBRARY})

define_test(client_tests "")
define_test(manager_tests "" plasma_manager.cc)
target_link_libraries(manager_tests ${Boost_SYSTEM_LIBRARY})
