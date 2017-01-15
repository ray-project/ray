cmake_minimum_required(VERSION 2.8)

project(photon)

# Recursively include common
include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

include_directories("${PYTHON_INCLUDE_DIRS}")

# set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} --std=c99 -Werror")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

include_directories("${CMAKE_CURRENT_LIST_DIR}/")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../plasma/")

add_library(photon SHARED
  photon_extension.c
  ../common/lib/python/common_extension.c)

get_filename_component(PYTHON_SHARED_LIBRARY ${PYTHON_LIBRARIES} NAME)
if(APPLE)
  add_custom_command(TARGET photon
                     POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change ${PYTHON_SHARED_LIBRARY} ${PYTHON_LIBRARIES} libphoton.so)
endif(APPLE)

add_library(photon_client STATIC photon_client.c)

target_link_libraries(photon photon_client ${COMMON_LIB} ${PYTHON_LIBRARIES})

add_executable(photon_scheduler photon_scheduler.c photon_algorithm.c)
target_link_libraries(photon_scheduler photon_client common ${HIREDIS_LIB} plasma_lib)

add_executable(photon_tests test/photon_tests.c photon_scheduler.c photon_algorithm.c )
target_link_libraries(photon_tests photon_client common ${HIREDIS_LIB} plasma_lib)
target_compile_options(photon_tests PUBLIC "-DPHOTON_TEST")

install(TARGETS photon DESTINATION ${CMAKE_SOURCE_DIR}/photon)
