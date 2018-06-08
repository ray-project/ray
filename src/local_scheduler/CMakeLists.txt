cmake_minimum_required(VERSION 3.4)

project(local_scheduler)

# Recursively include common
include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
  # Include plasma
  list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/../../thirdparty/pkg/arrow/python/cmake_modules)
  find_package(Arrow)
  find_package(Plasma)
  include_directories(SYSTEM ${PLASMA_INCLUDE_DIR})
endif()


add_definitions(-fPIC)

if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
  if(APPLE)
    SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
  endif(APPLE)
  include_directories("${PYTHON_INCLUDE_DIRS}")
  include_directories("${NUMPY_INCLUDE_DIR}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

include_directories("${CMAKE_CURRENT_LIST_DIR}/")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../")
# TODO(pcm): get rid of this:
if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
  include_directories("${CMAKE_CURRENT_LIST_DIR}/../plasma/")
endif()

include_directories("${ARROW_DIR}/cpp/src/")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../common/format/")

# Compile flatbuffers

set(LOCAL_SCHEDULER_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/local_scheduler.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

set(LOCAL_SCHEDULER_FBS_OUTPUT_FILES
  "${OUTPUT_DIR}/local_scheduler_generated.h")

add_custom_command(
  OUTPUT ${LOCAL_SCHEDULER_FBS_OUTPUT_FILES}
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${LOCAL_SCHEDULER_FBS_SRC} --gen-object-api --scoped-enums
  DEPENDS ${FBS_DEPENDS}
  COMMENT "Running flatc compiler on ${LOCAL_SCHEDULER_FBS_SRC}"
  VERBATIM)

add_custom_target(gen_local_scheduler_fbs DEPENDS ${LOCAL_SCHEDULER_FBS_OUTPUT_FILES})

add_dependencies(gen_local_scheduler_fbs flatbuffers_ep)

add_library(local_scheduler_client STATIC local_scheduler_client.cc)

add_dependencies(local_scheduler_client gen_local_scheduler_fbs)

add_executable(local_scheduler local_scheduler.cc local_scheduler_algorithm.cc)
target_link_libraries(local_scheduler local_scheduler_client common ${HIREDIS_LIB} ${PLASMA_STATIC_LIB} ray_static ${ARROW_STATIC_LIB} -lpthread ${Boost_SYSTEM_LIBRARY})

add_executable(local_scheduler_tests test/local_scheduler_tests.cc local_scheduler.cc local_scheduler_algorithm.cc)
target_link_libraries(local_scheduler_tests local_scheduler_client common ${HIREDIS_LIB} ${PLASMA_STATIC_LIB} ray_static ${ARROW_STATIC_LIB} -lpthread ${Boost_SYSTEM_LIBRARY})
target_compile_options(local_scheduler_tests PUBLIC "-DLOCAL_SCHEDULER_TEST")

macro(set_local_scheduler_library LANG)
  include_directories("${CMAKE_CURRENT_LIST_DIR}/../common/lib/${LANG}/")

  file(GLOB LOCAL_SCHEDULER_LIBRARY_${LANG}_SRC
    lib/${LANG}/*.cc
    ${CMAKE_CURRENT_LIST_DIR}/../common/lib/${LANG}/*.cc)
  add_library(local_scheduler_library_${LANG} SHARED
    ${LOCAL_SCHEDULER_LIBRARY_${LANG}_SRC})

    if(APPLE)
      target_link_libraries(local_scheduler_library_${LANG} "-undefined dynamic_lookup" local_scheduler_client common ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} ${Boost_SYSTEM_LIBRARY})
    else(APPLE)
      target_link_libraries(local_scheduler_library_${LANG} local_scheduler_client common ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} ${Boost_SYSTEM_LIBRARY})
    endif(APPLE)

  add_dependencies(local_scheduler_library_${LANG} gen_local_scheduler_fbs)

  install(TARGETS local_scheduler_library_${LANG} DESTINATION ${CMAKE_SOURCE_DIR}/local_scheduler)
endmacro()

if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
  set_local_scheduler_library("python")
endif()

if ("${CMAKE_RAY_LANG_JAVA}" STREQUAL "YES")
  add_compile_options("-I$ENV{JAVA_HOME}/include/")
  if(WIN32)
    add_compile_options("-I$ENV{JAVA_HOME}/include/win32")
  elseif(APPLE)
    add_compile_options("-I$ENV{JAVA_HOME}/include/darwin")
  else() # linux
    add_compile_options("-I$ENV{JAVA_HOME}/include/linux")
  endif()
  set_local_scheduler_library("java")
endif()
