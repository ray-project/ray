cmake_minimum_required(VERSION 2.8)

project(ray)

set(THIRDPARTY_DIR "${CMAKE_SOURCE_DIR}/thirdparty")

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

set(CMAKE_PREFIX_PATH "${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/" ${CMAKE_PREFIX_PATH})

if(NOT APPLE)
    find_package(PythonInterp REQUIRED)
    find_package(PythonLibs REQUIRED)
    set(CUSTOM_PYTHON_EXECUTABLE ${PYTHON_EXECUTABLE})
else()
    find_program(CUSTOM_PYTHON_EXECUTABLE python)
    message("-- Found Python program: ${CUSTOM_PYTHON_EXECUTABLE}")
    execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "import sys; print 'python' + sys.version[0:3]"
                    OUTPUT_VARIABLE PYTHON_LIBRARY_NAME OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "import sys; print sys.exec_prefix"
                    OUTPUT_VARIABLE PYTHON_PREFIX OUTPUT_STRIP_TRAILING_WHITESPACE)
    FIND_LIBRARY(PYTHON_LIBRARIES
                 NAMES ${PYTHON_LIBRARY_NAME}
                 HINTS "${PYTHON_PREFIX}"
                 PATH_SUFFIXES "lib" "libs"
                 NO_DEFAULT_PATH)
    execute_process(COMMAND ${CUSTOM_PYTHON_EXECUTABLE} -c
                    "from distutils.sysconfig import *; print get_python_inc()"
                    OUTPUT_VARIABLE PYTHON_INCLUDE_DIRS OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(PYTHON_LIBRARIES AND PYTHON_INCLUDE_DIRS)
        SET(PYTHONLIBS_FOUND TRUE)
        message("-- Found PythonLibs: " ${PYTHON_LIBRARIES})
        message("-- -- Used custom search path")
    else()
        find_package(PythonLibs REQUIRED)
        message("-- -- Used find_package(PythonLibs)")
    endif()
endif()

find_package(NumPy REQUIRED)
find_package(Boost REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories("${CMAKE_SOURCE_DIR}/include")
include_directories("${CMAKE_SOURCE_DIR}/thirdparty/grpc/include/")
include_directories("${CMAKE_SOURCE_DIR}/thirdparty/grpc/third_party/protobuf/src")
include_directories("${PYTHON_INCLUDE_DIRS}")
include_directories("${NUMPY_INCLUDE_DIR}")
include_directories("/usr/local/include")
include_directories("${Boost_INCLUDE_DIRS}")

set(PROTO_PATH "${CMAKE_SOURCE_DIR}/protos")

set(GRAPH_PROTO "${PROTO_PATH}/graph.proto")
set(RAY_PROTO "${PROTO_PATH}/ray.proto")
set(TYPES_PROTO "${PROTO_PATH}/types.proto")
set(GENERATED_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_PROTOBUF_PATH})

set(GRAPH_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/graph.pb.cc")
set(GRAPH_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/graph.pb.h")

set(RAY_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/ray.pb.cc")
set(RAY_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/ray.pb.h")
set(RAY_GRPC_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/ray.grpc.pb.cc")
set(RAY_GRPC_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/ray.grpc.pb.h")

set(TYPES_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/types.pb.cc")
set(TYPES_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/types.pb.h")

add_custom_command(
    OUTPUT "${GRAPH_PB_H_FILE}"
           "${GRAPH_PB_CPP_FILE}"
    COMMAND ${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/protobuf/protoc
    ARGS "--proto_path=${PROTO_PATH}"
         "--cpp_out=${GENERATED_PROTOBUF_PATH}"
         "${GRAPH_PROTO}"
    )

add_custom_command(
    OUTPUT "${RAY_PB_H_FILE}"
           "${RAY_PB_CPP_FILE}"
           "${RAY_GRPC_PB_H_FILE}"
           "${RAY_GRPC_PB_CPP_FILE}"
    COMMAND ${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/protobuf/protoc
    ARGS "--proto_path=${PROTO_PATH}"
         "--cpp_out=${GENERATED_PROTOBUF_PATH}"
         "${RAY_PROTO}"
    COMMAND ${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/protobuf/protoc
    ARGS "--proto_path=${PROTO_PATH}"
         "--grpc_out=${GENERATED_PROTOBUF_PATH}"
         "--plugin=protoc-gen-grpc=${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/grpc_cpp_plugin"
         "${RAY_PROTO}"
    )

add_custom_command(
    OUTPUT "${TYPES_PB_H_FILE}"
           "${TYPES_PB_CPP_FILE}"
    COMMAND ${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/protobuf/protoc
    ARGS "--proto_path=${PROTO_PATH}"
         "--cpp_out=${GENERATED_PROTOBUF_PATH}"
         "${TYPES_PROTO}"
    )

set(GENERATED_PROTOBUF_FILES
    ${GRAPH_PB_H_FILE} ${GRAPH_PB_CPP_FILE}
    ${RAY_PB_H_FILE} ${RAY_PB_CPP_FILE}
    ${RAY_GRPC_PB_H_FILE} ${RAY_GRPC_PB_CPP_FILE}
    ${TYPES_PB_H_FILE} ${TYPES_PB_CPP_FILE})

include_directories(${GENERATED_PROTOBUF_PATH})

link_libraries(${CMAKE_SOURCE_DIR}/thirdparty/grpc/libs/opt/libgrpc++_unsecure.a
               ${CMAKE_SOURCE_DIR}/thirdparty/grpc/libs/opt/libgrpc++.a
               ${CMAKE_SOURCE_DIR}/thirdparty/grpc/libs/opt/libgrpc.a
               ${CMAKE_SOURCE_DIR}/thirdparty/grpc/libs/opt/protobuf/libprotobuf.a
               pthread)

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

set(ARROW_LIB ${CMAKE_SOURCE_DIR}/thirdparty/arrow-old/cpp/build/release/libarrow.a)

add_definitions(-fPIC)

add_executable(objstore src/objstore.cc src/ipc.cc src/utils.cc ${GENERATED_PROTOBUF_FILES})
add_executable(scheduler src/scheduler.cc src/computation_graph.cc src/utils.cc ${GENERATED_PROTOBUF_FILES})
add_library(raylib SHARED src/raylib.cc src/worker.cc src/ipc.cc src/utils.cc ${GENERATED_PROTOBUF_FILES})
target_link_libraries(raylib ${PYTHON_LIBRARIES})

get_filename_component(PYTHON_SHARED_LIBRARY ${PYTHON_LIBRARIES} NAME)
if(APPLE)
  add_custom_command(TARGET raylib
      POST_BUILD COMMAND
      ${CMAKE_INSTALL_NAME_TOOL} -change ${PYTHON_SHARED_LIBRARY} ${PYTHON_LIBRARIES} libraylib.so)
endif(APPLE)

install(TARGETS objstore scheduler raylib DESTINATION ${CMAKE_SOURCE_DIR}/lib/python/ray)
