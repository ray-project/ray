cmake_minimum_required(VERSION 2.8)

project(halo)

set(THIRDPARTY_DIR "${CMAKE_SOURCE_DIR}/thirdparty")

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

set(CMAKE_PREFIX_PATH "${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/" ${CMAKE_PREFIX_PATH})

find_package(PythonInterp REQUIRED)
find_package(PythonLibs REQUIRED)
find_package(NumPy REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories("${CMAKE_SOURCE_DIR}/include")
include_directories("/usr/local/include")
include_directories("${CMAKE_SOURCE_DIR}/thirdparty/grpc/include/")
include_directories("${CMAKE_SOURCE_DIR}/thirdparty/grpc/third_party/protobuf/src")
include_directories("${PYTHON_INCLUDE_DIRS}")
include_directories("${NUMPY_INCLUDE_DIR}")

set(PROTO_PATH "${CMAKE_SOURCE_DIR}/protos")

set(HALO_PROTO "${PROTO_PATH}/halo.proto")
set(TYPES_PROTO "${PROTO_PATH}/types.proto")
set(GENERATED_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_PROTOBUF_PATH})

set(HALO_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/halo.pb.cc")
set(HALO_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/halo.pb.h")
set(HALO_GRPC_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/halo.grpc.pb.cc")
set(HALO_GRPC_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/halo.grpc.pb.h")

set(TYPES_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/types.pb.cc")
set(TYPES_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/types.pb.h")
set(TYPES_GRPC_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/types.grpc.pb.cc")
set(TYPES_GRPC_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/types.grpc.pb.h")

add_custom_command(
    OUTPUT "${HALO_PB_H_FILE}"
           "${HALO_PB_CPP_FILE}"
           "${HALO_GRPC_PB_H_FILE}"
           "${HALO_GRPC_PB_CPP_FILE}"
    COMMAND ${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/protobuf/protoc
    ARGS "--proto_path=${PROTO_PATH}"
         "--cpp_out=${GENERATED_PROTOBUF_PATH}"
         "${HALO_PROTO}"
    COMMAND ${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/protobuf/protoc
    ARGS "--proto_path=${PROTO_PATH}"
         "--grpc_out=${GENERATED_PROTOBUF_PATH}"
         "--plugin=protoc-gen-grpc=${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/grpc_cpp_plugin"
         "${HALO_PROTO}"
    )

add_custom_command(
    OUTPUT "${TYPES_PB_H_FILE}"
           "${TYPES_PB_CPP_FILE}"
           "${TYPES_GRPC_PB_H_FILE}"
           "${TYPES_GRPC_PB_CPP_FILE}"
    COMMAND ${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/protobuf/protoc
    ARGS "--proto_path=${PROTO_PATH}"
         "--cpp_out=${GENERATED_PROTOBUF_PATH}"
         "${TYPES_PROTO}"
    COMMAND ${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/protobuf/protoc
    ARGS "--proto_path=${PROTO_PATH}"
         "--grpc_out=${GENERATED_PROTOBUF_PATH}"
         "--plugin=protoc-gen-grpc=${CMAKE_SOURCE_DIR}/thirdparty/grpc/bins/opt/grpc_cpp_plugin"
         "${TYPES_PROTO}"
    )

set(GENERATED_PROTOBUF_FILES ${HALO_PB_H_FILE} ${HALO_PB_CPP_FILE}
    ${HALO_GRPC_PB_H_FILE} ${HALO_GRPC_PB_CPP_FILE}
    ${TYPES_PB_H_FILE} ${TYPES_PB_CPP_FILE}
    ${TYPES_GRPC_PB_H_FILE} ${TYPES_GRPC_PB_CPP_FILE})

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

set(ARROW_LIB ${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/build/release/libarrow.a)

add_definitions(-fPIC)
include_directories("${CMAKE_SOURCE_DIR}/thirdparty/arrow/cpp/src/")
include_directories("${CMAKE_SOURCE_DIR}/thirdparty/numbuf/cpp/src/")
include_directories("${CMAKE_SOURCE_DIR}/thirdparty/numbuf/python/src/")
add_library(pynumbuf STATIC ${CMAKE_SOURCE_DIR}/thirdparty/numbuf/cpp/src/numbuf/tensor.cc
                            ${CMAKE_SOURCE_DIR}/thirdparty/numbuf/cpp/src/numbuf/types.cc
                            ${CMAKE_SOURCE_DIR}/thirdparty/numbuf/cpp/src/numbuf/metadata.cc
                            ${CMAKE_SOURCE_DIR}/thirdparty/numbuf/cpp/src/numbuf/dict.cc
                            ${CMAKE_SOURCE_DIR}/thirdparty/numbuf/python/src/pynumbuf/serialize.cc
                            ${CMAKE_SOURCE_DIR}/thirdparty/numbuf/python/src/pynumbuf/numbuf.cc
                            ${CMAKE_SOURCE_DIR}/thirdparty/numbuf/python/src/pynumbuf/adapters/numpy.cc
                            ${CMAKE_SOURCE_DIR}/thirdparty/numbuf/python/src/pynumbuf/adapters/python.cc)
target_link_libraries(pynumbuf ${ARROW_LIB} ${PYTHON_LIBRARIES})

add_executable(objstore src/objstore.cc src/ipc.cc ${GENERATED_PROTOBUF_FILES})
target_link_libraries(objstore ${ARROW_LIB} pynumbuf)
add_executable(scheduler src/scheduler.cc src/computation_graph.cc ${GENERATED_PROTOBUF_FILES})
add_library(halolib SHARED src/halolib.cc src/worker.cc src/ipc.cc ${GENERATED_PROTOBUF_FILES})
target_link_libraries(halolib ${ARROW_LIB} pynumbuf)

install(TARGETS objstore scheduler halolib DESTINATION ${CMAKE_SOURCE_DIR}/lib/python/halo)
