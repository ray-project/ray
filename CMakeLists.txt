cmake_minimum_required(VERSION 2.8)

project(orchestra)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

find_package(Protobuf REQUIRED)
find_package(PythonInterp REQUIRED)
find_package(PythonLibs REQUIRED)
find_package(NumPy REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories("${CMAKE_SOURCE_DIR}/include")
include_directories("/usr/local/include")
include_directories("${PYTHON_INCLUDE_DIRS}")
include_directories("${NUMPY_INCLUDE_DIR}")

set(PROTO_PATH "${CMAKE_SOURCE_DIR}/protos")

set(ORCHESTRA_PROTO "${PROTO_PATH}/orchestra.proto")
set(TYPES_PROTO "${PROTO_PATH}/types.proto")
set(GENERATED_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_PROTOBUF_PATH})

set(ORCHESTRA_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/orchestra.pb.cc")
set(ORCHESTRA_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/orchestra.pb.h")
set(ORCHESTRA_GRPC_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/orchestra.grpc.pb.cc")
set(ORCHESTRA_GRPC_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/orchestra.grpc.pb.h")

set(TYPES_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/types.pb.cc")
set(TYPES_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/types.pb.h")
set(TYPES_GRPC_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/types.grpc.pb.cc")
set(TYPES_GRPC_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/types.grpc.pb.h")

add_custom_command(
    OUTPUT "${ORCHESTRA_PB_H_FILE}"
           "${ORCHESTRA_PB_CPP_FILE}"
           "${ORCHESTRA_GRPC_PB_H_FILE}"
           "${ORCHESTRA_GRPC_PB_CPP_FILE}"
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
    ARGS "--proto_path=${PROTO_PATH}"
         "--cpp_out=${GENERATED_PROTOBUF_PATH}"
         "${ORCHESTRA_PROTO}"
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
    ARGS "--proto_path=${PROTO_PATH}"
         "--grpc_out=${GENERATED_PROTOBUF_PATH}"
         "--plugin=protoc-gen-grpc=/usr/local/bin/grpc_cpp_plugin"
         "${ORCHESTRA_PROTO}"
    )

add_custom_command(
    OUTPUT "${TYPES_PB_H_FILE}"
           "${TYPES_PB_CPP_FILE}"
           "${TYPES_GRPC_PB_H_FILE}"
           "${TYPES_GRPC_PB_CPP_FILE}"
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
    ARGS "--proto_path=${PROTO_PATH}"
         "--cpp_out=${GENERATED_PROTOBUF_PATH}"
         "${TYPES_PROTO}"
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
    ARGS "--proto_path=${PROTO_PATH}"
         "--grpc_out=${GENERATED_PROTOBUF_PATH}"
         "--plugin=protoc-gen-grpc=/usr/local/bin/grpc_cpp_plugin"
         "${TYPES_PROTO}"
    )

set(GENERATED_PROTOBUF_FILES ${ORCHESTRA_PB_H_FILE} ${ORCHESTRA_PB_CPP_FILE}
    ${ORCHESTRA_GRPC_PB_H_FILE} ${ORCHESTRA_GRPC_PB_CPP_FILE}
    ${TYPES_PB_H_FILE} ${TYPES_PB_CPP_FILE}
    ${TYPES_GRPC_PB_H_FILE} ${TYPES_GRPC_PB_CPP_FILE})

include_directories(${GENERATED_PROTOBUF_PATH})
link_libraries(grpc++_unsecure grpc pthread ${PROTOBUF_LIBRARY})
if (UNIX AND NOT APPLE)
   link_libraries(rt)
endif()

add_executable(objstore src/objstore.cc src/ipc.cc ${GENERATED_PROTOBUF_FILES})
target_link_libraries(objstore arrow)
add_executable(scheduler src/scheduler.cc ${GENERATED_PROTOBUF_FILES})
add_library(orchpylib SHARED src/orchpylib.cc src/worker.cc src/ipc.cc src/serialize.cc ${GENERATED_PROTOBUF_FILES})
target_link_libraries(orchpylib arrow)

install(TARGETS objstore scheduler orchpylib DESTINATION ${CMAKE_SOURCE_DIR}/lib/orchpy/orchpy)
