cmake_minimum_required(VERSION 2.8)

project(common)

include(ExternalProject)

set(FLATCC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/flatcc-prefix/src/flatcc/")

ExternalProject_Add(flatcc
    URL "https://github.com/dvidelabs/flatcc/archive/v0.4.0.tar.gz"
    INSTALL_COMMAND "")

set(FLATBUFFERS_INCLUDE_DIR "${FLATCC_PREFIX}/include")
set(FLATBUFFERS_STATIC_LIB "${FLATCC_PREFIX}/lib/libflatcc.a")
set(FLATBUFFERS_COMPILER "${FLATCC_PREFIX}/bin/flatcc")
