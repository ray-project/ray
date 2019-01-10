include(FindCython)
include(UseCython)

include_directories("${CMAKE_CURRENT_LIST_DIR}/../src")
include_directories("${CMAKE_CURRENT_LIST_DIR}")

include_directories(SYSTEM "${CMAKE_CURRENT_LIST_DIR}/ray")

#get_directory_property(cmake_include_directories DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/ray" INCLUDE_DIRECTORIES)

message("${CMAKE_CURRENT_LIST_DIR}/ray")
include_directories("${CMAKE_CURRENT_LIST_DIR}/ray/includes")
# If the pyx file is a C++ file, we should specify that here.
set_source_files_properties(
  ${CMAKE_CURRENT_LIST_DIR}/ray/raylet_client.pyx
  PROPERTIES CYTHON_IS_CXX TRUE )

# Multi-file cython modules do not appear to be working at the moment.
cython_add_module(py_raylet_client ${CMAKE_CURRENT_LIST_DIR}/ray/raylet_client.pyx ${CMAKE_CURRENT_LIST_DIR}/../src/ray/raylet/raylet_client.cc)
