include(FindCython)
include(UseCython)

include_directories("${PYTHON_INCLUDE_DIRS}")
include_directories("${NUMPY_INCLUDE_DIR}")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../src")
include_directories("${CMAKE_CURRENT_LIST_DIR}")
include_directories("${CMAKE_CURRENT_LIST_DIR}/ray")
include_directories("${ARROW_SOURCE_DIR}/../arrow_ep-build/src")

#get_directory_property(cmake_include_directories DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/ray" INCLUDE_DIRECTORIES)

message("${CMAKE_CURRENT_LIST_DIR}/ray")
include_directories("${CMAKE_CURRENT_LIST_DIR}/ray/includes")
# If the pyx file is a C++ file, we should specify that here.
set_source_files_properties(
  ${CMAKE_CURRENT_LIST_DIR}/ray/libraylet_library_python.pyx
  PROPERTIES CYTHON_IS_CXX TRUE)

set(RAY_SRC_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/../src/ray")

# TODO(suquark): Better ways to find dependencies.
cython_add_module(
  libraylet_library_python
  ${CMAKE_CURRENT_LIST_DIR}/ray/libraylet_library_python.pyx
  ${ARROW_SOURCE_DIR}/cpp/src/plasma/common.cc
  ${RAY_SRC_DIRECTORY}/thirdparty/sha256.c
  ${RAY_SRC_DIRECTORY}/id.cc
  ${RAY_SRC_DIRECTORY}/status.cc
  ${RAY_SRC_DIRECTORY}/util/logging.cc
  ${RAY_SRC_DIRECTORY}/common/common_protocol.cc
  ${RAY_SRC_DIRECTORY}/raylet/scheduling_resources.cc
  ${RAY_SRC_DIRECTORY}/raylet/raylet_client.cc
  ${RAY_SRC_DIRECTORY}/raylet/task_spec.cc)
