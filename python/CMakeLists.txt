# NOTE: These must be checked before including Cython packages to ensure we are using the right python version.
# Segfaults could happen if we are using the wrong version.
set(PYTHON_INCLUDE_DIR ${PYTHON_INCLUDE_DIRS})
set(PYTHON_LIBRARY ${PYTHON_LIBRARIES})

# Find Cython executable
get_filename_component(_python_path ${PYTHON_EXECUTABLE} PATH)
find_program(CYTHON_EXECUTABLE
  NAMES cython cython.bat cython3
  HINTS ${_python_path})

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Cython REQUIRED_VARS CYTHON_EXECUTABLE)

include(UseCython)

include_directories("${NUMPY_INCLUDE_DIR}")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../src")
include_directories("${CMAKE_CURRENT_LIST_DIR}")
# Include some generated Arrow headers.
include_directories("${ARROW_SOURCE_DIR}/../arrow_ep-build/src")

# If the pyx file is a C++ file, we should specify that here.
set_source_files_properties(
  ${CMAKE_CURRENT_LIST_DIR}/ray/_raylet.pyx
  PROPERTIES CYTHON_IS_CXX TRUE)

set(RAY_SRC_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/../src/ray")

# TODO(suquark): Better ways to find dependencies.
cython_add_module(
  _raylet
  ${CMAKE_CURRENT_LIST_DIR}/ray/_raylet.pyx
  ${ARROW_SOURCE_DIR}/cpp/src/plasma/common.cc
  ${RAY_SRC_DIRECTORY}/thirdparty/sha256.c
  ${RAY_SRC_DIRECTORY}/id.cc
  ${RAY_SRC_DIRECTORY}/status.cc
  ${RAY_SRC_DIRECTORY}/util/logging.cc
  ${RAY_SRC_DIRECTORY}/common/common_protocol.cc
  ${RAY_SRC_DIRECTORY}/raylet/scheduling_resources.cc
  ${RAY_SRC_DIRECTORY}/raylet/task.cc
  ${RAY_SRC_DIRECTORY}/raylet/task_spec.cc
  ${RAY_SRC_DIRECTORY}/raylet/task_execution_spec.cc
  ${RAY_SRC_DIRECTORY}/raylet/raylet_client.cc)

# Make sure arrow is downloaded before _raylet is compiled. This is a hack
# that registers ${ARROW_SOURCE_DIR}/cpp/src/plasma/common.cc with CMake.
add_custom_command(OUTPUT ${ARROW_SOURCE_DIR}/cpp/src/plasma/common.cc
                   COMMAND "cat ${ARROW_SOURCE_DIR}/cpp/src/plasma/common.cc" DEPENDS ray_static)

add_dependencies(_raylet
    boost_thread
    arrow_ep
    gen_gcs_fbs
    gen_object_manager_fbs
    gen_node_manager_fbs
    ray_static)

# Make sure that the Python extensions are built before copying the files.
add_dependencies(copy_ray _raylet)
