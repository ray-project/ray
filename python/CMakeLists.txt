# NOTE: These must be checked before including Cython packages to ensure we are using the right python version.
# Segfaults could happen if we are using the wrong version.
set(PYTHON_INCLUDE_DIR ${PYTHON_INCLUDE_DIRS})

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

cython_add_module(
  _raylet
  ${RAY_SRC_DIRECTORY}/raylet/raylet_client.cc
  ${CMAKE_CURRENT_LIST_DIR}/ray/_raylet.pyx)

add_dependencies(_raylet ray_static)

if(APPLE)
    target_link_libraries(_raylet "-undefined dynamic_lookup" ray_static)
else()
    target_link_libraries(_raylet ray_static)
endif()

# Make sure that the Python extensions are built before copying the files.
add_dependencies(copy_ray _raylet)
