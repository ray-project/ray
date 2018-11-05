# include all ray third party dependencies

# Because we use the old C++ ABI to be compatible with TensorFlow,
# we have to turn it on for dependencies too
set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")

# The following is needed because in CentOS, the lib directory is named lib64
if(EXISTS "/etc/redhat-release" AND CMAKE_SIZEOF_VOID_P EQUAL 8)
  set(LIB_SUFFIX 64)
endif()

if(RAY_BUILD_TESTS OR RAY_BUILD_BENCHMARKS)
  add_custom_target(unittest ctest -L unittest)

  include(GtestExternalProject)
  message(STATUS "GTest include dir: ${GTEST_INCLUDE_DIR}")
  message(STATUS "GTest static library: ${GTEST_STATIC_LIB}")
  message(STATUS "GTest static main library: ${GTEST_MAIN_STATIC_LIB}")
  message(STATUS "GMock static main library: ${GMOCK_MAIN_STATIC_LIB}")
  include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(gtest
    STATIC_LIB ${GTEST_STATIC_LIB})
  ADD_THIRDPARTY_LIB(gtest_main
    STATIC_LIB ${GTEST_MAIN_STATIC_LIB})
  ADD_THIRDPARTY_LIB(gmock_main
    STATIC_LIB ${GMOCK_MAIN_STATIC_LIB})

  add_dependencies(gtest googletest_ep)
  add_dependencies(gtest_main googletest_ep)
  add_dependencies(gmock_main googletest_ep)
endif()

include(GlogExternalProject)
message(STATUS "Glog home: ${GLOG_HOME}")
message(STATUS "Glog include dir: ${GLOG_INCLUDE_DIR}")
message(STATUS "Glog static lib: ${GLOG_STATIC_LIB}")

include_directories(${GLOG_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(glog
  STATIC_LIB ${GLOG_STATIC_LIB})

add_dependencies(glog glog_ep)

# boost
include(BoostExternalProject)

message(STATUS "Boost root: ${BOOST_ROOT}")
message(STATUS "Boost include dir: ${Boost_INCLUDE_DIR}")
message(STATUS "Boost system library: ${Boost_SYSTEM_LIBRARY}")
message(STATUS "Boost filesystem library: ${Boost_FILESYSTEM_LIBRARY}")
include_directories(${Boost_INCLUDE_DIR})

ADD_THIRDPARTY_LIB(boost_system
  STATIC_LIB ${Boost_SYSTEM_LIBRARY})
ADD_THIRDPARTY_LIB(boost_filesystem
  STATIC_LIB ${Boost_FILESYSTEM_LIBRARY})

add_dependencies(boost_system boost_ep)
add_dependencies(boost_filesystem boost_ep)

add_custom_target(boost DEPENDS boost_system boost_filesystem)

# flatbuffers
include(FlatBuffersExternalProject)

message(STATUS "Flatbuffers home: ${FLATBUFFERS_HOME}")
message(STATUS "Flatbuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")
message(STATUS "Flatbuffers static library: ${FLATBUFFERS_STATIC_LIB}")
message(STATUS "Flatbuffers compiler: ${FLATBUFFERS_COMPILER}")
include_directories(SYSTEM ${FLATBUFFERS_INCLUDE_DIR})

ADD_THIRDPARTY_LIB(flatbuffers STATIC_LIB ${FLATBUFFERS_STATIC_LIB})

add_dependencies(flatbuffers flatbuffers_ep)

# Apache Arrow, use FLATBUFFERS_HOME and BOOST_ROOT
include(ArrowExternalProject)

message(STATUS "Arrow home: ${ARROW_HOME}")
message(STATUS "Arrow source dir: ${ARROW_SOURCE_DIR}")
message(STATUS "Arrow include dir: ${ARROW_INCLUDE_DIR}")
message(STATUS "Arrow static library: ${ARROW_STATIC_LIB}")
message(STATUS "Arrow shared library: ${ARROW_SHARED_LIB}")
include_directories(SYSTEM ${ARROW_INCLUDE_DIR})

ADD_THIRDPARTY_LIB(arrow STATIC_LIB ${ARROW_STATIC_LIB})

add_dependencies(arrow arrow_ep)

# Plasma, it is already built in arrow
message(STATUS "Plasma include dir: ${PLASMA_INCLUDE_DIR}")
message(STATUS "Plasma static library: ${PLASMA_STATIC_LIB}")
message(STATUS "Plasma shared library: ${PLASMA_SHARED_LIB}")
include_directories(SYSTEM ${PLASMA_INCLUDE_DIR})

ADD_THIRDPARTY_LIB(plasma STATIC_LIB ${PLASMA_STATIC_LIB})

add_dependencies(plasma plasma_ep)

if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
  # clean the arrow_ep/python/build/lib.xxxxx directory,
  # or when you build with another python version, it creates multiple lib.xxxx directories
  set_property(DIRECTORY APPEND PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "${ARROW_SOURCE_DIR}/python/build/")
  set_property(DIRECTORY APPEND PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "${CMAKE_SOURCE_DIR}/python/ray/pyarrow_files/pyarrow")

  # here we use externalProject to process pyarrow building
  # add_custom_command would have problem with setup.py
  if(EXISTS ${ARROW_SOURCE_DIR}/python/build/)
    # if we did not run `make clean`, skip the rebuild of pyarrow
    add_custom_target(pyarrow_ext)
  else()
    # pyarrow
    find_package(PythonInterp REQUIRED)
    message(STATUS "PYTHON_EXECUTABLE for pyarrow: ${PYTHON_EXECUTABLE}")

    # PYARROW_PARALLEL= , so it will add -j to pyarrow build
    set(pyarrow_ENV
      "PKG_CONFIG_PATH=${ARROW_LIBRARY_DIR}/pkgconfig"
      "PYARROW_WITH_PLASMA=1"
      "PYARROW_WITH_TENSORFLOW=1"
      "PYARROW_BUNDLE_ARROW_CPP=1"
      "PARQUET_HOME=${PARQUET_HOME}"
      "PYARROW_WITH_PARQUET=1"
      "PYARROW_PARALLEL=")

    if (APPLE)
      # Since 10.14, the XCode toolchain only accepts libc++ as the
      # standard library. This should also work on macOS starting from 10.9.
      set(pyarrow_ENV ${pyarrow_ENV} "CXXFLAGS='-stdlib=libc++'")
      set(pyarrow_ENV ${pyarrow_ENV} "MACOSX_DEPLOYMENT_TARGET=10.7")
    endif()

    ExternalProject_Add(pyarrow_ext
      PREFIX external/pyarrow
      DEPENDS arrow_ep
      DOWNLOAD_COMMAND ""
      BUILD_IN_SOURCE 1
      CONFIGURE_COMMAND cd ${ARROW_SOURCE_DIR}/python && ${CMAKE_COMMAND} -E env ${pyarrow_ENV} ${PYTHON_EXECUTABLE} setup.py build
      BUILD_COMMAND cd ${ARROW_SOURCE_DIR}/python && ${CMAKE_COMMAND} -E env ${pyarrow_ENV} ${PYTHON_EXECUTABLE} setup.py build_ext
      INSTALL_COMMAND bash -c "cp -rf \$(find ${ARROW_SOURCE_DIR}/python/build/ -maxdepth 1 -type d -print | grep -m1 'lib')/pyarrow ${CMAKE_SOURCE_DIR}/python/ray/pyarrow_files/")

  endif()

endif ()
