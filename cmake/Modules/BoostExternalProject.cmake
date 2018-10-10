# Boost external project
# target:
#  - boost_ep
# defines:
#  - BOOST_ROOT
#  - Boost_INCLUDE_DIR
#  - Boost_SYSTEM_LIBRARY
#  - Boost_FILESYSTEM_LIBRARY

# boost is a stable library in ray, and it supports to find
# the boost pre-built in environment to speed up build process
if (DEFINED ENV{RAY_BOOST_ROOT} AND EXISTS $ENV{RAY_BOOST_ROOT})
  set(Boost_USE_STATIC_LIBS ON)
  set(BOOST_ROOT "$ENV{RAY_BOOST_ROOT}")
  message(STATUS "Find BOOST_ROOT: ${BOOST_ROOT}")
#  find_package(Boost COMPONENTS system filesystem REQUIRED)
  set(Boost_INCLUDE_DIR ${BOOST_ROOT}/include)
  set(Boost_LIBRARY_DIR ${BOOST_ROOT}/lib)
  set(Boost_SYSTEM_LIBRARY ${Boost_LIBRARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_system${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(Boost_FILESYSTEM_LIBRARY ${Boost_LIBRARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_filesystem${CMAKE_STATIC_LIBRARY_SUFFIX})

  add_custom_target(boost_ep)
else()
  set(Boost_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/external/boost-install)

  set(Boost_INCLUDE_DIR ${Boost_INSTALL_PREFIX}/include)
  set(BOOST_ROOT ${Boost_INSTALL_PREFIX})
  set(Boost_LIBRARY_DIR ${Boost_INSTALL_PREFIX}/lib)
  set(Boost_SYSTEM_LIBRARY ${Boost_LIBRARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_system${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(Boost_FILESYSTEM_LIBRARY ${Boost_LIBRARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_filesystem${CMAKE_STATIC_LIBRARY_SUFFIX})

  #set(boost_URL https://github.com/boostorg/boost.git)
  #set(boost_TAG boost-1.65.1)

  set(Boost_TAR_GZ_URL http://dl.bintray.com/boostorg/release/1.65.1/source/boost_1_65_1.tar.gz)
  set(Boost_BUILD_PRODUCTS ${Boost_SYSTEM_LIBRARY} ${Boost_FILESYSTEM_LIBRARY})
  set(Boost_URL_MD5 "ee64fd29a3fe42232c6ac3c419e523cf")

  set(Boost_USE_STATIC_LIBS ON)

  ExternalProject_Add(boost_ep
    PREFIX external/boost
    URL ${Boost_TAR_GZ_URL}
    URL_MD5 ${Boost_URL_MD5}
    #    GIT_REPOSITORY ${boost_URL}
    #    GIT_TAG ${boost_TAG}
    #    GIT_SUBMODULES ""
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS ${Boost_BUILD_PRODUCTS}
    CONFIGURE_COMMAND ./bootstrap.sh
    BUILD_COMMAND bash -c "./b2 cxxflags=-fPIC cflags=-fPIC variant=release link=static --with-filesystem --with-system --with-regex -j8 install --prefix=${Boost_INSTALL_PREFIX} > /dev/null"
    INSTALL_COMMAND "")
endif ()
