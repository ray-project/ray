# Boost external project
# target:
#  - boost_ep
# defines:
#  - BOOST_ROOT
#  - Boost_INCLUDE_DIR
#  - Boost_SYSTEM_LIBRARY
#  - Boost_FILESYSTEM_LIBRARY

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

set(Boost_USE_STATIC_LIBS ON)

ExternalProject_Add(boost_ep
    PREFIX external/boost
    URL ${Boost_TAR_GZ_URL}
    #    GIT_REPOSITORY ${boost_URL}
    #    GIT_TAG ${boost_TAG}
    #    GIT_SUBMODULES ""
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS ${Boost_BUILD_PRODUCTS}
    CONFIGURE_COMMAND ./bootstrap.sh
    BUILD_COMMAND ./b2 cxxflags=-fPIC cflags=-fPIC variant=release link=static --with-filesystem --with-system --with-regex -j8 install --prefix=${Boost_INSTALL_PREFIX}
    INSTALL_COMMAND ""
    )
