cmake_minimum_required(VERSION 3.2)
set(CMAKE_CXX_FLAGS "-g -Wall -Werror -std=c++11")

add_subdirectory(util)
add_subdirectory(gcs)

set(RAY_SRCS
  id.cc
  status.cc
  gcs/client.cc
)

install(FILES
  api.h
  id.h
  status.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ray")

# pkg-config support
configure_file(ray.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/ray.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/ray.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

ADD_RAY_LIB(ray
    SOURCES ${RAY_SRCS}
    SHARED_LINK_LIBS
    STATIC_LINK_LIBS)
