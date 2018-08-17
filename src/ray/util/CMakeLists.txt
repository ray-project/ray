install(FILES
  logging.h
  macros.h
  util.h
  visibility.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ray/util"
)

ADD_RAY_TEST(logging_test STATIC_LINK_LIBS ray_static gtest)