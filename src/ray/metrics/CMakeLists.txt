ADD_RAY_TEST(registry/any_ptr_test STATIC_LINK_LIBS gtest gtest_main pthread)

install(FILES
        metrics
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ray/metrics")
