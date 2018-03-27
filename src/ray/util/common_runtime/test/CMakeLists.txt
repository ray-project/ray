SET(CMAKE_CXX_STANDARD 11)

find_package(GTest)
include_directories(${GTEST_INCLUDE_DIR})
link_directories(${GTEST_LIBS})

include_directories(../)

# workaround for the bug that linking against gtest library cannot find pthread symbols
# https://bugs.launchpad.net/ubuntu/+source/gcc-defaults/+bug/1228201
set(LD_FLAGS "${LD_FLAGS} -Wl,--no-as-needed")

add_executable(runtime_test
        configuration_test.cpp
        logging_test.cpp
)

target_link_libraries(runtime_test common.runtime gtest gtest_main pthread)