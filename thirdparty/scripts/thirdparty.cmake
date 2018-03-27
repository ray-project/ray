
# add all thirdparty related path definitions here

list(APPEND CMAKE_MODULE_PATH
            ${CMAKE_CURRENT_LIST_DIR}/../build/arrow/python/cmake_modules)

set(GTest_HOME ${CMAKE_CURRENT_LIST_DIR}/../pkg/gtest)

message (WARNING ${CMAKE_MODULE_PATH})
