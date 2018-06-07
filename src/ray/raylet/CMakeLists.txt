include(${CMAKE_CURRENT_LIST_DIR}/../../common/cmake/Common.cmake)

set(NODE_MANAGER_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/node_manager.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

set(NODE_MANAGER_FBS_OUTPUT_FILES
        "${OUTPUT_DIR}/node_manager_generated.h")

add_custom_command(
        OUTPUT ${NODE_MANAGER_FBS_OUTPUT_FILES}
        # The --gen-object-api flag generates a C++ class MessageT for each
        # flatbuffers message Message, which can be used to store deserialized
        # messages in data structures. This is currently used for ObjectInfo for
        # example.
        COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${NODE_MANAGER_FBS_SRC} --cpp --gen-object-api --gen-mutable --scoped-enums
        DEPENDS ${FBS_DEPENDS}
        COMMENT "Running flatc compiler on ${NODE_MANAGER_FBS_SRC}"
        VERBATIM)

add_custom_target(gen_node_manager_fbs DEPENDS ${NODE_MANAGER_FBS_OUTPUT_FILES})

# Generate Python bindings for the flatbuffers objects.
set(PYTHON_OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/../../../python/ray/core/generated/)
add_custom_command(
    TARGET gen_node_manager_fbs
    COMMAND ${FLATBUFFERS_COMPILER} -p -o ${PYTHON_OUTPUT_DIR} ${NODE_MANAGER_FBS_SRC}
    DEPENDS ${FBS_DEPENDS}
    COMMENT "Running flatc compiler on ${NODE_MANAGER_FBS_SRC}"
    VERBATIM)

add_dependencies(gen_node_manager_fbs flatbuffers_ep)

ADD_RAY_TEST(object_manager_integration_test STATIC_LINK_LIBS ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} gtest gtest_main pthread ${Boost_SYSTEM_LIBRARY})

ADD_RAY_TEST(worker_pool_test STATIC_LINK_LIBS ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} gtest gtest_main gmock_main pthread ${Boost_SYSTEM_LIBRARY})

ADD_RAY_TEST(task_test STATIC_LINK_LIBS ray_static gtest gtest_main gmock_main pthread ${Boost_SYSTEM_LIBRARY})
ADD_RAY_TEST(lineage_cache_test STATIC_LINK_LIBS ray_static gtest gtest_main gmock_main pthread ${Boost_SYSTEM_LIBRARY})
ADD_RAY_TEST(task_dependency_manager_test STATIC_LINK_LIBS ray_static gtest gtest_main gmock_main pthread ${Boost_SYSTEM_LIBRARY})

add_library(rayletlib raylet.cc ${NODE_MANAGER_FBS_OUTPUT_FILES})
target_link_libraries(rayletlib ray_static ${Boost_SYSTEM_LIBRARY})

add_executable(raylet main.cc)
target_link_libraries(raylet rayletlib ${Boost_SYSTEM_LIBRARY} pthread)

add_executable(raylet_monitor monitor_main.cc)
target_link_libraries(raylet_monitor rayletlib ${Boost_SYSTEM_LIBRARY} pthread)

install(FILES
        raylet
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ray/raylet")
