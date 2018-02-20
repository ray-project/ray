include(${CMAKE_CURRENT_LIST_DIR}/../../common/cmake/Common.cmake)

set(NM_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/nm.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

set(NM_FBS_OUTPUT_FILES
        "${OUTPUT_DIR}/nm_generated.h")

add_custom_command(
        OUTPUT ${NM_FBS_OUTPUT_FILES}
        # The --gen-object-api flag generates a C++ class MessageT for each
        # flatbuffers message Message, which can be used to store deserialized
        # messages in data structures. This is currently used for ObjectInfo for
        # example.
        COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${NM_FBS_SRC} --cpp --gen-object-api --gen-mutable
        DEPENDS ${FBS_DEPENDS}
        COMMENT "Running flatc compiler on ${NM_FBS_SRC}"
        VERBATIM)

add_custom_target(gen_nm_fbs DEPENDS ${NM_FBS_OUTPUT_FILES})

ADD_RAY_TEST(node_manager_test STATIC_LINK_LIBS ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} gtest gtest_main pthread ${Boost_SYSTEM_LIBRARY})
add_executable(node_manager node_manager.cc ${NM_FBS_OUTPUT_FILES})
target_link_libraries(node_manager ray_static ${Boost_SYSTEM_LIBRARY})

install(TARGETS
        node_manager
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ray/raylet")
