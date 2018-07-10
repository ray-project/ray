include(${CMAKE_CURRENT_LIST_DIR}/../../common/cmake/Common.cmake)

set(OBJECT_MANAGER_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/object_manager.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

set(OBJECT_MANAGER_FBS_OUTPUT_FILES
        "${OUTPUT_DIR}/object_manager_generated.h")

add_custom_command(
        OUTPUT ${OBJECT_MANAGER_FBS_OUTPUT_FILES}
        # The --gen-object-api flag generates a C++ class MessageT for each
        # flatbuffers message Message, which can be used to store deserialized
        # messages in data structures. This is currently used for ObjectInfo for
        # example.
        COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${OBJECT_MANAGER_FBS_SRC} --cpp --gen-object-api --gen-mutable --scoped-enums
        DEPENDS ${FBS_DEPENDS}
        COMMENT "Running flatc compiler on ${OBJECT_MANAGER_FBS_SRC}"
        VERBATIM)

add_custom_target(gen_object_manager_fbs DEPENDS ${OBJECT_MANAGER_FBS_OUTPUT_FILES})

ADD_RAY_TEST(test/object_manager_test STATIC_LINK_LIBS ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} gtest gtest_main pthread ${Boost_SYSTEM_LIBRARY})
ADD_RAY_TEST(test/object_manager_stress_test STATIC_LINK_LIBS ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} gtest gtest_main pthread ${Boost_SYSTEM_LIBRARY})

add_library(object_manager object_manager.cc object_manager.h ${OBJECT_MANAGER_FBS_OUTPUT_FILES})
target_link_libraries(object_manager common ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} ${Boost_SYSTEM_LIBRARY})

install(FILES
        object_manager
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ray/object_manager")
