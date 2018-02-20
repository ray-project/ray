include(${CMAKE_CURRENT_LIST_DIR}/../../common/cmake/Common.cmake)

set(OM_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/om.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

set(OM_FBS_OUTPUT_FILES
        "${OUTPUT_DIR}/om_generated.h")

add_custom_command(
        OUTPUT ${OM_FBS_OUTPUT_FILES}
        # The --gen-object-api flag generates a C++ class MessageT for each
        # flatbuffers message Message, which can be used to store deserialized
        # messages in data structures. This is currently used for ObjectInfo for
        # example.
        COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${OM_FBS_SRC} --cpp --gen-object-api --gen-mutable
        DEPENDS ${FBS_DEPENDS}
        COMMENT "Running flatc compiler on ${OM_FBS_SRC}"
        VERBATIM)

add_custom_target(gen_om_fbs DEPENDS ${OM_FBS_OUTPUT_FILES})

ADD_RAY_TEST(object_manager_test STATIC_LINK_LIBS common ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} gtest gtest_main pthread ${Boost_SYSTEM_LIBRARY})

add_library(object_manager object_manager.cc object_manager.h object_directory.h object_directory.cc ${OM_FBS_OUTPUT_FILES})
target_link_libraries(object_manager common ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} ${Boost_SYSTEM_LIBRARY})

install(FILES
        object_manager.h
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ray/om")
