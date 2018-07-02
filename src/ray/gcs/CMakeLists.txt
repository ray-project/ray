include(${CMAKE_CURRENT_LIST_DIR}/../../common/cmake/Common.cmake)

set(GCS_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/gcs.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

set(GCS_FBS_OUTPUT_FILES
  "${OUTPUT_DIR}/gcs_generated.h")

add_custom_command(
  OUTPUT ${GCS_FBS_OUTPUT_FILES}
  # The --gen-object-api flag generates a C++ class MessageT for each
  # flatbuffers message Message, which can be used to store deserialized
  # messages in data structures. This is currently used for ObjectInfo for
  # example.
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${GCS_FBS_SRC} --cpp --gen-object-api --gen-mutable --scoped-enums
  DEPENDS ${FBS_DEPENDS}
  COMMENT "Running flatc compiler on ${GCS_FBS_SRC}"
  VERBATIM)

add_custom_target(gen_gcs_fbs DEPENDS ${GCS_FBS_OUTPUT_FILES})

# Generate Python bindings for the flatbuffers objects.
set(PYTHON_OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/../../../python/ray/core/generated/)
add_custom_command(
  TARGET gen_gcs_fbs
  COMMAND ${FLATBUFFERS_COMPILER} -p -o ${PYTHON_OUTPUT_DIR} ${GCS_FBS_SRC}
  DEPENDS ${FBS_DEPENDS}
  COMMENT "Running flatc compiler on ${GCS_FBS_SRC}"
  VERBATIM)

ADD_RAY_TEST(client_test STATIC_LINK_LIBS ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} gtest gtest_main pthread ${Boost_SYSTEM_LIBRARY})
ADD_RAY_TEST(asio_test STATIC_LINK_LIBS ray_static ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} gtest gtest_main pthread ${Boost_SYSTEM_LIBRARY})

install(FILES
  client.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ray/gcs")
