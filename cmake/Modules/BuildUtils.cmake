function(ADD_THIRDPARTY_LIB LIB_NAME)
  set(options)
  set(one_value_args SHARED_LIB STATIC_LIB)
  set(multi_value_args DEPS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(ARG_STATIC_LIB AND ARG_SHARED_LIB)
    if(NOT ARG_STATIC_LIB)
      message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
    endif()

    SET(AUG_LIB_NAME "${LIB_NAME}_static")
    add_library(${AUG_LIB_NAME} STATIC IMPORTED)
    set_target_properties(${AUG_LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")

    SET(AUG_LIB_NAME "${LIB_NAME}_shared")
    add_library(${AUG_LIB_NAME} SHARED IMPORTED)

    if(MSVC)
        # Mark the ”.lib” location as part of a Windows DLL
        set_target_properties(${AUG_LIB_NAME}
            PROPERTIES IMPORTED_IMPLIB "${ARG_SHARED_LIB}")
    else()
        set_target_properties(${AUG_LIB_NAME}
            PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    endif()
    if(ARG_DEPS)
      set_target_properties(${AUG_LIB_NAME}
        PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
    endif()
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
  elseif(ARG_STATIC_LIB)
    add_library(${LIB_NAME} STATIC IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    SET(AUG_LIB_NAME "${LIB_NAME}_static")
    add_library(${AUG_LIB_NAME} STATIC IMPORTED)
    set_target_properties(${AUG_LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    if(ARG_DEPS)
      set_target_properties(${AUG_LIB_NAME}
        PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
    endif()
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")
  elseif(ARG_SHARED_LIB)
    add_library(${LIB_NAME} SHARED IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    SET(AUG_LIB_NAME "${LIB_NAME}_shared")
    add_library(${AUG_LIB_NAME} SHARED IMPORTED)

    if(MSVC)
        # Mark the ”.lib” location as part of a Windows DLL
        set_target_properties(${AUG_LIB_NAME}
            PROPERTIES IMPORTED_IMPLIB "${ARG_SHARED_LIB}")
    else()
        set_target_properties(${AUG_LIB_NAME}
            PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    endif()
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
    if(ARG_DEPS)
      set_target_properties(${AUG_LIB_NAME}
        PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
    endif()
  else()
    message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
  endif()
endfunction()

function(ADD_RAY_LIB LIB_NAME)
  set(options)
  set(one_value_args SHARED_LINK_FLAGS)
  set(multi_value_args SOURCES STATIC_LINK_LIBS STATIC_PRIVATE_LINK_LIBS SHARED_LINK_LIBS SHARED_PRIVATE_LINK_LIBS DEPENDENCIES)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  add_library(${LIB_NAME}_objlib OBJECT
    ${ARG_SOURCES}
  )

  if (ARG_DEPENDENCIES)
    add_dependencies(${LIB_NAME}_objlib ${ARG_DEPENDENCIES})
  endif()

  # Necessary to make static linking into other shared libraries work properly
  set_property(TARGET ${LIB_NAME}_objlib PROPERTY POSITION_INDEPENDENT_CODE 1)

  set(RUNTIME_INSTALL_DIR bin)

  if (RAY_BUILD_SHARED)
    add_library(${LIB_NAME}_shared SHARED $<TARGET_OBJECTS:${LIB_NAME}_objlib>)

    if(APPLE)
      # On OS X, you can avoid linking at library load time and instead
      # expecting that the symbols have been loaded separately. This happens
      # with libpython* where there can be conflicts between system Python and
      # the Python from a thirdparty distribution
      set(ARG_SHARED_LINK_FLAGS
        "-undefined dynamic_lookup ${ARG_SHARED_LINK_FLAGS}")
    endif()

    set_target_properties(${LIB_NAME}_shared
      PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      RUNTIME_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      PDB_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      LINK_FLAGS "${ARG_SHARED_LINK_FLAGS}"
      OUTPUT_NAME ${LIB_NAME}
      VERSION "${RAY_ABI_VERSION}"
      SOVERSION "${RAY_SO_VERSION}")

    target_link_libraries(${LIB_NAME}_shared
      LINK_PUBLIC ${ARG_SHARED_LINK_LIBS}
      LINK_PRIVATE ${ARG_SHARED_PRIVATE_LINK_LIBS})

    if (RAY_RPATH_ORIGIN)
        if (APPLE)
            set(_lib_install_rpath "@loader_path")
        else()
            set(_lib_install_rpath "\$ORIGIN")
        endif()
        set_target_properties(${LIB_NAME}_shared PROPERTIES
            INSTALL_RPATH ${_lib_install_rpath})
    endif()

    install(TARGETS ${LIB_NAME}_shared
      RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
  endif()

  if (RAY_BUILD_STATIC)
      if (MSVC)
        set(LIB_NAME_STATIC ${LIB_NAME}_static)
      else()
        set(LIB_NAME_STATIC ${LIB_NAME})
      endif()
      add_library(${LIB_NAME}_static STATIC $<TARGET_OBJECTS:${LIB_NAME}_objlib>)
    set_target_properties(${LIB_NAME}_static
      PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      OUTPUT_NAME ${LIB_NAME_STATIC})

  target_link_libraries(${LIB_NAME}_static
      LINK_PUBLIC ${ARG_STATIC_LINK_LIBS}
      LINK_PRIVATE ${ARG_STATIC_PRIVATE_LINK_LIBS})

  install(TARGETS ${LIB_NAME}_static
      RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
  endif()

  if (APPLE)
      set_target_properties(${LIB_NAME}_shared
      PROPERTIES
      BUILD_WITH_INSTALL_RPATH ON
      INSTALL_NAME_DIR "@rpath")
  endif()

endfunction()

############################################################
# Testing
############################################################
# Add a new test case, with or without an executable that should be built.
#
# REL_TEST_NAME is the name of the test. It may be a single component
# (e.g. monotime-test) or contain additional components (e.g.
# net/net_util-test). Either way, the last component must be a globally
# unique name.
#
# The unit test is added with a label of "unittest" to support filtering with
# ctest.
#
# Arguments after the test name will be passed to set_tests_properties().
function(ADD_RAY_TEST REL_TEST_NAME)
  set(options NO_VALGRIND)
  set(single_value_args)
  set(multi_value_args STATIC_LINK_LIBS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(NO_TESTS OR NOT RAY_BUILD_STATIC)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")

    if (ARG_STATIC_LINK_LIBS)
      # Customize link libraries
      target_link_libraries(${TEST_NAME} ${ARG_STATIC_LINK_LIBS})
    else()
      target_link_libraries(${TEST_NAME} ${RAY_TEST_LINK_LIBS})
    endif()
    add_dependencies(unittest ${TEST_NAME})
  else()
    # No executable, just invoke the test (probably a script) directly.
    set(TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME})
  endif()

  if (RAY_TEST_MEMCHECK AND NOT ARG_NO_VALGRIND)
    SET_PROPERTY(TARGET ${TEST_NAME}
      APPEND_STRING PROPERTY
      COMPILE_FLAGS " -RAY_VALGRIND")
    add_test(${TEST_NAME}
      bash -c "cd ${EXECUTABLE_OUTPUT_PATH}; valgrind --tool=memcheck --leak-check=full --leak-check-heuristics=stdstring --error-exitcode=1 ${TEST_PATH}")
  elseif(MSVC)
    add_test(${TEST_NAME} ${TEST_PATH})
  else()
    add_test(${TEST_NAME}
      ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} test ${TEST_PATH})
  endif()
  set_tests_properties(${TEST_NAME} PROPERTIES LABELS "unittest")
endfunction()

# A wrapper for target_link_libraries() that is compatible with NO_TESTS.
function(RAY_TEST_LINK_LIBRARIES REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  target_link_libraries(${TEST_NAME} ${ARGN})
endfunction()
