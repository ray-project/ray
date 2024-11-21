#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "tensorpipe_uv" for configuration "Release"
set_property(TARGET tensorpipe_uv APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(tensorpipe_uv PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "C"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libtensorpipe_uv.a"
  )

list(APPEND _cmake_import_check_targets tensorpipe_uv )
list(APPEND _cmake_import_check_files_for_tensorpipe_uv "${_IMPORT_PREFIX}/lib/libtensorpipe_uv.a" )

# Import target "tensorpipe" for configuration "Release"
set_property(TARGET tensorpipe APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(tensorpipe PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libtensorpipe.a"
  )

list(APPEND _cmake_import_check_targets tensorpipe )
list(APPEND _cmake_import_check_files_for_tensorpipe "${_IMPORT_PREFIX}/lib/libtensorpipe.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
