# IMPORTANT: ordering of targets in this file is dependent on the build graph.
# You will also see things "out of order" from a BUILD.bazel perspective,

# === @begin: function_traits_cpp_lib (@original: function_traits) ===
# Header-only
function_traits_cpp_lib_dependencies = [
    # Eugo-managed
    boost_functional,
]

function_traits_cpp_lib_dep = declare_dependency(
    sources: ['function_traits.h'],
    dependencies: function_traits_cpp_lib_dependencies
)
# === @end: function_traits_cpp_lib ===


# === @begin: postable_cpp_lib (@original: postable) ===
# Header-only
postable_cpp_lib_dependencies = [
    # Package-managed
    function_traits_cpp_lib_dep,
    asio_cpp_lib_dep,
]

postable_cpp_lib_dep = declare_dependency(
    sources: ['postable.h'],
    dependencies: postable_cpp_lib_dependencies
)
# === @end: postable_cpp_lib ===


