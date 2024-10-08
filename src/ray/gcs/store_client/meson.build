# === @begin: observable_store_client_cpp_lib (original: observable_store_client) ===
observable_store_client_cpp_lib = static_library(
    'observable_store_client_cpp_lib',
    ['observable_store_client.cc'],
    install: false,
    dependencies: [
        # Package-managed
        ray_common_cpp_lib_dep,
        util_cpp_lib_dep
    ]
)

observable_store_client_cpp_lib_dep = declare_dependency(
    link_with: [observable_store_client_cpp_lib]
)
# === @end: observable_store_client_cpp_lib (original: observable_store_client) ===
