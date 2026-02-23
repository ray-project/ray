# @IMPORTANT: Most GCS targets (gcs_table_storage, gcs_node_manager, gcs_server_lib,
# gcs_server binary, and ~20 others) are NOT defined in this file. They are inlined
# directly in `src/ray/meson.build` (after `subdir('core_worker_rpc_client/')`) because
# those targets depend on:
#   - `gcs_rpc_client/` (processed after gcs/)
#   - `raylet/scheduling/` (processed after gcs/)
#   - `core_worker_rpc_client/` (processed after gcs/)
#   - `observability/` (processed after gcs/)
# Meson requires define-before-use, so they cannot live here.
#
# This file only defines:
#   1. `subdir()` calls for subdirectories whose targets CAN be resolved at this point
#      (`postable/`, `store_client/`).
#   2. A placeholder note for `actor/`, which is also inlined in `src/ray/meson.build`.
#
# NOTE: The old Bazel `gcs` and `redis_client` targets no longer exist upstream. Their
# `.cc` files (`asio.cc`, `redis_async_context.cc`, `redis_client.cc`, `redis_context.cc`)
# were moved into `store_client/` during the upstream restructure. The virtual umbrella
# deps that used to forward them (`gcs_cpp_lib_dep`, `redis_client_cpp_lib_dep`) have
# been removed — hiredis/hiredis_ssl and all other transitive deps are now provided
# transitively by `redis_store_client_cpp_lib_dep` (via `gcs_server_cpp_lib_dep`).

# === @begin: postable/ ===
subdir('postable/')
# === @end: postable/ ===


# === @begin: store_client/ ===
subdir('store_client/')
# === @end: store_client/ ===


# === @begin: actor/ ===
# @IMPORTANT: `actor/` moved to `src/ray/meson.build` (after `core_worker_rpc_client/`)
# because gcs_actor depends on worker_rpc_cpp_lib_dep which is defined in
# core_worker_rpc_client/, processed after gcs/.
# === @end: actor/ ===
