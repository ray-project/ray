# === @begin: gcs_table_storage_cpp_lib (original: gcs_table_storage_lib) ===
gcs_table_storage_cpp_lib_dependencies = [
    # Package-managed
    gcs_cpp_lib_dep,
    gcs_in_memory_store_client_cpp_lib_dep,
    observable_store_client_cpp_lib_dep,
    pubsub_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    redis_store_client_cpp_lib_dep,
]

gcs_table_storage_cpp_lib = static_library(
    'gcs_table_storage_cpp_lib',
    ['gcs_table_storage.cc'],
    install: false,
    dependencies: gcs_table_storage_cpp_lib_dependencies
)

gcs_table_storage_cpp_lib_dep = declare_dependency(
    link_with: [gcs_table_storage_cpp_lib],
    dependencies: gcs_table_storage_cpp_lib_dependencies
)
# === @end: gcs_table_storage_cpp_lib (original: gcs_table_storage_lib) ===


## === @begin: gcs_server_cpp_lib (original: gcs_server_lib) ===
#gcs_server_cpp_lib_private_deps = [
#    # Package-managed
#    autoscaler_rpc_cpp_lib_dep,
#    gcs_cpp_lib_dep,
#    gcs_pub_sub_cpp_lib_dep,
#    gcs_service_proto_cpp_grpc_lib_dep,
#    gcs_service_rpc_cpp_lib_dep,
#    gcs_table_storage_cpp_lib_dep,
#    node_manager_rpc_cpp_lib_dep,
#    observable_store_client_cpp_lib_dep,
#    pubsub_cpp_lib_dep,
#    raylet_client_cpp_lib_dep,
#    scheduler_cpp_lib_dep,
#    worker_rpc_cpp_lib_dep,
#    agent_manager_proto_cpp_lib_dep
#]
#
#gcs_server_cpp_lib_public_deps = [
#    # Eugo-managed
#    boost_bimap,
#    absl_container_btree,
#
#    # Eugo-managed, in-tree
#    grpc_health_proto_cpp_dep,
#    grpc_health_proto_cpp_grpc_dep,
#]
#
#gcs_server_cpp_lib = static_library(
#    'gcs_server_cpp_lib',
#    [
#        'gcs_autoscaler_state_manager.cc',
#        'gcs_init_data.cc',
#        'gcs_worker_manager.cc',
#        'gcs_actor_scheduler.cc',
#        'gcs_node_manager.cc',
#        'gcs_resource_manager.cc',
#        'pubsub_handler.cc',
#        'gcs_placement_group_manager.cc',
#        'runtime_env_handler.cc',
#        'gcs_task_manager.cc',
#        'gcs_health_check_manager.cc',
#        'state_util.cc',
#        'gcs_server.cc',
#        'gcs_kv_manager.cc',
#        'store_client_kv.cc',
#        'gcs_job_manager.cc',
#        'gcs_actor_manager.cc',
#        'gcs_table_storage.cc',
#        'gcs_redis_failure_detector.cc',
#        'gcs_placement_group_scheduler.cc',
#        'usage_stats_client.cc'
#    ],
#    install: false,
#    dependencies: [
#        gcs_server_cpp_lib_private_deps,
#        gcs_server_cpp_lib_public_deps
#    ]
#)
#
#gcs_server_cpp_lib_dep = declare_dependency(
#    link_with: [gcs_server_cpp_lib],
#    dependencies: gcs_server_cpp_lib_public_deps
#)
## === @end: gcs_server_cpp_lib (original: gcs_server_lib) ===
#
#
## === @begin: gcs_server ===
#gcs_server = executable(
#    'gcs_server',
#    ['gcs_server_main.cc'],
#    dependencies: [
#        # Package-managed
#        gcs_server_cpp_lib_dep,
#        stats_cpp_lib_dep,
#
#        # Eugo-managed
#        gflags
#    ]
#)
## === @end: gcs_server ===
