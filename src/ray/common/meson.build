# === @begin: constants (originial: constants) ===
constants_cpp_lib_dep = declare_dependency(
    include_directories: ['.']
)
# === @end: constants (original: constants) ===


# === @begin: status (original: status) ===
status_cpp_lib = static_library(
    'status_cpp_lib',
    ['status.cc'],
    install: false,
    dependencies: [
        util_cpp_lib_dep
    ]
)

status_cpp_lib_dep = declare_dependency(
    link_with: [status_cpp_lib]
)
# === @end: status (original: status) ===


# === @begin: id (original: id) ===
id_cpp_lib = static_library(
    'id_cpp_lib',
    [
        'id.cc',
        'common_protocol.cc'
    ],
    install: false,
    dependencies: [
        # Project-managed
        constants_cpp_lib_dep,
        status_cpp_lib_dep,
        common_proto_cpp_lib_dep,
        gcs_proto_cpp_lib_dep,
        util_cpp_lib_dep,

        # Eugo-managed
        flatbuffers,
        msgpack_cxx
    ]
)

id_cpp_lib_dep = declare_dependency(
    link_with: [id_cpp_lib]
)
# === @end: id (original: id) ===


# === @begin: ray_object (original: c) ===
ray_object_cpp_lib = static_library(
    'ray_object_cpp_lib',
    ['ray_object.cc'],
    install: false,
    dependencies: [
        # Package-managed
        id_cpp_lib_dep,
        status_cpp_lib_dep,
        aligned_alloc_cpp_lib_dep,
        gcs_proto_cpp_lib_dep,
        util_cpp_lib_dep,

        # Eugo-managed
        absl_time,
        absl_types_optional
    ]
)

ray_object_cpp_lib_dep = declare_dependency(
    link_with: [ray_object_cpp_lib]
)
# === @end: ray_object (original: c) ===


# === @begin: ray_config (original: ray_config) ===
ray_config_cpp_lib = static_library(
    'ray_config_cpp_lib',
    ['ray_config.cc'],
    install: false,
    dependencies: [
        # Package-managed
        util_cpp_lib_dep,

        # Eugo-managed
        absl_algorithm,
        absl_strings,
        nlohmann_json
    ]
)

ray_config_cpp_lib_dep = declare_dependency(
    link_with: [ray_config_cpp_lib]
)
# === @end: ray_config (original: ray_config) ===


# === @begin: grpc_util (originial: grpc_util) ===
grpc_util_cpp_lib_dep = declare_dependency(
    include_directories: ['.'],
    dependencies: [
        # Package-managed
        ray_config_cpp_lib_dep,
        status_cpp_lib_dep,

        # Eugo-managed
        grpc,
        absl_container_flat_hash_map
    ]
)
# === @end: grpc_util (originial: grpc_util) ===



# === @begin: event_stats (original: event_stats) ===
event_stats_cpp_lib = static_library(
    'event_stats_cpp_lib',
    ['event_stats.cc'],
    install: false,
    dependencies: [
        # Package-managed
        ray_config_cpp_lib_dep,
        stats_metric_cpp_lib_dep,
        util_cpp_lib_dep,

        # Eugo-managed
        absl_container_flat_hash_map,
        absl_synchronization
    ]
)

event_stats_cpp_lib_dep = declare_dependency(
    link_with: [event_stats_cpp_lib]
)
# === @end: event_stats (original: event_stats) ===


# === @begin: asio/ ===
subdir('asio')
# === @end: asio/ ===


# === @begin: memory_monitor (original: memory_monitor) ===
memory_monitor_cpp_lib = static_library(
    'memory_monitor_cpp_lib',
    ['memory_monitor.cc'],
    install: false,
    dependencies: [
        # Package-managed
        asio_cpp_lib_dep,
        ray_config_cpp_lib_dep,
        util_cpp_lib_dep,

        # Eugo-managed
        absl_strings,
        #"@com_google_googletest//:gtest_prod",
        nlohmann_json
    ]
)

memory_monitor_cpp_lib_dep = declare_dependency(
    link_with: [memory_monitor_cpp_lib]
)
# === @end: memory_monitor (original: memory_monitor) ===


#ray_cc_library(
#    name = "file_system_monitor",
#    srcs = [
#    "file_system_monitor.cc",
#    ],
#    hdrs = [
#    "file_system_monitor.h",
#    ],
#    deps = [
#    ":asio",
#    "//src/ray/util",
#    "@com_google_googletest//:gtest_prod",
#    ],
#)
#
#ray_cc_library(
#    name = "runtime_env",
#    srcs = [
#    "runtime_env_common.cc",
#    "runtime_env_manager.cc",
#    ],
#    hdrs = [
#    "runtime_env_common.h",
#    "runtime_env_manager.h",
#    ],
#    deps = [
#    ":id",
#    "//src/ray/protobuf:common_cc_proto",
#    "@com_google_absl//absl/container:flat_hash_map",
#    ],
#)
#
#ray_cc_library(
#    name = "network",
#    srcs = [
#    "client_connection.cc",
#    "network_util.cc",
#    ],
#    hdrs = [
#    "client_connection.h",
#    "network_util.h",
#    ],
#    deps = [
#    ":asio",
#    ":id",
#    ":status",
#    "//:node_manager_fbs",
#    ],
#)
#

#
#ray_cc_library(
#    name = "task_common",
#    srcs = [
#    "bundle_location_index.cc",
#    "bundle_spec.cc",
#    "function_descriptor.cc",
#    "placement_group.cc",
#    "scheduling/cluster_resource_data.cc",
#    "scheduling/fixed_point.cc",
#    "scheduling/resource_instance_set.cc",
#    "scheduling/resource_set.cc",
#    "scheduling/scheduling_ids.cc",
#    "task/task.cc",
#    "task/task_spec.cc",
#    ],
#    hdrs = [
#    "bundle_location_index.h",
#    "bundle_spec.h",
#    "function_descriptor.h",
#    "placement_group.h",
#    "scheduling/cluster_resource_data.h",
#    "scheduling/fixed_point.h",
#    "scheduling/resource_instance_set.h",
#    "scheduling/resource_set.h",
#    "scheduling/scheduling_ids.h",
#    "task/task.h",
#    "task/task_common.h",
#    "task/task_spec.h",
#    "task/task_util.h",
#    ],
#    deps = [
#    ":event_stats",
#    ":grpc_util",
#    ":id",
#    ":ray_config",
#    ":ray_object",
#    ":runtime_env",
#    "//:node_manager_fbs",
#    "//src/ray/util",
#    "@com_google_absl//absl/container:flat_hash_map",
#    "@com_google_absl//absl/container:flat_hash_set",
#    "@com_google_absl//absl/strings",
#    "@com_google_absl//absl/synchronization",
#    ],
#)
#


#
#ray_cc_library(
#    name = "ray_syncer",
#    srcs = [
#    "ray_syncer/ray_syncer.cc",
#    ],
#    hdrs = [
#    "ray_syncer/ray_syncer.h",
#    "ray_syncer/ray_syncer-inl.h",
#    ],
#    deps = [
#    ":asio",
#    ":id",
#    "//:ray_syncer_cc_grpc",
#    "//src/ray/util",
#    "@com_github_grpc_grpc//:grpc++",
#    "@com_google_absl//absl/container:flat_hash_map",
#    ],
#)


#ray_cc_library(
#    name = "test_util",
#    srcs = ["test_util.cc"],
#    hdrs = ["test_util.h"],
#    deps = [
#    ":id",
#    ":network",
#    ":ray_object",
#    "//src/ray/protobuf:common_cc_proto",
#    "//src/ray/util",
#    "@boost//:optional",
#    "@com_google_googletest//:gtest",
#    ],
#)
