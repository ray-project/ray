# === @begin: constants ===
# header-only library
constants_cpp_lib_dep = declare_dependency(
    sources: ['constants.h'],
    include_directories: ['.']
)
# === @end: constants ===


# === @begin: status_cpp_lib (original: status) ===
status_cpp_lib = static_library(
    'status_cpp_lib',
    ['status.cc'],
    install: false,
    dependencies: [util_cpp_lib_dep]
)

status_cpp_lib_dep = declare_dependency(
    link_with: [status_cpp_lib]
)
# === @end: status_cpp_lib (original: status) ===


# === @begin: id_cpp_lib (original: id) ===
id_cpp_lib_private_deps = [
    # Project-managed
    constants_cpp_lib_dep,
    status_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    util_cpp_lib_dep,
]

id_cpp_lib_public_deps = [
    # Eugo-managed
    flatbuffers,
    msgpack_cxx
]

id_cpp_lib = static_library(
    'id_cpp_lib',
    [
        'id.cc',
        'common_protocol.cc'
    ],
    install: false,
    dependencies: [
        id_cpp_lib_private_deps,
        id_cpp_lib_public_deps
    ]
)

id_cpp_lib_dep = declare_dependency(
    link_with: [id_cpp_lib],
    dependencies: id_cpp_lib_public_deps
)
# === @end: id_cpp_lib (original: id) ===


# === @begin: ray_object_cpp_lib (original: c) ===
ray_object_cpp_lib_private_deps = [
    # Package-managed
    id_cpp_lib_dep,
    status_cpp_lib_dep,
    aligned_alloc_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    util_cpp_lib_dep,
]

ray_object_cpp_lib_public_deps = [
    # Eugo-managed
    absl_time,
    absl_types_optional
]

ray_object_cpp_lib = static_library(
    'ray_object_cpp_lib',
    ['ray_object.cc'],
    install: false,
    dependencies: [
        ray_object_cpp_lib_private_deps,
        ray_object_cpp_lib_public_deps
    ]
)

ray_object_cpp_lib_dep = declare_dependency(
    link_with: [ray_object_cpp_lib],
    dependencies: ray_object_cpp_lib_public_deps
)
# === @end: ray_object_cpp_lib (original: c) ===


# === @begin: ray_config_cpp_lib (original: ray_config) ===
ray_config_cpp_lib_private_deps = [
    # Package-managed
    util_cpp_lib_dep,
]

ray_config_cpp_lib_public_deps = [
    # Eugo-managed
    absl_algorithm,
    absl_strings,
    nlohmann_json
]

ray_config_cpp_lib = static_library(
    'ray_config_cpp_lib',
    ['ray_config.cc'],
    install: false,
    dependencies: [
        ray_config_cpp_lib_private_deps,
        ray_config_cpp_lib_public_deps
    ]
)

ray_config_cpp_lib_dep = declare_dependency(
    link_with: [ray_config_cpp_lib],
    dependencies: ray_config_cpp_lib_public_deps
)
# === @end: ray_config_cpp_lib (original: ray_config) ===


# === @begin: grpc_util (originial: grpc_util) ===
# header-only
grpc_util_cpp_lib_dep = declare_dependency(
    include_directories: ['.'],
    dependencies: [
        # @TODO+:should those guys become a private interface?
        # Package-managed
        ray_config_cpp_lib_dep,
        status_cpp_lib_dep,

        # Eugo-managed
        grpc,
        absl_container_flat_hash_map
    ]
)
# === @end: grpc_util (originial: grpc_util) ===


# === @begin: event_stats_cpp_lib (original: event_stats) ===
event_stats_cpp_lib_private_deps = [
    # Package-managed
    ray_config_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    util_cpp_lib_dep,
]

event_stats_cpp_lib_public_deps = [
    # Eugo-managed
    absl_container_flat_hash_map,
    absl_synchronization
]

event_stats_cpp_lib = static_library(
    'event_stats_cpp_lib',
    ['event_stats.cc'],
    install: false,
    dependencies: [
        event_stats_cpp_lib_private_deps,
        event_stats_cpp_lib_public_deps
    ]
)

event_stats_cpp_lib_dep = declare_dependency(
    link_with: [event_stats_cpp_lib],
    dependencies: event_stats_cpp_lib_public_deps
)
# === @end: event_stats (original: event_stats) ===


# === @begin: asio/ ===
subdir('asio')
# === @end: asio/ ===


# === @begin: memory_monitor_cpp_lib (original: memory_monitor) ===
memory_monitor_cpp_lib_private_deps = [
    # Package-managed
    asio_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    util_cpp_lib_dep,
]

memory_monitor_cpp_lib_public_deps = [
    # Eugo-managed
    absl_strings,
    #"@com_google_googletest//:gtest_prod",
    nlohmann_json
]

memory_monitor_cpp_lib = static_library(
    'memory_monitor_cpp_lib',
    ['memory_monitor.cc'],
    install: false,
    dependencies: [
        memory_monitor_cpp_lib_private_deps,
        memory_monitor_cpp_lib_public_deps
    ]
)

memory_monitor_cpp_lib_dep = declare_dependency(
    link_with: [memory_monitor_cpp_lib],
    dependencies: memory_monitor_cpp_lib_public_deps
)
# === @end: memory_monitor_cpp_lib (original: memory_monitor) ===


# === @begin: file_system_monitor_cpp_lib (original: file_system_monitor) ===
file_system_monitor_cpp_lib_private_deps = [
    # Package-managed
    asio_cpp_lib_dep,
    util_cpp_lib_dep
]

file_system_monitor_cpp_lib_public_deps = [
    # Eugo-managed
    #    "@com_google_googletest//:gtest_prod",
]

file_system_monitor_cpp_lib = static_library(
    'file_system_monitor_cpp_lib',
    ['file_system_monitor.cc'],
    install: false,
    dependencies: [
        file_system_monitor_cpp_lib_private_deps,
        file_system_monitor_cpp_lib_public_deps
    ]
)

file_system_monitor_cpp_lib_dep = declare_dependency(
    link_with: [file_system_monitor_cpp_lib],
    dependencies: file_system_monitor_cpp_lib_public_deps
)
# === @end: file_system_monitor_cpp_lib (original: file_system_monitor) ===


# === @begin: runtime_env_cpp_lib (original: runtime_env) ===
runtime_env_cpp_lib_private_deps = [
    # Package-managed
    id_cpp_lib_dep,
    common_proto_cpp_lib_dep,
]

runtime_env_cpp_lib_public_deps = [
    # Eugo-managed
    absl_container_flat_hash_map
]

runtime_env_cpp_lib = static_library(
    'runtime_env_cpp_lib',
    ['runtime_env_common.cc', 'runtime_env_manager.cc'],
    install: false,
    dependencies: [
        runtime_env_cpp_lib_private_deps,
        runtime_env_cpp_lib_public_deps
    ]
)

runtime_env_cpp_lib_dep = declare_dependency(
    link_with: [runtime_env_cpp_lib],
    dependencies: runtime_env_cpp_lib_public_deps
)
# === @end: runtime_env_cpp_lib (original: runtime_env) ===


# === @begin: ray_syncer ===
subdir('ray_syncer/')
# === @end: ray_syncer ===


# === @begin: network_cpp_lib (original: network) ===
network_cpp_lib = static_library(
    'network_cpp_lib',
    [
        'client_connection.cc',
        'network_util.cc'
    ],
    install: false,
    dependencies: [
        # Package-managed
        asio_cpp_lib_dep,
        id_cpp_lib_dep,
        status_cpp_lib_dep,
        node_manager_cpp_fbs_lib_dep
    ]
)

network_cpp_lib_dep = declare_dependency(
    link_with: [network_cpp_lib],
)
# === @end: network_cpp_lib (original: network) ===


## === @begin: ray_common_cpp_lib (original: runtime_env) ===
#ray_common_cpp_lib_private_deps = [
#    # Package-managed
#    stats_metric_cpp_lib_dep,
#    asio_cpp_lib_dep,
#    constants_cpp_lib_dep,
#    event_stats_cpp_lib_dep,
#    file_system_monitor_cpp_lib_dep,
#    grpc_util_cpp_lib_dep,
#    id_cpp_lib_dep,
#    memory_monitor_cpp_lib_dep,
#    network_cpp_lib_dep,
#    ray_config_cpp_lib_dep,
#    ray_syncer_cpp_lib_dep,
#    status_cpp_lib_dep,
#    task_common_cpp_lib_dep,
#    #    "//src/ray/common:test_util",
#    gcs_proto_cpp_lib_dep,
#]
#
#ray_common_cpp_lib_public_deps = [
#    # Eugo-managed
#    #    "@com_google_googletest//:gtest",
#]
#
## Header-only guy? Or even virtual dependency? meh
#ray_common_cpp_lib_dep = declare_dependency(
#    dependencies: [
#        runtime_env_cpp_lib_private_deps,
#        runtime_env_cpp_lib_public_deps
#    ]
#)
## === @end: ray_common_cpp_lib (original: ray_common) ===




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
