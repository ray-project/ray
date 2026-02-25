# === @begin: constants_cpp_lib (@original: constants) ===
# Header-only
constants_cpp_lib_dep = declare_dependency(
    sources: ['constants.h']
)
# === @end: constants_cpp_lib (@original: constants) ===


# === @begin: macros_cpp_lib (@original: macros) ===
# Header-only
# NOTE: Renamed to common_macros_cpp_lib_dep to avoid overwriting util/macros_cpp_lib_dep
# which was defined in util/meson.build (processed earlier). Both common/macros.h and
# util/macros.h exist as separate files with different content.
common_macros_cpp_lib_dep = declare_dependency(
    sources: ['macros.h']
)
# === @end: macros_cpp_lib (@original: macros) ===


# === @begin: source_location_cpp_lib (@original: source_location) ===
source_location_cpp_lib = static_library(
    'source_location_cpp_lib',
    ['source_location.cc'],
    install: false
)

source_location_cpp_lib_dep = declare_dependency(
    link_with: [source_location_cpp_lib]
)
# === @end: source_location_cpp_lib (@original: source_location) ===


# === @begin: status_cpp_lib (@original: status) ===
status_cpp_lib_dependencies = [
    # Package-managed
    common_macros_cpp_lib_dep,
    source_location_cpp_lib_dep,
    logging_cpp_lib_dep,
    macros_cpp_lib_dep,
    visibility_cpp_lib_dep,

    # Eugo-managed
    boost_system,
    absl_container_flat_hash_map,
    absl_strings,
]

status_cpp_lib = static_library(
    'status_cpp_lib',
    ['status.cc'],
    install: false,
    dependencies: status_cpp_lib_dependencies
)

status_cpp_lib_dep = declare_dependency(
    link_with: [status_cpp_lib],
    dependencies: status_cpp_lib_dependencies
)
# === @end: status_cpp_lib (@original: status) ===


# === @begin: status_or_cpp_lib (@original: status_or) ===
# Header-only
status_or_cpp_lib_dependencies = [
    # Package-managed
    common_macros_cpp_lib_dep,
    status_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_base_core_headers,
]

status_or_cpp_lib_dep = declare_dependency(
    sources: ['status_or.h'],
    dependencies: status_or_cpp_lib_dependencies
)
# === @end: status_or_cpp_lib (@original: status_or) ===


# === @begin: id_cpp_lib (@original: id) ===
id_cpp_lib_dependencies = [
    # Package-managed
    constants_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    sha256_cpp_lib_dep,
    logging_cpp_lib_dep,
    random_cpp_lib_dep,
    visibility_cpp_lib_dep,

    # Eugo-managed
    msgpack_cxx,
]

id_cpp_lib = static_library(
    'id_cpp_lib',
    ['id.cc'],
    install: false,
    dependencies: id_cpp_lib_dependencies
)

id_cpp_lib_dep = declare_dependency(
    link_with: [id_cpp_lib],
    dependencies: id_cpp_lib_dependencies
)
# === @end: id_cpp_lib (@original: id) ===


# === @begin: buffer_cpp_lib (@original: buffer) ===
# Header-only
buffer_cpp_lib_dependencies = [
    # Package-managed
    status_cpp_lib_dep,
    aligned_alloc_cpp_lib_dep,
    logging_cpp_lib_dep,
]

buffer_cpp_lib_dep = declare_dependency(
    sources: ['buffer.h'],
    dependencies: buffer_cpp_lib_dependencies
)
# === @end: buffer_cpp_lib (@original: buffer) ===


# === @begin: ray_object_cpp_lib (@original: ray_object) ===
ray_object_cpp_lib_dependencies = [
    # Package-managed
    buffer_cpp_lib_dep,
    id_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_time,
    absl_types_optional,
    msgpack_cxx,
]

ray_object_cpp_lib = static_library(
    'ray_object_cpp_lib',
    ['ray_object.cc'],
    install: false,
    dependencies: ray_object_cpp_lib_dependencies
)

ray_object_cpp_lib_dep = declare_dependency(
    link_with: [ray_object_cpp_lib],
    dependencies: ray_object_cpp_lib_dependencies
)
# === @end: ray_object_cpp_lib (@original: ray_object) ===


# === @begin: ray_config_cpp_lib (@original: ray_config) ===
ray_config_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,

    # Eugo-managed
    boost_algorithm,                # @boost//:algorithm
    absl_strings,
    nlohmann_json,
]

ray_config_cpp_lib = static_library(
    'ray_config_cpp_lib',
    ['ray_config.cc'],
    install: false,
    dependencies: ray_config_cpp_lib_dependencies
)

ray_config_cpp_lib_dep = declare_dependency(
    link_with: [ray_config_cpp_lib],
    dependencies: ray_config_cpp_lib_dependencies
)
# === @end: ray_config_cpp_lib (@original: ray_config) ===


# === @begin: grpc_util_cpp_lib (@original: grpc_util) ===
# Header-only
grpc_util_cpp_lib_dependencies = [
    # Package-managed
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    logging_cpp_lib_dep,
    type_traits_cpp_lib_dep,

    # Eugo-managed
    grpc,
    absl_container_flat_hash_map,
]

grpc_util_cpp_lib_dep = declare_dependency(
    sources: ['grpc_util.h'],
    dependencies: grpc_util_cpp_lib_dependencies
)
# === @end: grpc_util_cpp_lib (@original: grpc_util) ===


# === @begin: flatbuf_utils_cpp_lib (@original: flatbuf_utils) ===
# Header-only
flatbuf_utils_cpp_lib_dependencies = [
    # Eugo-managed
    flatbuffers,
]

flatbuf_utils_cpp_lib_dep = declare_dependency(
    sources: ['flatbuf_utils.h'],
    dependencies: flatbuf_utils_cpp_lib_dependencies
)
# === @end: flatbuf_utils_cpp_lib (@original: flatbuf_utils) ===


# === @begin: python_callbacks_cpp_lib (@original: python_callbacks) ===
# Header-only
python_callbacks_cpp_lib_dep = declare_dependency(
    sources: ['python_callbacks.h']
)
# === @end: python_callbacks_cpp_lib (@original: python_callbacks) ===


# === @begin: metrics_cpp_lib (@original: metrics) ===
# Header-only
metrics_cpp_lib_dependencies = [
    # Package-managed
    stats_metric_cpp_lib_dep,
]

metrics_cpp_lib_dep = declare_dependency(
    sources: ['metrics.h'],
    dependencies: metrics_cpp_lib_dependencies
)
# === @end: metrics_cpp_lib (@original: metrics) ===


# === @begin: event_stats_cpp_lib (@original: event_stats) ===
event_stats_cpp_lib_dependencies = [
    # Package-managed
    metrics_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    time_cpp_lib_dep,
]

event_stats_cpp_lib = static_library(
    'event_stats_cpp_lib',
    ['event_stats.cc'],
    install: false,
    dependencies: event_stats_cpp_lib_dependencies
)

event_stats_cpp_lib_dep = declare_dependency(
    link_with: [event_stats_cpp_lib],
    dependencies: event_stats_cpp_lib_dependencies
)
# === @end: event_stats_cpp_lib (@original: event_stats) ===


# === @begin: asio_cpp_lib (@original: asio) ===
asio_cpp_lib_dependencies = [
    # Package-managed
    event_stats_cpp_lib_dep,
    metrics_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    array_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    absl_container_flat_hash_map,
    absl_synchronization,
]

asio_cpp_lib = static_library(
    'asio_cpp_lib',
    [
        'asio/asio_chaos.cc',
        'asio/instrumented_io_context.cc',
        'asio/io_service_pool.cc',
        'asio/periodical_runner.cc',
    ],
    install: false,
    dependencies: asio_cpp_lib_dependencies
)

asio_cpp_lib_dep = declare_dependency(
    link_with: [asio_cpp_lib],
    dependencies: asio_cpp_lib_dependencies
)
# === @end: asio_cpp_lib (@original: asio) ===

# NOTE: @skip: fake_periodical_runner (@original: fake_periodical_runner) — test-only header
# (`asio/fake_periodical_runner.h`); not compiled in production Meson build.

# === @begin: memory_monitor_interface_cpp_lib (@original: memory_monitor_interface) ===
# Header-only
memory_monitor_interface_cpp_lib_dependencies = [
    # Package-managed
    compat_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

memory_monitor_interface_cpp_lib_dep = declare_dependency(
    sources: ['memory_monitor_interface.h'],
    dependencies: memory_monitor_interface_cpp_lib_dependencies
)
# === @end: memory_monitor_interface_cpp_lib (@original: memory_monitor_interface) ===


# === @begin: memory_monitor_utils_cpp_lib (@original: memory_monitor_utils) ===
memory_monitor_utils_cpp_lib_dependencies = [
    # Package-managed
    memory_monitor_interface_cpp_lib_dep,
    compat_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    boost_algorithm,                # @boost//:algorithm
    absl_strings,
    # @com_google_googletest//:gtest_prod — handled by vendored stub
]

memory_monitor_utils_cpp_lib = static_library(
    'memory_monitor_utils_cpp_lib',
    ['memory_monitor_utils.cc'],
    install: false,
    dependencies: memory_monitor_utils_cpp_lib_dependencies
)

memory_monitor_utils_cpp_lib_dep = declare_dependency(
    link_with: [memory_monitor_utils_cpp_lib],
    dependencies: memory_monitor_utils_cpp_lib_dependencies
)
# === @end: memory_monitor_utils_cpp_lib (@original: memory_monitor_utils) ===


# === @begin: threshold_memory_monitor_cpp_lib (@original: threshold_memory_monitor) ===
# NOTE: In Bazel this target has target_compatible_with = ["@platforms//os:linux"],
# but the code compiles fine on all platforms via #ifdefs.
threshold_memory_monitor_cpp_lib_dependencies = [
    # Package-managed
    asio_cpp_lib_dep,
    memory_monitor_interface_cpp_lib_dep,
    memory_monitor_utils_cpp_lib_dep,
    logging_cpp_lib_dep,
    thread_utils_cpp_lib_dep,

    # Eugo-managed
    absl_strings,
]

threshold_memory_monitor_cpp_lib = static_library(
    'threshold_memory_monitor_cpp_lib',
    ['threshold_memory_monitor.cc'],
    install: false,
    dependencies: threshold_memory_monitor_cpp_lib_dependencies
)

threshold_memory_monitor_cpp_lib_dep = declare_dependency(
    link_with: [threshold_memory_monitor_cpp_lib],
    dependencies: threshold_memory_monitor_cpp_lib_dependencies
)
# === @end: threshold_memory_monitor_cpp_lib (@original: threshold_memory_monitor) ===


# === @begin: noop_memory_monitor_cpp_lib (@original: noop_memory_monitor) ===
# Header-only
noop_memory_monitor_cpp_lib_dependencies = [
    # Package-managed
    memory_monitor_interface_cpp_lib_dep,
]

noop_memory_monitor_cpp_lib_dep = declare_dependency(
    sources: ['noop_memory_monitor.h'],
    dependencies: noop_memory_monitor_cpp_lib_dependencies
)
# === @end: noop_memory_monitor_cpp_lib (@original: noop_memory_monitor) ===


# === @begin: memory_monitor_factory_cpp_lib (@original: memory_monitor_factory) ===
# NOTE: Bazel uses select() to choose threshold_memory_monitor_factory.cc on Linux
# and noop_memory_monitor_factory.cc on non-Linux. We compile both on all platforms
# since the code compiles cleanly everywhere, matching the previous consolidated target.
memory_monitor_factory_cpp_lib_dependencies = [
    # Package-managed
    memory_monitor_interface_cpp_lib_dep,
    noop_memory_monitor_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    threshold_memory_monitor_cpp_lib_dep,
    logging_cpp_lib_dep,
]

memory_monitor_factory_cpp_lib = static_library(
    'memory_monitor_factory_cpp_lib',
    [
        'threshold_memory_monitor_factory.cc',
        'noop_memory_monitor_factory.cc',
    ],
    install: false,
    dependencies: memory_monitor_factory_cpp_lib_dependencies
)

memory_monitor_factory_cpp_lib_dep = declare_dependency(
    link_with: [memory_monitor_factory_cpp_lib],
    dependencies: memory_monitor_factory_cpp_lib_dependencies
)
# === @end: memory_monitor_factory_cpp_lib (@original: memory_monitor_factory) ===


# === @begin: cgroup2/ ===
subdir('cgroup2')
# === @end: cgroup2/ ===


# === @begin: file_system_monitor_cpp_lib (@original: file_system_monitor) ===
file_system_monitor_cpp_lib_dependencies = [
    # Package-managed
    asio_cpp_lib_dep,
    event_cpp_lib_dep,
    # @com_google_googletest//:gtest_prod — handled by vendored stub
]

file_system_monitor_cpp_lib = static_library(
    'file_system_monitor_cpp_lib',
    ['file_system_monitor.cc'],
    install: false,
    dependencies: file_system_monitor_cpp_lib_dependencies
)

file_system_monitor_cpp_lib_dep = declare_dependency(
    link_with: [file_system_monitor_cpp_lib],
    dependencies: file_system_monitor_cpp_lib_dependencies
)
# === @end: file_system_monitor_cpp_lib (@original: file_system_monitor) ===


# === @begin: runtime_env_cpp_lib (@original: runtime_env) ===
runtime_env_cpp_lib_dependencies = [
    # Package-managed
    id_cpp_lib_dep,
    common_proto_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

runtime_env_cpp_lib = static_library(
    'runtime_env_cpp_lib',
    [
        'runtime_env_common.cc',
        'runtime_env_manager.cc'
    ],
    install: false,
    dependencies: runtime_env_cpp_lib_dependencies
)

runtime_env_cpp_lib_dep = declare_dependency(
    link_with: [runtime_env_cpp_lib],
    dependencies: runtime_env_cpp_lib_dependencies
)
# === @end: runtime_env_cpp_lib (@original: runtime_env) ===


# NOTE: ray_syncer moved to top-level src/ray/ray_syncer/ during upstream restructure
# NOTE: network_cpp_lib removed — client_connection.cc moved to raylet_ipc_client/, network_util.cc moved to util/


# === @begin: function_descriptor_cpp_lib (@original: function_descriptor) ===
function_descriptor_cpp_lib_dependencies = [
    # Package-managed
    grpc_util_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_strings,                   # includes str_format
]

function_descriptor_cpp_lib = static_library(
    'function_descriptor_cpp_lib',
    ['function_descriptor.cc'],
    install: false,
    dependencies: function_descriptor_cpp_lib_dependencies
)

function_descriptor_cpp_lib_dep = declare_dependency(
    link_with: [function_descriptor_cpp_lib],
    dependencies: function_descriptor_cpp_lib_dependencies
)
# === @end: function_descriptor_cpp_lib (@original: function_descriptor) ===


# === @begin: scheduling/ ===
subdir('scheduling')
# === @end: scheduling/ ===


# === @begin: bundle_spec_cpp_lib (@original: bundle_spec) ===
bundle_spec_cpp_lib_dependencies = [
    # Package-managed
    grpc_util_cpp_lib_dep,
    id_cpp_lib_dep,
    cluster_resource_data_cpp_lib_dep,
    label_selector_cpp_lib_dep,
    placement_group_util_cpp_lib_dep,
    scheduling_ids_cpp_lib_dep,
    common_proto_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    protobuf,
]

bundle_spec_cpp_lib = static_library(
    'bundle_spec_cpp_lib',
    ['bundle_spec.cc'],
    install: false,
    dependencies: bundle_spec_cpp_lib_dependencies
)

bundle_spec_cpp_lib_dep = declare_dependency(
    link_with: [bundle_spec_cpp_lib],
    dependencies: bundle_spec_cpp_lib_dependencies
)
# === @end: bundle_spec_cpp_lib (@original: bundle_spec) ===


# === @begin: placement_group_cpp_lib (@original: placement_group) ===
placement_group_cpp_lib_dependencies = [
    # Package-managed
    bundle_spec_cpp_lib_dep,
    id_cpp_lib_dep,
    common_proto_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    protobuf,
]

placement_group_cpp_lib = static_library(
    'placement_group_cpp_lib',
    ['placement_group.cc'],
    install: false,
    dependencies: placement_group_cpp_lib_dependencies
)

placement_group_cpp_lib_dep = declare_dependency(
    link_with: [placement_group_cpp_lib],
    dependencies: placement_group_cpp_lib_dependencies
)
# === @end: placement_group_cpp_lib (@original: placement_group) ===


# === @begin: bundle_location_index_cpp_lib (@original: bundle_location_index) ===
bundle_location_index_cpp_lib_dependencies = [
    # Package-managed
    id_cpp_lib_dep,
    placement_group_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

bundle_location_index_cpp_lib = static_library(
    'bundle_location_index_cpp_lib',
    ['bundle_location_index.cc'],
    install: false,
    dependencies: bundle_location_index_cpp_lib_dependencies
)

bundle_location_index_cpp_lib_dep = declare_dependency(
    link_with: [bundle_location_index_cpp_lib],
    dependencies: bundle_location_index_cpp_lib_dependencies
)
# === @end: bundle_location_index_cpp_lib (@original: bundle_location_index) ===


# === @begin: lease_cpp_lib (@original: lease) ===
lease_cpp_lib_dependencies = [
    # Package-managed
    function_descriptor_cpp_lib_dep,
    id_cpp_lib_dep,
    runtime_env_cpp_lib_dep,
    label_selector_cpp_lib_dep,
    resource_set_cpp_lib_dep,
    scheduling_class_util_cpp_lib_dep,
    common_proto_cpp_lib_dep,
]

lease_cpp_lib = static_library(
    'lease_cpp_lib',
    ['lease/lease_spec.cc'],
    install: false,
    dependencies: lease_cpp_lib_dependencies
)

lease_cpp_lib_dep = declare_dependency(
    link_with: [lease_cpp_lib],
    dependencies: lease_cpp_lib_dependencies
)
# === @end: lease_cpp_lib (@original: lease) ===


# === @begin: task_common_cpp_lib (@original: task_common) ===
task_common_cpp_lib_dependencies = [
    # Package-managed
    event_stats_cpp_lib_dep,
    function_descriptor_cpp_lib_dep,
    grpc_util_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    ray_object_cpp_lib_dep,
    runtime_env_cpp_lib_dep,
    # Scheduling
    fallback_strategy_cpp_lib_dep,
    label_selector_cpp_lib_dep,
    resource_set_cpp_lib_dep,
    scheduling_class_util_cpp_lib_dep,
    # FlatBuffers
    node_manager_cpp_fbs_lib_dep,
    # Util
    container_util_cpp_lib_dep,
    # Observability
    metric_interface_cpp_lib_dep,   # defined in `src/ray/meson.build` before `subdir('common/')`

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_strings,
]

task_common_cpp_lib = static_library(
    'task_common_cpp_lib',
    ['task/task_spec.cc'],
    install: false,
    dependencies: task_common_cpp_lib_dependencies
)

task_common_cpp_lib_dep = declare_dependency(
    link_with: [task_common_cpp_lib],
    dependencies: task_common_cpp_lib_dependencies
)
# === @end: task_common_cpp_lib (@original: task_common) ===


# === @begin: protobuf_utils_cpp_lib (@original: protobuf_utils) ===
protobuf_utils_cpp_lib_dependencies = [
    # Package-managed
    constants_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    task_common_cpp_lib_dep,
    autoscaler_proto_cpp_lib_dep,
    export_task_event_proto_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    time_cpp_lib_dep,

    # Eugo-managed
    absl_time,
]

protobuf_utils_cpp_lib = static_library(
    'protobuf_utils_cpp_lib',
    ['protobuf_utils.cc'],
    install: false,
    dependencies: protobuf_utils_cpp_lib_dependencies
)

protobuf_utils_cpp_lib_dep = declare_dependency(
    link_with: [protobuf_utils_cpp_lib],
    dependencies: protobuf_utils_cpp_lib_dependencies
)
# === @end: protobuf_utils_cpp_lib (@original: protobuf_utils) ===


# === @begin: ray_common_cpp_lib (@original: ray_common) ===
# Umbrella dependency that aggregates all common/ sub-targets.
ray_common_cpp_lib_dep = declare_dependency(
    dependencies: [
        buffer_cpp_lib_dep,
        bundle_location_index_cpp_lib_dep,
        bundle_spec_cpp_lib_dep,
        constants_cpp_lib_dep,
        event_stats_cpp_lib_dep,
        file_system_monitor_cpp_lib_dep,
        flatbuf_utils_cpp_lib_dep,
        grpc_util_cpp_lib_dep,
        id_cpp_lib_dep,
        lease_cpp_lib_dep,
        common_macros_cpp_lib_dep,
        memory_monitor_factory_cpp_lib_dep,
        memory_monitor_interface_cpp_lib_dep,
        metrics_cpp_lib_dep,
        noop_memory_monitor_cpp_lib_dep,
        placement_group_cpp_lib_dep,
        protobuf_utils_cpp_lib_dep,
        python_callbacks_cpp_lib_dep,
        ray_config_cpp_lib_dep,
        ray_object_cpp_lib_dep,
        runtime_env_cpp_lib_dep,
        status_cpp_lib_dep,
        status_or_cpp_lib_dep,
        task_common_cpp_lib_dep,
    ]
)
# === @end: ray_common_cpp_lib (@original: ray_common) ===
