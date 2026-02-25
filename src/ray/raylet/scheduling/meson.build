# @IMPORTANT: This subdir is processed via `subdir('raylet/scheduling/')` directly from
# `src/ray/meson.build`, BEFORE `raylet/` and `observability/` are processed. Therefore:
#   - //src/ray/raylet:metrics, :worker_interface, :worker_pool,
#     :lease_dependency_manager are NOT available (documented as comments)
#   - //src/ray/observability:metric_interface, :fake_metric are NOT available
# @IMPORTANT: These deps are resolved at final link time via `_raylet` shared library.
#
# NOTE: Available from parent scope:
#   From common/: task_common_cpp_lib_dep (consolidates lease, bundle_location_index,
#     placement_group), ray_config_cpp_lib_dep, id_cpp_lib_dep, grpc_util_cpp_lib_dep,
#     ray_object_cpp_lib_dep, ray_common_cpp_lib_dep
#   From common/scheduling/: cluster_resource_data_cpp_lib_dep,
#     placement_group_util_cpp_lib_dep
#   From protobuf/: gcs_proto_cpp_lib_dep, node_manager_proto_cpp_lib_dep
#   From util/: logging_cpp_lib_dep, container_util_cpp_lib_dep
#   From stats/: stats_metric_cpp_lib_dep, stats_cpp_lib_dep
#   From ray_syncer/: ray_syncer_cpp_lib_dep
#   From rpc/: grpc_common_cpp_lib_dep
#   From object_manager/: object_manager_common_dep
#   From gcs_rpc_client/: gcs_client_cpp_lib_dep


# === @begin: scheduling_context_cpp_lib (@original: scheduling_context) ===
# Header-only
scheduling_context_cpp_lib_dependencies = [
    # Package-managed
    id_cpp_lib_dep,
    placement_group_cpp_lib_dep,
]

scheduling_context_cpp_lib_dep = declare_dependency(
    sources: ['policy/scheduling_context.h'],
    dependencies: scheduling_context_cpp_lib_dependencies
)
# === @end: scheduling_context_cpp_lib (@original: scheduling_context) ===


# === @begin: scheduler_internal_cpp_lib (@original: scheduler_internal) ===
# Header-only
scheduler_internal_cpp_lib_dependencies = [
    # Package-managed
    lease_cpp_lib_dep,
    cluster_resource_data_cpp_lib_dep,
    node_manager_proto_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
]

scheduler_internal_cpp_lib_dep = declare_dependency(
    sources: ['internal.h'],
    dependencies: scheduler_internal_cpp_lib_dependencies
)
# === @end: scheduler_internal_cpp_lib (@original: scheduler_internal) ===


# === @begin: cluster_lease_manager_interface_cpp_lib (@original: cluster_lease_manager_interface) ===
# Header-only
cluster_lease_manager_interface_cpp_lib_dependencies = [
    # Package-managed
    node_manager_proto_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
]

cluster_lease_manager_interface_cpp_lib_dep = declare_dependency(
    sources: ['cluster_lease_manager_interface.h'],
    dependencies: cluster_lease_manager_interface_cpp_lib_dependencies
)
# === @end: cluster_lease_manager_interface_cpp_lib (@original: cluster_lease_manager_interface) ===


# === @begin: scheduling_options_cpp_lib (@original: scheduling_options) ===
# Header-only
scheduling_options_cpp_lib_dependencies = [
    # Package-managed
    scheduling_context_cpp_lib_dep,
    ray_config_cpp_lib_dep,
]

scheduling_options_cpp_lib_dep = declare_dependency(
    sources: ['policy/scheduling_options.h'],
    dependencies: scheduling_options_cpp_lib_dependencies
)
# === @end: scheduling_options_cpp_lib (@original: scheduling_options) ===


# === @begin: scheduling_policy_cpp_lib (@original: scheduling_policy) ===
# Header-only
scheduling_policy_cpp_lib_dependencies = [
    # Package-managed
    scheduling_options_cpp_lib_dep,
    cluster_resource_data_cpp_lib_dep,
]

scheduling_policy_cpp_lib_dep = declare_dependency(
    sources: ['policy/scheduling_policy.h'],
    dependencies: scheduling_policy_cpp_lib_dependencies
)
# === @end: scheduling_policy_cpp_lib (@original: scheduling_policy) ===


# === @begin: local_resource_manager_cpp_lib (@original: local_resource_manager) ===
local_resource_manager_cpp_lib_dependencies = [
    # Package-managed
    cluster_resource_data_cpp_lib_dep,
    placement_group_util_cpp_lib_dep,
    # `metric_interface_cpp_lib_dep` is NOT AVAILABLE (processed after): //src/ray/observability:metric_interface is resolved at final link time via _raylet shared library.
    gcs_proto_cpp_lib_dep,
    node_manager_proto_cpp_lib_dep,
    ray_syncer_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    # "@com_google_googletest//:gtest_prod",
]

local_resource_manager_cpp_lib = static_library(
    'local_resource_manager_cpp_lib',
    ['local_resource_manager.cc'],
    install: false,
    dependencies: local_resource_manager_cpp_lib_dependencies
)

local_resource_manager_cpp_lib_dep = declare_dependency(
    link_with: [local_resource_manager_cpp_lib],
    dependencies: local_resource_manager_cpp_lib_dependencies
)
# === @end: local_resource_manager_cpp_lib (@original: local_resource_manager) ===


# === @begin: local_lease_manager_interface_cpp_lib (@original: local_lease_manager_interface) ===
# Header-only
local_lease_manager_interface_cpp_lib_dependencies = [
    # Package-managed
    scheduler_internal_cpp_lib_dep,
    # `fake_metric_cpp_lib_dep` (only for tests) is NOT AVAILABLE (processed after): //src/ray/observability:fake_metric (intentionally omitted — test/fake helper, no Meson target)
    # `raylet_metrics_cpp_lib_dep` is NOT AVAILABLE (processed after): //src/ray/raylet:metrics

    # Eugo-managed
    absl_container_flat_hash_map,
]

local_lease_manager_interface_cpp_lib_dep = declare_dependency(
    sources: ['local_lease_manager_interface.h'],
    dependencies: local_lease_manager_interface_cpp_lib_dependencies
)
# === @end: local_lease_manager_interface_cpp_lib (@original: local_lease_manager_interface) ===


# === @begin: cluster_resource_manager_cpp_lib (@original: cluster_resource_manager) ===
cluster_resource_manager_cpp_lib_dependencies = [
    # Package-managed
    local_resource_manager_cpp_lib_dep,
    bundle_location_index_cpp_lib_dep,
    grpc_util_cpp_lib_dep,
    lease_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    cluster_resource_data_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    # `raylet_metrics_cpp_lib_dep` is NOT AVAILABLE (processed after): `//src/ray/raylet:metrics`. Resolved at final link time via _raylet shared library.
    container_util_cpp_lib_dep,
    logging_cpp_lib_dep,
    # Virtual targets
    # @Important: This ensures no broken dependency chain, which may be caused by `NOT AVAILABLE`
    all_proto_cpp_libs_dep,
    all_proto_cpp_grpc_libs_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    # "@com_google_googletest//:gtest_prod",
]

cluster_resource_manager_cpp_lib = static_library(
    'cluster_resource_manager_cpp_lib',
    ['cluster_resource_manager.cc'],
    install: false,
    dependencies: cluster_resource_manager_cpp_lib_dependencies
)

cluster_resource_manager_cpp_lib_dep = declare_dependency(
    link_with: [cluster_resource_manager_cpp_lib],
    dependencies: cluster_resource_manager_cpp_lib_dependencies
)
# === @end: cluster_resource_manager_cpp_lib (@original: cluster_resource_manager) ===


# === @begin: scheduler_resource_reporter_cpp_lib (@original: scheduler_resource_reporter) ===
scheduler_resource_reporter_cpp_lib_dependencies = [
    # Package-managed
    local_lease_manager_interface_cpp_lib_dep,
    scheduler_internal_cpp_lib_dep,
    ray_config_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

scheduler_resource_reporter_cpp_lib = static_library(
    'scheduler_resource_reporter_cpp_lib',
    ['scheduler_resource_reporter.cc'],
    install: false,
    dependencies: scheduler_resource_reporter_cpp_lib_dependencies
)

scheduler_resource_reporter_cpp_lib_dep = declare_dependency(
    link_with: [scheduler_resource_reporter_cpp_lib],
    dependencies: scheduler_resource_reporter_cpp_lib_dependencies
)
# === @end: scheduler_resource_reporter_cpp_lib (@original: scheduler_resource_reporter) ===


# === @begin: scorer_cpp_lib (@original: scorer) ===
scorer_cpp_lib_dependencies = [
    # Package-managed
    cluster_resource_data_cpp_lib_dep,
]

scorer_cpp_lib = static_library(
    'scorer_cpp_lib',
    ['policy/scorer.cc'],
    install: false,
    dependencies: scorer_cpp_lib_dependencies
)

scorer_cpp_lib_dep = declare_dependency(
    link_with: [scorer_cpp_lib],
    dependencies: scorer_cpp_lib_dependencies
)
# === @end: scorer_cpp_lib (@original: scorer) ===


# === @begin: hybrid_scheduling_policy_cpp_lib (@original: hybrid_scheduling_policy) ===
hybrid_scheduling_policy_cpp_lib_dependencies = [
    # Package-managed
    scheduling_policy_cpp_lib_dep,

    # Eugo-managed
    absl_random_random,
    absl_random_bit_gen_ref,
    # "@com_google_googletest//:gtest_prod",
]

hybrid_scheduling_policy_cpp_lib = static_library(
    'hybrid_scheduling_policy_cpp_lib',
    ['policy/hybrid_scheduling_policy.cc'],
    install: false,
    dependencies: hybrid_scheduling_policy_cpp_lib_dependencies
)

hybrid_scheduling_policy_cpp_lib_dep = declare_dependency(
    link_with: [hybrid_scheduling_policy_cpp_lib],
    dependencies: hybrid_scheduling_policy_cpp_lib_dependencies
)
# === @end: hybrid_scheduling_policy_cpp_lib (@original: hybrid_scheduling_policy) ===


# === @begin: affinity_with_bundle_scheduling_policy_cpp_lib (@original: affinity_with_bundle_scheduling_policy) ===
affinity_with_bundle_scheduling_policy_cpp_lib_dependencies = [
    # Package-managed
    scheduling_policy_cpp_lib_dep,
    bundle_location_index_cpp_lib_dep,
]

affinity_with_bundle_scheduling_policy_cpp_lib = static_library(
    'affinity_with_bundle_scheduling_policy_cpp_lib',
    ['policy/affinity_with_bundle_scheduling_policy.cc'],
    install: false,
    dependencies: affinity_with_bundle_scheduling_policy_cpp_lib_dependencies
)

affinity_with_bundle_scheduling_policy_cpp_lib_dep = declare_dependency(
    link_with: [affinity_with_bundle_scheduling_policy_cpp_lib],
    dependencies: affinity_with_bundle_scheduling_policy_cpp_lib_dependencies
)
# === @end: affinity_with_bundle_scheduling_policy_cpp_lib (@original: affinity_with_bundle_scheduling_policy) ===


# === @begin: node_label_scheduling_policy_cpp_lib (@original: node_label_scheduling_policy) ===
node_label_scheduling_policy_cpp_lib_dependencies = [
    # Package-managed
    scheduling_context_cpp_lib_dep,
    scheduling_policy_cpp_lib_dep,
]

node_label_scheduling_policy_cpp_lib = static_library(
    'node_label_scheduling_policy_cpp_lib',
    ['policy/node_label_scheduling_policy.cc'],
    install: false,
    dependencies: node_label_scheduling_policy_cpp_lib_dependencies
)

node_label_scheduling_policy_cpp_lib_dep = declare_dependency(
    link_with: [node_label_scheduling_policy_cpp_lib],
    dependencies: node_label_scheduling_policy_cpp_lib_dependencies
)
# === @end: node_label_scheduling_policy_cpp_lib (@original: node_label_scheduling_policy) ===


# === @begin: random_scheduling_policy_cpp_lib (@original: random_scheduling_policy) ===
random_scheduling_policy_cpp_lib_dependencies = [
    # Package-managed
    scheduling_policy_cpp_lib_dep,
]

random_scheduling_policy_cpp_lib = static_library(
    'random_scheduling_policy_cpp_lib',
    ['policy/random_scheduling_policy.cc'],
    install: false,
    dependencies: random_scheduling_policy_cpp_lib_dependencies
)

random_scheduling_policy_cpp_lib_dep = declare_dependency(
    link_with: [random_scheduling_policy_cpp_lib],
    dependencies: random_scheduling_policy_cpp_lib_dependencies
)
# === @end: random_scheduling_policy_cpp_lib (@original: random_scheduling_policy) ===


# === @begin: bundle_scheduling_policy_cpp_lib (@original: bundle_scheduling_policy) ===
bundle_scheduling_policy_cpp_lib_dependencies = [
    # Package-managed
    cluster_resource_manager_cpp_lib_dep,
    scheduling_context_cpp_lib_dep,
    scheduling_policy_cpp_lib_dep,
    scorer_cpp_lib_dep,
]

bundle_scheduling_policy_cpp_lib = static_library(
    'bundle_scheduling_policy_cpp_lib',
    ['policy/bundle_scheduling_policy.cc'],
    install: false,
    dependencies: bundle_scheduling_policy_cpp_lib_dependencies
)

bundle_scheduling_policy_cpp_lib_dep = declare_dependency(
    link_with: [bundle_scheduling_policy_cpp_lib],
    dependencies: bundle_scheduling_policy_cpp_lib_dependencies
)
# === @end: bundle_scheduling_policy_cpp_lib (@original: bundle_scheduling_policy) ===


# === @begin: node_affinity_scheduling_policy_cpp_lib (@original: node_affinity_scheduling_policy) ===
node_affinity_scheduling_policy_cpp_lib_dependencies = [
    # Package-managed
    hybrid_scheduling_policy_cpp_lib_dep,
    scheduling_policy_cpp_lib_dep,
]

node_affinity_scheduling_policy_cpp_lib = static_library(
    'node_affinity_scheduling_policy_cpp_lib',
    ['policy/node_affinity_scheduling_policy.cc'],
    install: false,
    dependencies: node_affinity_scheduling_policy_cpp_lib_dependencies
)

node_affinity_scheduling_policy_cpp_lib_dep = declare_dependency(
    link_with: [node_affinity_scheduling_policy_cpp_lib],
    dependencies: node_affinity_scheduling_policy_cpp_lib_dependencies
)
# === @end: node_affinity_scheduling_policy_cpp_lib (@original: node_affinity_scheduling_policy) ===


# === @begin: spread_scheduling_policy_cpp_lib (@original: spread_scheduling_policy) ===
spread_scheduling_policy_cpp_lib_dependencies = [
    # Package-managed
    hybrid_scheduling_policy_cpp_lib_dep,
    scheduling_policy_cpp_lib_dep,
    container_util_cpp_lib_dep,
]

spread_scheduling_policy_cpp_lib = static_library(
    'spread_scheduling_policy_cpp_lib',
    ['policy/spread_scheduling_policy.cc'],
    install: false,
    dependencies: spread_scheduling_policy_cpp_lib_dependencies
)

spread_scheduling_policy_cpp_lib_dep = declare_dependency(
    link_with: [spread_scheduling_policy_cpp_lib],
    dependencies: spread_scheduling_policy_cpp_lib_dependencies
)
# === @end: spread_scheduling_policy_cpp_lib (@original: spread_scheduling_policy) ===


# === @begin: composite_scheduling_policy_cpp_lib (@original: composite_scheduling_policy) ===
composite_scheduling_policy_cpp_lib_dependencies = [
    # Package-managed
    affinity_with_bundle_scheduling_policy_cpp_lib_dep,
    bundle_scheduling_policy_cpp_lib_dep,
    cluster_resource_manager_cpp_lib_dep,
    hybrid_scheduling_policy_cpp_lib_dep,
    node_affinity_scheduling_policy_cpp_lib_dep,
    node_label_scheduling_policy_cpp_lib_dep,
    random_scheduling_policy_cpp_lib_dep,
    spread_scheduling_policy_cpp_lib_dep,
]

composite_scheduling_policy_cpp_lib = static_library(
    'composite_scheduling_policy_cpp_lib',
    ['policy/composite_scheduling_policy.cc'],
    install: false,
    dependencies: composite_scheduling_policy_cpp_lib_dependencies
)

composite_scheduling_policy_cpp_lib_dep = declare_dependency(
    link_with: [composite_scheduling_policy_cpp_lib],
    dependencies: composite_scheduling_policy_cpp_lib_dependencies
)
# === @end: composite_scheduling_policy_cpp_lib (@original: composite_scheduling_policy) ===


# === @begin: cluster_resource_scheduler_cpp_lib (@original: cluster_resource_scheduler) ===
cluster_resource_scheduler_cpp_lib_dependencies = [
    # Package-managed
    cluster_resource_manager_cpp_lib_dep,
    composite_scheduling_policy_cpp_lib_dep,
    scheduler_internal_cpp_lib_dep,
    # `metric_interface_cpp_lib_dep` is NOT AVAILABLE (processed after): //src/ray/observability:metric_interface
    gcs_proto_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    # gtest_prod: handled by vendored stub (FRIEND_TEST only)
]

cluster_resource_scheduler_cpp_lib = static_library(
    'cluster_resource_scheduler_cpp_lib',
    ['cluster_resource_scheduler.cc'],
    install: false,
    dependencies: cluster_resource_scheduler_cpp_lib_dependencies
)

cluster_resource_scheduler_cpp_lib_dep = declare_dependency(
    link_with: [cluster_resource_scheduler_cpp_lib],
    dependencies: cluster_resource_scheduler_cpp_lib_dependencies
)
# === @end: cluster_resource_scheduler_cpp_lib (@original: cluster_resource_scheduler) ===


# === @begin: local_lease_manager_cpp_lib (@original: local_lease_manager) ===
local_lease_manager_cpp_lib_dependencies = [
    # Package-managed
    lease_cpp_lib_dep,
    ray_object_cpp_lib_dep,
    cluster_resource_data_cpp_lib_dep,
    placement_group_util_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    # `lease_dependency_manager_cpp_lib_dep` is NOT AVAILABLE (processed after): //src/ray/raylet:lease_dependency_manager. It is resolved at final link time via _raylet shared library.
    # `raylet_metrics_cpp_lib_dep` is NOT AVAILABLE (processed after): //src/ray/raylet:metrics. It is resolved at final link time via _raylet shared library.
    # `worker_interface_cpp_lib_dep` is NOT AVAILABLE (processed after): //src/ray/raylet:worker_interface. It is resolved at final link time via _raylet shared library.
    # `worker_pool_cpp_lib_dep` is NOT AVAILABLE (processed after): //src/ray/raylet:worker_pool. It is resolved at final link time via _raylet shared library.
    cluster_resource_scheduler_cpp_lib_dep,
    local_lease_manager_interface_cpp_lib_dep,
    scheduler_internal_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

local_lease_manager_cpp_lib = static_library(
    'local_lease_manager_cpp_lib',
    ['local_lease_manager.cc'],
    install: false,
    dependencies: local_lease_manager_cpp_lib_dependencies
)

local_lease_manager_cpp_lib_dep = declare_dependency(
    link_with: [local_lease_manager_cpp_lib],
    dependencies: local_lease_manager_cpp_lib_dependencies
)
# === @end: local_lease_manager_cpp_lib (@original: local_lease_manager) ===


# === @begin: cluster_lease_manager_cpp_lib (@original: cluster_lease_manager) ===
# NOTE: Bazel target also includes scheduler_stats.cc in its srcs.
cluster_lease_manager_cpp_lib_dependencies = [
    # Package-managed
    cluster_lease_manager_interface_cpp_lib_dep,
    cluster_resource_scheduler_cpp_lib_dep,
    local_lease_manager_interface_cpp_lib_dep,
    scheduler_internal_cpp_lib_dep,
    scheduler_resource_reporter_cpp_lib_dep,
    lease_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    stats_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

cluster_lease_manager_cpp_lib = static_library(
    'cluster_lease_manager_cpp_lib',
    [
        'cluster_lease_manager.cc',
        'scheduler_stats.cc',
    ],
    install: false,
    dependencies: cluster_lease_manager_cpp_lib_dependencies
)

cluster_lease_manager_cpp_lib_dep = declare_dependency(
    link_with: [cluster_lease_manager_cpp_lib],
    dependencies: cluster_lease_manager_cpp_lib_dependencies
)
# === @end: cluster_lease_manager_cpp_lib (@original: cluster_lease_manager) ===


# === @begin: scheduler_cpp_lib (@original: scheduler) ===
# Umbrella target — no srcs, just deps on all scheduling sub-targets.
# Bazel uses linkopts for -lpthread; in Meson we use 'threads' dep.
scheduler_cpp_lib_dep = declare_dependency(
    dependencies: [
        affinity_with_bundle_scheduling_policy_cpp_lib_dep,
        bundle_scheduling_policy_cpp_lib_dep,
        cluster_lease_manager_cpp_lib_dep,
        cluster_resource_manager_cpp_lib_dep,
        cluster_resource_scheduler_cpp_lib_dep,
        composite_scheduling_policy_cpp_lib_dep,
        hybrid_scheduling_policy_cpp_lib_dep,
        local_lease_manager_cpp_lib_dep,
        local_resource_manager_cpp_lib_dep,
        node_affinity_scheduling_policy_cpp_lib_dep,
        node_label_scheduling_policy_cpp_lib_dep,
        random_scheduling_policy_cpp_lib_dep,
        spread_scheduling_policy_cpp_lib_dep,

        # System-managed
        threads, # -lpthread
    ]
)
# === @end: scheduler_cpp_lib (@original: scheduler) ===

