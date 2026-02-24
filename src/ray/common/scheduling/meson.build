# IMPORTANT: Ordering within this file respects the internal dependency graph.
# You will see things "out of order" from a BUILD.bazel perspective because
# Meson requires define-before-use: if target A depends on target B and both
# are in this file, B must appear first regardless of Bazel ordering.
# Specifically: resource_instance_set and fallback_strategy are moved before
# cluster_resource_data and scheduling_class_util (which depend on them).


# === @begin: scheduling_ids_cpp_lib (@original: scheduling_ids) ===
scheduling_ids_cpp_lib_dependencies = [
    # Package-managed
    constants_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    boost_algorithm,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_strings,
    absl_synchronization,
]

scheduling_ids_cpp_lib = static_library(
    'scheduling_ids_cpp_lib',
    ['scheduling_ids.cc'],
    install: false,
    dependencies: scheduling_ids_cpp_lib_dependencies
)

scheduling_ids_cpp_lib_dep = declare_dependency(
    link_with: [scheduling_ids_cpp_lib],
    dependencies: scheduling_ids_cpp_lib_dependencies
)
# === @end: scheduling_ids_cpp_lib (@original: scheduling_ids) ===


# === @begin: label_selector_cpp_lib (@original: label_selector) ===
label_selector_cpp_lib_dependencies = [
    # Package-managed
    constants_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_set,
    absl_strings,
    protobuf,
]

label_selector_cpp_lib = static_library(
    'label_selector_cpp_lib',
    ['label_selector.cc'],
    install: false,
    dependencies: label_selector_cpp_lib_dependencies
)

label_selector_cpp_lib_dep = declare_dependency(
    link_with: [label_selector_cpp_lib],
    dependencies: label_selector_cpp_lib_dependencies
)
# === @end: label_selector_cpp_lib (@original: label_selector) ===


# === @begin: fixed_point_cpp_lib (@original: fixed_point) ===
fixed_point_cpp_lib_dependencies = [
    # Package-managed
    constants_cpp_lib_dep,
]

fixed_point_cpp_lib = static_library(
    'fixed_point_cpp_lib',
    ['fixed_point.cc'],
    install: false,
    dependencies: fixed_point_cpp_lib_dependencies
)

fixed_point_cpp_lib_dep = declare_dependency(
    link_with: [fixed_point_cpp_lib],
    dependencies: fixed_point_cpp_lib_dependencies
)
# === @end: fixed_point_cpp_lib (@original: fixed_point) ===


# === @begin: placement_group_util_cpp_lib (@original: placement_group_util) ===
placement_group_util_cpp_lib_dependencies = [
    # Package-managed
    scheduling_ids_cpp_lib_dep,
    logging_cpp_lib_dep,
]

placement_group_util_cpp_lib = static_library(
    'placement_group_util_cpp_lib',
    ['placement_group_util.cc'],
    install: false,
    dependencies: placement_group_util_cpp_lib_dependencies
)

placement_group_util_cpp_lib_dep = declare_dependency(
    link_with: [placement_group_util_cpp_lib],
    dependencies: placement_group_util_cpp_lib_dependencies
)
# === @end: placement_group_util_cpp_lib (@original: placement_group_util) ===


# === @begin: resource_set_cpp_lib (@original: resource_set) ===
resource_set_cpp_lib_dependencies = [
    # Package-managed
    fixed_point_cpp_lib_dep,
    scheduling_ids_cpp_lib_dep,

    # Eugo-managed
    boost_range,
    absl_container_flat_hash_map,
]

resource_set_cpp_lib = static_library(
    'resource_set_cpp_lib',
    ['resource_set.cc'],
    install: false,
    dependencies: resource_set_cpp_lib_dependencies
)

resource_set_cpp_lib_dep = declare_dependency(
    link_with: [resource_set_cpp_lib],
    dependencies: resource_set_cpp_lib_dependencies
)
# === @end: resource_set_cpp_lib (@original: resource_set) ===


# === @begin: resource_instance_set_cpp_lib (@original: resource_instance_set) ===
# Moved before cluster_resource_data (define-before-use constraint)
resource_instance_set_cpp_lib_dependencies = [
    # Package-managed
    fixed_point_cpp_lib_dep,
    placement_group_util_cpp_lib_dep,
    resource_set_cpp_lib_dep,
    scheduling_ids_cpp_lib_dep,
    container_util_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_strings,
]

resource_instance_set_cpp_lib = static_library(
    'resource_instance_set_cpp_lib',
    ['resource_instance_set.cc'],
    install: false,
    dependencies: resource_instance_set_cpp_lib_dependencies
)

resource_instance_set_cpp_lib_dep = declare_dependency(
    link_with: [resource_instance_set_cpp_lib],
    dependencies: resource_instance_set_cpp_lib_dependencies
)
# === @end: resource_instance_set_cpp_lib (@original: resource_instance_set) ===


# === @begin: fallback_strategy_cpp_lib (@original: fallback_strategy) ===
# Moved before scheduling_class_util (define-before-use constraint)
fallback_strategy_cpp_lib_dependencies = [
    # Package-managed
    label_selector_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_hash,
]

fallback_strategy_cpp_lib = static_library(
    'fallback_strategy_cpp_lib',
    ['fallback_strategy.cc'],
    install: false,
    dependencies: fallback_strategy_cpp_lib_dependencies
)

fallback_strategy_cpp_lib_dep = declare_dependency(
    link_with: [fallback_strategy_cpp_lib],
    dependencies: fallback_strategy_cpp_lib_dependencies
)
# === @end: fallback_strategy_cpp_lib (@original: fallback_strategy) ===


# === @begin: cluster_resource_data_cpp_lib (@original: cluster_resource_data) ===
cluster_resource_data_cpp_lib_dependencies = [
    # Package-managed
    fixed_point_cpp_lib_dep,
    label_selector_cpp_lib_dep,
    resource_instance_set_cpp_lib_dep,
    resource_set_cpp_lib_dep,
    scheduling_ids_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    boost_range,
    absl_container_flat_hash_map,
    absl_time,
]

cluster_resource_data_cpp_lib = static_library(
    'cluster_resource_data_cpp_lib',
    ['cluster_resource_data.cc'],
    install: false,
    dependencies: cluster_resource_data_cpp_lib_dependencies
)

cluster_resource_data_cpp_lib_dep = declare_dependency(
    link_with: [cluster_resource_data_cpp_lib],
    dependencies: cluster_resource_data_cpp_lib_dependencies
)
# === @end: cluster_resource_data_cpp_lib (@original: cluster_resource_data) ===


# === @begin: scheduling_class_util_cpp_lib (@original: scheduling_class_util) ===
scheduling_class_util_cpp_lib_dependencies = [
    # Package-managed (local)
    fallback_strategy_cpp_lib_dep,
    label_selector_cpp_lib_dep,
    resource_set_cpp_lib_dep,

    # Package-managed (from parent: common/)
    function_descriptor_cpp_lib_dep,
    runtime_env_cpp_lib_dep,

    # Package-managed (from protobuf/)
    common_proto_cpp_lib_dep,

    # Package-managed (from util/)
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_synchronization,
    protobuf,
]

scheduling_class_util_cpp_lib = static_library(
    'scheduling_class_util_cpp_lib',
    ['scheduling_class_util.cc'],
    install: false,
    dependencies: scheduling_class_util_cpp_lib_dependencies
)

scheduling_class_util_cpp_lib_dep = declare_dependency(
    link_with: [scheduling_class_util_cpp_lib],
    dependencies: scheduling_class_util_cpp_lib_dependencies
)
# === @end: scheduling_class_util_cpp_lib (@original: scheduling_class_util) ===