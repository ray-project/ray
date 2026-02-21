# Proto codegen targets for src/ray/protobuf/public/
#
# NOTE: This is split into two parts:
# Part 1 (this file): runtime_environment.proto — full Pattern 4 (no parent deps)
#   + events_* codegen-only custom_targets and _cpp_dep (no static_library)
# Part 2 (parent protobuf/meson.build): events_* static_libraries
#   (defined after common_proto_cpp_dep is available)
#
# This split is required because:
# - parent's common.proto imports public/runtime_environment.proto
# - public's events_*.proto import parent's common.proto
# So public/ must be subdir'd before common, but events can only be compiled after common.


# ==========================================================================
# runtime_environment.proto — no imports, no parent deps
# ==========================================================================

# === @begin: runtime_environment.proto ===
runtime_environment_proto = ['runtime_environment.proto']

# @begin: proto_cpp (original: runtime_environment_cc_proto)
runtime_environment_proto_cpp = custom_target(
    input: runtime_environment_proto,
    kwargs: proto_cpp_default_kwargs
)

runtime_environment_proto_cpp_dep = declare_dependency(
    sources: runtime_environment_proto_cpp
)

runtime_environment_proto_cpp_lib_dependencies = [
    # Package-managed
    runtime_environment_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

runtime_environment_proto_cpp_lib = static_library(
    'runtime_environment_proto_cpp_lib',
    dependencies: runtime_environment_proto_cpp_lib_dependencies,
    install: false
)

runtime_environment_proto_cpp_lib_dep = declare_dependency(
    link_with: [runtime_environment_proto_cpp_lib],
    dependencies: runtime_environment_proto_cpp_lib_dependencies
)
# @end: proto_cpp (original: runtime_environment_cc_proto)
# === @end: runtime_environment.proto ===


# ==========================================================================
# events_*.proto — CODEGEN ONLY (custom_target + _cpp_dep)
# Static libraries are defined in parent protobuf/meson.build after
# common_proto_cpp_dep and events_task_profile_events_proto_cpp_dep
# are available.
# ==========================================================================

# === @begin: events_driver_job_lifecycle_event.proto (deps: timestamp only) ===
events_driver_job_lifecycle_event_proto = ['events_driver_job_lifecycle_event.proto']

events_driver_job_lifecycle_event_proto_cpp = custom_target(
    input: events_driver_job_lifecycle_event_proto,
    kwargs: proto_cpp_default_kwargs
)

events_driver_job_lifecycle_event_proto_cpp_dep = declare_dependency(
    sources: events_driver_job_lifecycle_event_proto_cpp
)
# === @end: events_driver_job_lifecycle_event.proto ===


# === @begin: events_node_definition_event.proto (deps: timestamp only) ===
events_node_definition_event_proto = ['events_node_definition_event.proto']

events_node_definition_event_proto_cpp = custom_target(
    input: events_node_definition_event_proto,
    kwargs: proto_cpp_default_kwargs
)

events_node_definition_event_proto_cpp_dep = declare_dependency(
    sources: events_node_definition_event_proto_cpp
)
# === @end: events_node_definition_event.proto ===


# === @begin: events_node_lifecycle_event.proto (deps: timestamp only) ===
events_node_lifecycle_event_proto = ['events_node_lifecycle_event.proto']

events_node_lifecycle_event_proto_cpp = custom_target(
    input: events_node_lifecycle_event_proto,
    kwargs: proto_cpp_default_kwargs
)

events_node_lifecycle_event_proto_cpp_dep = declare_dependency(
    sources: events_node_lifecycle_event_proto_cpp
)
# === @end: events_node_lifecycle_event.proto ===


# === @begin: events_actor_definition_event.proto (deps: common) ===
events_actor_definition_event_proto = ['events_actor_definition_event.proto']

events_actor_definition_event_proto_cpp = custom_target(
    input: events_actor_definition_event_proto,
    kwargs: proto_cpp_default_kwargs
)

events_actor_definition_event_proto_cpp_dep = declare_dependency(
    sources: events_actor_definition_event_proto_cpp
)
# === @end: events_actor_definition_event.proto ===


# === @begin: events_actor_lifecycle_event.proto (deps: common, timestamp) ===
events_actor_lifecycle_event_proto = ['events_actor_lifecycle_event.proto']

events_actor_lifecycle_event_proto_cpp = custom_target(
    input: events_actor_lifecycle_event_proto,
    kwargs: proto_cpp_default_kwargs
)

events_actor_lifecycle_event_proto_cpp_dep = declare_dependency(
    sources: events_actor_lifecycle_event_proto_cpp
)
# === @end: events_actor_lifecycle_event.proto ===


# === @begin: events_task_lifecycle_event.proto (deps: common, timestamp) ===
events_task_lifecycle_event_proto = ['events_task_lifecycle_event.proto']

events_task_lifecycle_event_proto_cpp = custom_target(
    input: events_task_lifecycle_event_proto,
    kwargs: proto_cpp_default_kwargs
)

events_task_lifecycle_event_proto_cpp_dep = declare_dependency(
    sources: events_task_lifecycle_event_proto_cpp
)
# === @end: events_task_lifecycle_event.proto ===


# === @begin: events_actor_task_definition_event.proto (deps: runtime_environment, common) ===
events_actor_task_definition_event_proto = ['events_actor_task_definition_event.proto']

events_actor_task_definition_event_proto_cpp = custom_target(
    input: events_actor_task_definition_event_proto,
    kwargs: proto_cpp_default_kwargs
)

events_actor_task_definition_event_proto_cpp_dep = declare_dependency(
    sources: events_actor_task_definition_event_proto_cpp
)
# === @end: events_actor_task_definition_event.proto ===


# === @begin: events_task_definition_event.proto (deps: runtime_environment, common) ===
events_task_definition_event_proto = ['events_task_definition_event.proto']

events_task_definition_event_proto_cpp = custom_target(
    input: events_task_definition_event_proto,
    kwargs: proto_cpp_default_kwargs
)

events_task_definition_event_proto_cpp_dep = declare_dependency(
    sources: events_task_definition_event_proto_cpp
)
# === @end: events_task_definition_event.proto ===


# === @begin: events_driver_job_definition_event.proto (deps: runtime_environment, common, timestamp) ===
events_driver_job_definition_event_proto = ['events_driver_job_definition_event.proto']

events_driver_job_definition_event_proto_cpp = custom_target(
    input: events_driver_job_definition_event_proto,
    kwargs: proto_cpp_default_kwargs
)

events_driver_job_definition_event_proto_cpp_dep = declare_dependency(
    sources: events_driver_job_definition_event_proto_cpp
)
# === @end: events_driver_job_definition_event.proto ===


# === @begin: events_base_event.proto (deps: all events_* above + events_task_profile_events from parent + timestamp) ===
events_base_event_proto = ['events_base_event.proto']

events_base_event_proto_cpp = custom_target(
    input: events_base_event_proto,
    kwargs: proto_cpp_default_kwargs
)

events_base_event_proto_cpp_dep = declare_dependency(
    sources: events_base_event_proto_cpp
)
# === @end: events_base_event.proto ===
