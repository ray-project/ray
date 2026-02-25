# Proto codegen targets for src/ray/protobuf/public/
#
# @IMPORTANT: This is split into two parts:
# Part 1 (this file): events_* codegen-only custom_targets (no _cpp_dep, no static_library)
#   + runtime_environment.proto — full Pattern 4 (no parent deps)
#   Order matches BUILD.bazel (events_* first, runtime_environment last).
# Part 2 (parent `protobuf/meson.build`): `events_*` `_cpp_dep` + static_libraries
#   (defined after `common_proto_cpp_dep` is available, so import deps can be listed)
#
# This split is required because:
# - parent's `common.proto` imports `public/runtime_environment.proto`
# - public's `events_*.proto` import parent's `common.proto`
# So `public/` must be subdir'd before common, but events can only be compiled after common.
#
# @IMPORTANT: The events_* custom_targets below have no Meson `depends:`. This is intentional.
# protoc resolves `import` statements at codegen time via `-I` include paths baked into
# `proto_cpp_default_kwargs` — it finds all imported `.proto` files on disk directly, so the
# custom_target doesn't need Meson depends: to order code generation. The actual import
# dependency tracking happens one layer up, on the `_cpp_dep` declarations in the parent
# `meson.build` — that's where `events_base_event_proto_cpp_dep` lists all 9 events +
# `events_task_profile_events_proto_cpp_dep` as dependencies, so the C++ compiler can find
# the generated `.pb.h` headers when compiling consumers.


# ==========================================================================
# events_*.proto — CODEGEN ONLY (custom_target only, no _cpp_dep)
# _cpp_dep and static_libraries are defined in parent protobuf/meson.build
# after common_proto_cpp_dep is available, so import deps can be properly listed.
# ==========================================================================

# === @begin: events_base_event.proto (deps: all events_* + events_task_profile_events + timestamp) ===
events_base_event_proto = ['events_base_event.proto']

events_base_event_proto_cpp = custom_target(
    input: events_base_event_proto,
    kwargs: proto_cpp_default_kwargs
)
# === @end: events_base_event.proto ===


# === @begin: events_actor_task_definition_event.proto (deps: runtime_environment, common) ===
events_actor_task_definition_event_proto = ['events_actor_task_definition_event.proto']

events_actor_task_definition_event_proto_cpp = custom_target(
    input: events_actor_task_definition_event_proto,
    kwargs: proto_cpp_default_kwargs
)
# === @end: events_actor_task_definition_event.proto ===


# === @begin: events_task_definition_event.proto (deps: runtime_environment, common) ===
events_task_definition_event_proto = ['events_task_definition_event.proto']

events_task_definition_event_proto_cpp = custom_target(
    input: events_task_definition_event_proto,
    kwargs: proto_cpp_default_kwargs
)
# === @end: events_task_definition_event.proto ===


# === @begin: events_task_lifecycle_event.proto (deps: common, timestamp) ===
events_task_lifecycle_event_proto = ['events_task_lifecycle_event.proto']

events_task_lifecycle_event_proto_cpp = custom_target(
    input: events_task_lifecycle_event_proto,
    kwargs: proto_cpp_default_kwargs
)
# === @end: events_task_lifecycle_event.proto ===


# === @begin: events_driver_job_definition_event.proto (deps: runtime_environment, common, timestamp) ===
events_driver_job_definition_event_proto = ['events_driver_job_definition_event.proto']

events_driver_job_definition_event_proto_cpp = custom_target(
    input: events_driver_job_definition_event_proto,
    kwargs: proto_cpp_default_kwargs
)
# === @end: events_driver_job_definition_event.proto ===


# === @begin: events_driver_job_lifecycle_event.proto (deps: timestamp only) ===
events_driver_job_lifecycle_event_proto = ['events_driver_job_lifecycle_event.proto']

events_driver_job_lifecycle_event_proto_cpp = custom_target(
    input: events_driver_job_lifecycle_event_proto,
    kwargs: proto_cpp_default_kwargs
)
# === @end: events_driver_job_lifecycle_event.proto ===


# === @begin: events_actor_definition_event.proto (deps: common) ===
events_actor_definition_event_proto = ['events_actor_definition_event.proto']

events_actor_definition_event_proto_cpp = custom_target(
    input: events_actor_definition_event_proto,
    kwargs: proto_cpp_default_kwargs
)
# === @end: events_actor_definition_event.proto ===


# === @begin: events_actor_lifecycle_event.proto (deps: common, timestamp) ===
events_actor_lifecycle_event_proto = ['events_actor_lifecycle_event.proto']

events_actor_lifecycle_event_proto_cpp = custom_target(
    input: events_actor_lifecycle_event_proto,
    kwargs: proto_cpp_default_kwargs
)
# === @end: events_actor_lifecycle_event.proto ===


# === @begin: events_node_definition_event.proto (deps: timestamp only) ===
events_node_definition_event_proto = ['events_node_definition_event.proto']

events_node_definition_event_proto_cpp = custom_target(
    input: events_node_definition_event_proto,
    kwargs: proto_cpp_default_kwargs
)
# === @end: events_node_definition_event.proto ===


# === @begin: events_node_lifecycle_event.proto (deps: timestamp only) ===
events_node_lifecycle_event_proto = ['events_node_lifecycle_event.proto']

events_node_lifecycle_event_proto_cpp = custom_target(
    input: events_node_lifecycle_event_proto,
    kwargs: proto_cpp_default_kwargs
)
# === @end: events_node_lifecycle_event.proto ===


# ==========================================================================
# events_*.proto — PYTHON CODEGEN
# Generates *_pb2.py/*_pb2_grpc.py for all events_* protos.
# These are consumed by Python dashboard aggregator code at runtime.
# ==========================================================================

# @begin: proto_py (@original: core_py_proto — events_base_event)
events_base_event_proto_py = custom_target(
    input: events_base_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py

# @begin: proto_py (@original: core_py_proto — events_actor_task_definition_event)
events_actor_task_definition_event_proto_py = custom_target(
    input: events_actor_task_definition_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py

# @begin: proto_py (@original: core_py_proto — events_task_definition_event)
events_task_definition_event_proto_py = custom_target(
    input: events_task_definition_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py

# @begin: proto_py (@original: core_py_proto — events_task_lifecycle_event)
events_task_lifecycle_event_proto_py = custom_target(
    input: events_task_lifecycle_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py

# @begin: proto_py (@original: core_py_proto — events_driver_job_definition_event)
events_driver_job_definition_event_proto_py = custom_target(
    input: events_driver_job_definition_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py

# @begin: proto_py (@original: core_py_proto — events_driver_job_lifecycle_event)
events_driver_job_lifecycle_event_proto_py = custom_target(
    input: events_driver_job_lifecycle_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py

# @begin: proto_py (@original: core_py_proto — events_actor_definition_event)
events_actor_definition_event_proto_py = custom_target(
    input: events_actor_definition_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py

# @begin: proto_py (@original: core_py_proto — events_actor_lifecycle_event)
events_actor_lifecycle_event_proto_py = custom_target(
    input: events_actor_lifecycle_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py

# @begin: proto_py (@original: core_py_proto — events_node_definition_event)
events_node_definition_event_proto_py = custom_target(
    input: events_node_definition_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py

# @begin: proto_py (@original: core_py_proto — events_node_lifecycle_event)
events_node_lifecycle_event_proto_py = custom_target(
    input: events_node_lifecycle_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py


# ==========================================================================
# runtime_environment.proto — full Pattern 4 (no imports, no parent deps)
# Last in BUILD.bazel; no define-before-use constraint forces it earlier.
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

all_proto_cpp_libs_dependencies += [runtime_environment_proto_cpp_lib_dep]
# @end: proto_cpp (original: runtime_environment_cc_proto)

# @begin: proto_py (@original: core_py_proto)
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
runtime_environment_proto_py = custom_target(
    input: runtime_environment_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: runtime_environment.proto ===


