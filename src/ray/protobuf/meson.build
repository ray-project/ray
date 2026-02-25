# @IMPORTANT: 1. Ordering within this file respects the internal dependency graph.
# You will also see things "out of order" from a BUILD.bazel perspective,
# @NOTE: `experimental/` subdir removed — `instance_manager.proto` moved to this top level file (see below)
# @NOTE: `export_api/` subdir removed — `export_*.proto` files moved to this top level file (see below)
# @IMPORTANT: Bazel implicitly links @com_google_protobuf//:protobuf — it's a built-in rule that always pulls in the protobuf runtime without you having to list it.
# Meson has no such implicit behavior, so we must add protobuf explicitly to every *_proto_cpp_lib_dependencies list

# === @begin: runtime_env_common.proto ===
runtime_env_common_proto = ['runtime_env_common.proto']

# @begin: proto_cpp (original: runtime_env_common_cc_proto)
runtime_env_common_proto_cpp = custom_target(
    input: runtime_env_common_proto,
    kwargs: proto_cpp_default_kwargs
)

runtime_env_common_proto_cpp_dep = declare_dependency(
    sources: runtime_env_common_proto_cpp
)

runtime_env_common_proto_cpp_lib_dependencies = [
    # Package-managed
    runtime_env_common_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

runtime_env_common_proto_cpp_lib = static_library(
    'runtime_env_common_proto_cpp_lib',
    dependencies: runtime_env_common_proto_cpp_lib_dependencies,
    install: false
)

runtime_env_common_proto_cpp_lib_dep = declare_dependency(
    link_with: [runtime_env_common_proto_cpp_lib],
    dependencies: runtime_env_common_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [runtime_env_common_proto_cpp_lib_dep]
# @end: proto_cpp (original: runtime_env_common_cc_proto)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
runtime_env_common_proto_py = custom_target(
    input: runtime_env_common_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: runtime_env_common.proto ===


# === @begin: public/ (runtime_environment.proto + events_* codegen) ===
# MUST come before common.proto because common.proto imports public/runtime_environment.proto.
# Events static_libraries are defined later in this file (after common_proto_cpp_dep is available).
subdir('public/')
# === @end: public/ ===


# === @begin: common.proto ===
common_proto = ['common.proto']

# @begin: proto_cpp (original: common_cc_proto)
common_proto_cpp = custom_target(
    input: common_proto,
    kwargs: proto_cpp_default_kwargs
)

common_proto_cpp_dep = declare_dependency(
    sources: common_proto_cpp,
    dependencies: [runtime_environment_proto_cpp_dep]
)

common_proto_cpp_lib_dependencies = [
    # Package-managed
    common_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

common_proto_cpp_lib = static_library(
    'common_proto_cpp_lib',
    dependencies: common_proto_cpp_lib_dependencies,
    install: false
)

common_proto_cpp_lib_dep = declare_dependency(
    link_with: [common_proto_cpp_lib],
    dependencies: common_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [common_proto_cpp_lib_dep]
# @end: proto_cpp (original: common_cc_proto)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
common_proto_py = custom_target(
    input: common_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: common.proto ===


# === @begin: autoscaler.proto ===
autoscaler_proto = ['autoscaler.proto']

# @begin: proto_cpp (original: autoscaler_cc_proto)
autoscaler_proto_cpp = custom_target(
    input: autoscaler_proto,
    kwargs: proto_cpp_default_kwargs
)

autoscaler_proto_cpp_dep = declare_dependency(
    sources: autoscaler_proto_cpp,
    dependencies: [common_proto_cpp_dep]
)

autoscaler_proto_cpp_lib_dependencies = [
    # Package-managed
    autoscaler_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

autoscaler_proto_cpp_lib = static_library(
    'autoscaler_proto_cpp_lib',
    dependencies: autoscaler_proto_cpp_lib_dependencies,
    install: false
)

autoscaler_proto_cpp_lib_dep = declare_dependency(
    link_with: [autoscaler_proto_cpp_lib],
    dependencies: autoscaler_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [autoscaler_proto_cpp_lib_dep]
# @end: proto_cpp (original: autoscaler_cc_proto)

# @begin: proto_cpp_grpc (original: autoscaler_cc_grpc)
autoscaler_proto_cpp_grpc = custom_target(
    input: autoscaler_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

autoscaler_proto_cpp_grpc_dep = declare_dependency(
    sources: autoscaler_proto_cpp_grpc
)

autoscaler_proto_cpp_grpc_lib_dependencies = [
    # Package-managed
    autoscaler_proto_cpp_grpc_dep,
    autoscaler_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
    grpc
]

autoscaler_proto_cpp_grpc_lib = static_library(
    'autoscaler_proto_cpp_grpc_lib',
    dependencies: autoscaler_proto_cpp_grpc_lib_dependencies,
    install: false
)

autoscaler_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [autoscaler_proto_cpp_grpc_lib],
    dependencies: autoscaler_proto_cpp_grpc_lib_dependencies
)

all_proto_cpp_grpc_libs_dependencies += [autoscaler_proto_cpp_grpc_lib_dep]
# @end: proto_cpp_grpc (original: autoscaler_cc_grpc)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
autoscaler_proto_py = custom_target(
    input: autoscaler_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: autoscaler.proto ===


# === @begin: ray_syncer.proto ===
ray_syncer_proto = ['ray_syncer.proto']

# @begin: proto_cpp (original: ray_syncer_cc_proto)
ray_syncer_proto_cpp = custom_target(
    input: ray_syncer_proto,
    kwargs: proto_cpp_default_kwargs
)

ray_syncer_proto_cpp_dep = declare_dependency(
    sources: ray_syncer_proto_cpp
)

ray_syncer_proto_cpp_lib_dependencies = [
    # Package-managed
    ray_syncer_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

ray_syncer_proto_cpp_lib = static_library(
    'ray_syncer_proto_cpp_lib',
    dependencies: ray_syncer_proto_cpp_lib_dependencies,
    install: false
)

ray_syncer_proto_cpp_lib_dep = declare_dependency(
    link_with: [ray_syncer_proto_cpp_lib],
    dependencies: ray_syncer_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [ray_syncer_proto_cpp_lib_dep]
# @end: proto_cpp (original: ray_syncer_cc_proto)

# @begin: proto_cpp_grpc (original: ray_syncer_cc_grpc)
ray_syncer_proto_cpp_grpc = custom_target(
    input: ray_syncer_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

ray_syncer_proto_cpp_grpc_dep = declare_dependency(
    sources: ray_syncer_proto_cpp_grpc
)

ray_syncer_proto_cpp_grpc_lib_dependencies = [
    # Package-managed
    ray_syncer_proto_cpp_grpc_dep,
    ray_syncer_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
    grpc
]

ray_syncer_proto_cpp_grpc_lib = static_library(
    'ray_syncer_proto_cpp_grpc_lib',
    dependencies: ray_syncer_proto_cpp_grpc_lib_dependencies,
    install: false
)

ray_syncer_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [ray_syncer_proto_cpp_grpc_lib],
    dependencies: ray_syncer_proto_cpp_grpc_lib_dependencies
)

all_proto_cpp_grpc_libs_dependencies += [ray_syncer_proto_cpp_grpc_lib_dep]
# @end: proto_cpp_grpc (original: ray_syncer_cc_grpc)
# === @end: ray_syncer.proto ===


# === @begin: profile_events.proto ===
profile_events_proto = ['profile_events.proto']

# @begin: proto_cpp (original: profile_events_cc_proto)
profile_events_proto_cpp = custom_target(
    input: profile_events_proto,
    kwargs: proto_cpp_default_kwargs
)

profile_events_proto_cpp_dep = declare_dependency(
    sources: profile_events_proto_cpp
)

profile_events_proto_cpp_lib_dependencies = [
    # Package-managed
    profile_events_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

profile_events_proto_cpp_lib = static_library(
    'profile_events_proto_cpp_lib',
    dependencies: profile_events_proto_cpp_lib_dependencies,
    install: false
)

profile_events_proto_cpp_lib_dep = declare_dependency(
    link_with: [profile_events_proto_cpp_lib],
    dependencies: profile_events_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [profile_events_proto_cpp_lib_dep]
# @end: proto_cpp (original: profile_events_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
profile_events_proto_py = custom_target(
    input: profile_events_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: profile_events.proto ===


# === @begin: gcs.proto ===
gcs_proto = ['gcs.proto']

# @begin: proto_cpp (@original: gcs_cc_proto)
gcs_proto_cpp = custom_target(
    input: gcs_proto,
    kwargs: proto_cpp_default_kwargs
)

gcs_proto_cpp_dep = declare_dependency(
    sources: gcs_proto_cpp,
    dependencies: [common_proto_cpp_dep, profile_events_proto_cpp_dep]
)

gcs_proto_cpp_lib_dependencies = [
    # Package-managed
    gcs_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

gcs_proto_cpp_lib = static_library(
    'gcs_proto_cpp_lib',
    dependencies: gcs_proto_cpp_lib_dependencies,
    install: false
)

gcs_proto_cpp_lib_dep = declare_dependency(
    link_with: [gcs_proto_cpp_lib],
    dependencies: gcs_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [gcs_proto_cpp_lib_dep]
# @end: proto_cpp (@original: gcs_cc_proto)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
gcs_proto_py = custom_target(
    input: gcs_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: gcs.proto ===


# === @begin: logging.proto ===
logging_proto = ['logging.proto']

# @begin: proto_cpp (@original: logging_cc_proto)
logging_proto_cpp = custom_target(
    input: logging_proto,
    kwargs: proto_cpp_default_kwargs
)

logging_proto_cpp_dep = declare_dependency(
    sources: logging_proto_cpp
)

logging_proto_cpp_lib_dependencies = [
    # Package-managed
    logging_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

logging_proto_cpp_lib = static_library(
    'logging_proto_cpp_lib',
    dependencies: logging_proto_cpp_lib_dependencies,
    install: false
)

logging_proto_cpp_lib_dep = declare_dependency(
    link_with: [logging_proto_cpp_lib],
    dependencies: logging_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [logging_proto_cpp_lib_dep]
# @end: proto_cpp (@original: logging_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
logging_proto_py = custom_target(
    input: logging_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: logging.proto ===


# === @begin: node_manager.proto ===
node_manager_proto = ['node_manager.proto']

# @begin: proto_cpp (@original: node_manager_cc_proto)
node_manager_proto_cpp = custom_target(
    input: node_manager_proto,
    kwargs: proto_cpp_default_kwargs
)

node_manager_proto_cpp_dep = declare_dependency(
    sources: node_manager_proto_cpp,
    dependencies: [common_proto_cpp_dep, gcs_proto_cpp_dep, autoscaler_proto_cpp_dep, runtime_environment_proto_cpp_dep]
)

node_manager_proto_cpp_lib_dependencies = [
    # Package-managed
    node_manager_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

node_manager_proto_cpp_lib = static_library(
    'node_manager_proto_cpp_lib',
    dependencies: node_manager_proto_cpp_lib_dependencies,
    install: false
)

node_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [node_manager_proto_cpp_lib],
    dependencies: node_manager_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [node_manager_proto_cpp_lib_dep]
# @end: proto_cpp (@original: node_manager_cc_proto)

# @begin: proto_cpp_grpc (@original: node_manager_cc_grpc)
node_manager_proto_cpp_grpc = custom_target(
    input: node_manager_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

node_manager_proto_cpp_grpc_dep = declare_dependency(
    sources: node_manager_proto_cpp_grpc
)

node_manager_proto_cpp_grpc_lib_dependencies = [
    # Package-managed
    node_manager_proto_cpp_grpc_dep,
    node_manager_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
    grpc
]

node_manager_proto_cpp_grpc_lib = static_library(
    'node_manager_proto_cpp_grpc_lib',
    dependencies: node_manager_proto_cpp_grpc_lib_dependencies,
    install: false
)

node_manager_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [node_manager_proto_cpp_grpc_lib],
    dependencies: node_manager_proto_cpp_grpc_lib_dependencies
)

all_proto_cpp_grpc_libs_dependencies += [node_manager_proto_cpp_grpc_lib_dep]
# @end: proto_cpp_grpc (@original: node_manager_cc_grpc)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
node_manager_proto_py = custom_target(
    input: node_manager_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: node_manager.proto ===


# === @begin: reporter.proto ===
reporter_proto = 'reporter.proto'

# @begin: proto_cpp (@original: reporter_cc_proto)
reporter_proto_cpp = custom_target(
    input: reporter_proto,
    kwargs: proto_cpp_default_kwargs
)

reporter_proto_cpp_dep = declare_dependency(
    sources: reporter_proto_cpp,
    dependencies: [common_proto_cpp_dep, opencensus_metrics_proto_cpp_dep]
)

reporter_proto_cpp_lib_dependencies = [
    # Package-managed
    reporter_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

reporter_proto_cpp_lib = static_library(
    'reporter_proto_cpp_lib',
    dependencies: reporter_proto_cpp_lib_dependencies,
    install: false
)

reporter_proto_cpp_lib_dep = declare_dependency(
    link_with: [reporter_proto_cpp_lib],
    dependencies: reporter_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [reporter_proto_cpp_lib_dep]
# @end: proto_cpp (@original: reporter_cc_proto)

# @begin: proto_cpp_grpc (@original: reporter_cc_grpc)
reporter_proto_cpp_grpc = custom_target(
    input: reporter_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

reporter_proto_cpp_grpc_dep = declare_dependency(
    sources: reporter_proto_cpp_grpc
)

reporter_proto_cpp_grpc_lib_dependencies = [
    # Package-managed
    reporter_proto_cpp_grpc_dep,
    reporter_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
    grpc
]

reporter_proto_cpp_grpc_lib = static_library(
    'reporter_proto_cpp_grpc_lib',
    dependencies: reporter_proto_cpp_grpc_lib_dependencies,
    install: false
)

reporter_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [reporter_proto_cpp_grpc_lib],
    dependencies: reporter_proto_cpp_grpc_lib_dependencies
)

all_proto_cpp_grpc_libs_dependencies += [reporter_proto_cpp_grpc_lib_dep]
# @end: proto_cpp_grpc (@original: reporter_cc_grpc)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
reporter_proto_py = custom_target(
    input: reporter_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: reporter.proto ===


# === @begin: pubsub.proto ===
pubsub_proto = ['pubsub.proto']

# @begin: proto_cpp (@original: pubsub_cc_proto)
pubsub_proto_cpp = custom_target(
    input: pubsub_proto,
    kwargs: proto_cpp_default_kwargs
)

pubsub_proto_cpp_dep = declare_dependency(
    sources: pubsub_proto_cpp,
    dependencies: [common_proto_cpp_dep, gcs_proto_cpp_dep, logging_proto_cpp_dep]
)

pubsub_proto_cpp_lib_dependencies = [
    # Package-managed
    pubsub_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

pubsub_proto_cpp_lib = static_library(
    'pubsub_proto_cpp_lib',
    dependencies: pubsub_proto_cpp_lib_dependencies,
    install: false
)

pubsub_proto_cpp_lib_dep = declare_dependency(
    link_with: [pubsub_proto_cpp_lib],
    dependencies: pubsub_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [pubsub_proto_cpp_lib_dep]
# @end: proto_cpp (@original: pubsub_cc_proto)

# @begin: proto_cpp_grpc (@original: pubsub_cc_grpc)
pubsub_proto_cpp_grpc = custom_target(
    input: pubsub_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

pubsub_proto_cpp_grpc_dep = declare_dependency(
    sources: pubsub_proto_cpp_grpc,
    dependencies: [common_proto_cpp_lib_dep, gcs_proto_cpp_lib_dep, pubsub_proto_cpp_lib_dep]
)

pubsub_proto_cpp_grpc_lib_dependencies = [
    # Package-managed
    pubsub_proto_cpp_grpc_dep,
    pubsub_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
    grpc
]

pubsub_proto_cpp_grpc_lib = static_library(
    'pubsub_proto_cpp_grpc_lib',
    dependencies: pubsub_proto_cpp_grpc_lib_dependencies,
    install: false
)

pubsub_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [pubsub_proto_cpp_grpc_lib],
    dependencies: pubsub_proto_cpp_grpc_lib_dependencies
)

all_proto_cpp_grpc_libs_dependencies += [pubsub_proto_cpp_grpc_lib_dep]
# @end: proto_cpp_grpc (@original: pubsub_cc_grpc)

# @Important:
# Original Bazel script doesn't have a rule to build these Python protos, but likely behind the scenes it generates and
# installs them together with some other protos which depend on them. In the same way, `metrics.proto` and `resource.proto`
# from `opencensus-proto` aren't specified anywhere for installation but included there, so we decided to emulate that.
# For details, find them in `@SOURCE_ROOT@/eugo/include/opencensus/`
# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
pubsub_proto_py = custom_target(
    input: pubsub_proto,
    kwargs: proto_py_default_kwargs
)

# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: pubsub.proto ===


# === @begin: events_task_profile_events.proto ===
events_task_profile_events_proto = ['events_task_profile_events.proto']

# @begin: proto_cpp (original: events_task_profile_events_cc_proto)
events_task_profile_events_proto_cpp = custom_target(
    input: events_task_profile_events_proto,
    kwargs: proto_cpp_default_kwargs
)

events_task_profile_events_proto_cpp_dep = declare_dependency(
    sources: events_task_profile_events_proto_cpp,
    dependencies: [profile_events_proto_cpp_dep]
)

events_task_profile_events_proto_cpp_lib_dependencies = [
    # Package-managed
    events_task_profile_events_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

events_task_profile_events_proto_cpp_lib = static_library(
    'events_task_profile_events_proto_cpp_lib',
    dependencies: events_task_profile_events_proto_cpp_lib_dependencies,
    install: false
)

events_task_profile_events_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_task_profile_events_proto_cpp_lib],
    dependencies: events_task_profile_events_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_task_profile_events_proto_cpp_lib_dep]
# @end: proto_cpp (original: events_task_profile_events_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
events_task_profile_events_proto_py = custom_target(
    input: events_task_profile_events_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: events_task_profile_events.proto ===


# ==========================================================================
# `public/events_*` `_cpp_dep` and static libraries
# Codegen custom_targets are in `public/meson.build`. The `_cpp_dep` declarations
# are here (not in `public/`) because several events protos import `common.proto`
# and/or `runtime_environment.proto`, which require `common_proto_cpp_dep` and
# `runtime_environment_proto_cpp_dep` to be available. Listing import deps on
# `_cpp_dep` (rather than on `_lib_dependencies`) is consistent with all
# non-events proto targets in this file.
# ==========================================================================

# --- `_cpp_dep` declarations (matching Bazel proto_library deps from `public/BUILD.bazel`) ---

# deps: timestamp only (no local proto deps)
events_driver_job_lifecycle_event_proto_cpp_dep = declare_dependency(
    sources: events_driver_job_lifecycle_event_proto_cpp
)

events_node_definition_event_proto_cpp_dep = declare_dependency(
    sources: events_node_definition_event_proto_cpp
)

events_node_lifecycle_event_proto_cpp_dep = declare_dependency(
    sources: events_node_lifecycle_event_proto_cpp
)

# deps: common
events_actor_definition_event_proto_cpp_dep = declare_dependency(
    sources: events_actor_definition_event_proto_cpp,
    dependencies: [common_proto_cpp_dep]
)

events_actor_lifecycle_event_proto_cpp_dep = declare_dependency(
    sources: events_actor_lifecycle_event_proto_cpp,
    dependencies: [common_proto_cpp_dep]
)

events_task_lifecycle_event_proto_cpp_dep = declare_dependency(
    sources: events_task_lifecycle_event_proto_cpp,
    dependencies: [common_proto_cpp_dep]
)

# deps: runtime_environment + common
events_actor_task_definition_event_proto_cpp_dep = declare_dependency(
    sources: events_actor_task_definition_event_proto_cpp,
    dependencies: [runtime_environment_proto_cpp_dep, common_proto_cpp_dep]
)

events_task_definition_event_proto_cpp_dep = declare_dependency(
    sources: events_task_definition_event_proto_cpp,
    dependencies: [runtime_environment_proto_cpp_dep, common_proto_cpp_dep]
)

events_driver_job_definition_event_proto_cpp_dep = declare_dependency(
    sources: events_driver_job_definition_event_proto_cpp,
    dependencies: [runtime_environment_proto_cpp_dep, common_proto_cpp_dep]
)

# deps: all events_* + events_task_profile_events (Bazel order)
events_base_event_proto_cpp_dep = declare_dependency(
    sources: events_base_event_proto_cpp,
    dependencies: [
        events_actor_definition_event_proto_cpp_dep,
        events_actor_lifecycle_event_proto_cpp_dep,
        events_actor_task_definition_event_proto_cpp_dep,
        events_driver_job_definition_event_proto_cpp_dep,
        events_driver_job_lifecycle_event_proto_cpp_dep,
        events_node_definition_event_proto_cpp_dep,
        events_node_lifecycle_event_proto_cpp_dep,
        events_task_definition_event_proto_cpp_dep,
        events_task_lifecycle_event_proto_cpp_dep,
        events_task_profile_events_proto_cpp_dep,
    ]
)

# --- static libraries ---

# @begin: events_driver_job_lifecycle_event_proto_cpp_lib
events_driver_job_lifecycle_event_proto_cpp_lib_dependencies = [
    # Package-managed
    events_driver_job_lifecycle_event_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]

events_driver_job_lifecycle_event_proto_cpp_lib = static_library(
    'events_driver_job_lifecycle_event_proto_cpp_lib',
    dependencies: events_driver_job_lifecycle_event_proto_cpp_lib_dependencies,
    install: false
)
events_driver_job_lifecycle_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_driver_job_lifecycle_event_proto_cpp_lib],
    dependencies: events_driver_job_lifecycle_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_driver_job_lifecycle_event_proto_cpp_lib_dep]
# @end: events_driver_job_lifecycle_event_proto_cpp_lib

# @begin: events_node_definition_event_proto_cpp_lib
events_node_definition_event_proto_cpp_lib_dependencies = [
    # Package-managed
    events_node_definition_event_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]

events_node_definition_event_proto_cpp_lib = static_library(
    'events_node_definition_event_proto_cpp_lib',
    dependencies: events_node_definition_event_proto_cpp_lib_dependencies,
    install: false
)
events_node_definition_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_node_definition_event_proto_cpp_lib],
    dependencies: events_node_definition_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_node_definition_event_proto_cpp_lib_dep]
# @end: events_node_definition_event_proto_cpp_lib

# @begin: events_node_lifecycle_event_proto_cpp_lib
events_node_lifecycle_event_proto_cpp_lib_dependencies = [
    # Package-managed
    events_node_lifecycle_event_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]

events_node_lifecycle_event_proto_cpp_lib = static_library(
    'events_node_lifecycle_event_proto_cpp_lib',
    dependencies: events_node_lifecycle_event_proto_cpp_lib_dependencies,
    install: false
)
events_node_lifecycle_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_node_lifecycle_event_proto_cpp_lib],
    dependencies: events_node_lifecycle_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_node_lifecycle_event_proto_cpp_lib_dep]
# @end: events_node_lifecycle_event_proto_cpp_lib

# @begin: events_actor_definition_event_proto_cpp_lib
events_actor_definition_event_proto_cpp_lib_dependencies = [
    # Package-managed
    events_actor_definition_event_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]
events_actor_definition_event_proto_cpp_lib = static_library(
    'events_actor_definition_event_proto_cpp_lib',
    dependencies: events_actor_definition_event_proto_cpp_lib_dependencies,
    install: false
)
events_actor_definition_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_actor_definition_event_proto_cpp_lib],
    dependencies: events_actor_definition_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_actor_definition_event_proto_cpp_lib_dep]
# @end: events_actor_definition_event_proto_cpp_lib

# @begin: events_actor_lifecycle_event_proto_cpp_lib
events_actor_lifecycle_event_proto_cpp_lib_dependencies = [
    # Package-managed
    events_actor_lifecycle_event_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]
events_actor_lifecycle_event_proto_cpp_lib = static_library(
    'events_actor_lifecycle_event_proto_cpp_lib',
    dependencies: events_actor_lifecycle_event_proto_cpp_lib_dependencies,
    install: false
)
events_actor_lifecycle_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_actor_lifecycle_event_proto_cpp_lib],
    dependencies: events_actor_lifecycle_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_actor_lifecycle_event_proto_cpp_lib_dep]
# @end: events_actor_lifecycle_event_proto_cpp_lib

# @begin: events_task_lifecycle_event_proto_cpp_lib
events_task_lifecycle_event_proto_cpp_lib_dependencies = [
    # Package-managed
    events_task_lifecycle_event_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]
events_task_lifecycle_event_proto_cpp_lib = static_library(
    'events_task_lifecycle_event_proto_cpp_lib',
    dependencies: events_task_lifecycle_event_proto_cpp_lib_dependencies,
    install: false
)
events_task_lifecycle_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_task_lifecycle_event_proto_cpp_lib],
    dependencies: events_task_lifecycle_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_task_lifecycle_event_proto_cpp_lib_dep]
# @end: events_task_lifecycle_event_proto_cpp_lib

# @begin: events_actor_task_definition_event_proto_cpp_lib
events_actor_task_definition_event_proto_cpp_lib_dependencies = [
    # Package-managed
    events_actor_task_definition_event_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]
events_actor_task_definition_event_proto_cpp_lib = static_library(
    'events_actor_task_definition_event_proto_cpp_lib',
    dependencies: events_actor_task_definition_event_proto_cpp_lib_dependencies,
    install: false
)
events_actor_task_definition_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_actor_task_definition_event_proto_cpp_lib],
    dependencies: events_actor_task_definition_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_actor_task_definition_event_proto_cpp_lib_dep]
# @end: events_actor_task_definition_event_proto_cpp_lib

# @begin: events_task_definition_event_proto_cpp_lib
events_task_definition_event_proto_cpp_lib_dependencies = [
    # Package-managed
    events_task_definition_event_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]
events_task_definition_event_proto_cpp_lib = static_library(
    'events_task_definition_event_proto_cpp_lib',
    dependencies: events_task_definition_event_proto_cpp_lib_dependencies,
    install: false
)
events_task_definition_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_task_definition_event_proto_cpp_lib],
    dependencies: events_task_definition_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_task_definition_event_proto_cpp_lib_dep]
# @end: events_task_definition_event_proto_cpp_lib

# @begin: events_driver_job_definition_event_proto_cpp_lib
events_driver_job_definition_event_proto_cpp_lib_dependencies = [
    # Package-managed
    events_driver_job_definition_event_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]
events_driver_job_definition_event_proto_cpp_lib = static_library(
    'events_driver_job_definition_event_proto_cpp_lib',
    dependencies: events_driver_job_definition_event_proto_cpp_lib_dependencies,
    install: false
)
events_driver_job_definition_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_driver_job_definition_event_proto_cpp_lib],
    dependencies: events_driver_job_definition_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_driver_job_definition_event_proto_cpp_lib_dep]
# @end: events_driver_job_definition_event_proto_cpp_lib

# @begin: events_base_event_proto_cpp_lib
events_base_event_proto_cpp_lib_dependencies = [
    # Package-managed
    events_base_event_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]
events_base_event_proto_cpp_lib = static_library(
    'events_base_event_proto_cpp_lib',
    dependencies: events_base_event_proto_cpp_lib_dependencies,
    install: false
)
events_base_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_base_event_proto_cpp_lib],
    dependencies: events_base_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_base_event_proto_cpp_lib_dep]
# @end: events_base_event_proto_cpp_lib


# ==========================================================================
# events_event_aggregator_service.proto (in this directory, not public/)
# Depends on common.proto and public/events_base_event.proto
# ==========================================================================

# === @begin: events_event_aggregator_service.proto ===
events_event_aggregator_service_proto = ['events_event_aggregator_service.proto']

# @begin: proto_cpp (original: events_event_aggregator_service_cc_proto)
events_event_aggregator_service_proto_cpp = custom_target(
    input: events_event_aggregator_service_proto,
    kwargs: proto_cpp_default_kwargs
)

events_event_aggregator_service_proto_cpp_dep = declare_dependency(
    sources: events_event_aggregator_service_proto_cpp,
    dependencies: [common_proto_cpp_dep, events_base_event_proto_cpp_dep]
)

events_event_aggregator_service_proto_cpp_lib_dependencies = [
    # Package-managed
    events_event_aggregator_service_proto_cpp_dep,

    # Eugo-managed
    protobuf,
]

events_event_aggregator_service_proto_cpp_lib = static_library(
    'events_event_aggregator_service_proto_cpp_lib',
    dependencies: events_event_aggregator_service_proto_cpp_lib_dependencies,
    install: false
)

events_event_aggregator_service_proto_cpp_lib_dep = declare_dependency(
    link_with: [events_event_aggregator_service_proto_cpp_lib],
    dependencies: events_event_aggregator_service_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [events_event_aggregator_service_proto_cpp_lib_dep]
# @end: proto_cpp (original: events_event_aggregator_service_cc_proto)

# @begin: proto_cpp_grpc (original: events_event_aggregator_service_cc_grpc)
events_event_aggregator_service_proto_cpp_grpc = custom_target(
    input: events_event_aggregator_service_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

events_event_aggregator_service_proto_cpp_grpc_dep = declare_dependency(
    sources: events_event_aggregator_service_proto_cpp_grpc
)

events_event_aggregator_service_proto_cpp_grpc_lib_dependencies = [
    # Package-managed
    events_event_aggregator_service_proto_cpp_grpc_dep,
    events_event_aggregator_service_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
    grpc,
]

events_event_aggregator_service_proto_cpp_grpc_lib = static_library(
    'events_event_aggregator_service_proto_cpp_grpc_lib',
    dependencies: events_event_aggregator_service_proto_cpp_grpc_lib_dependencies,
    install: false
)

events_event_aggregator_service_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [events_event_aggregator_service_proto_cpp_grpc_lib],
    dependencies: events_event_aggregator_service_proto_cpp_grpc_lib_dependencies
)

all_proto_cpp_grpc_libs_dependencies += [events_event_aggregator_service_proto_cpp_grpc_lib_dep]
# @end: proto_cpp_grpc (original: events_event_aggregator_service_cc_grpc)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
events_event_aggregator_service_proto_py = custom_target(
    input: events_event_aggregator_service_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: events_event_aggregator_service.proto ===


# === @begin: gcs_service.proto ===
gcs_service_proto = ['gcs_service.proto']

# @begin: proto_cpp (@original: gcs_service_cc_proto)
gcs_service_proto_cpp = custom_target(
    input: gcs_service_proto,
    kwargs: proto_cpp_default_kwargs
)

gcs_service_proto_cpp_dep = declare_dependency(
    sources: gcs_service_proto_cpp,
    dependencies: [common_proto_cpp_dep, gcs_proto_cpp_dep, pubsub_proto_cpp_dep, events_event_aggregator_service_proto_cpp_dep]
)

gcs_service_proto_cpp_lib_dependencies = [
    # Package-managed
    gcs_service_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

gcs_service_proto_cpp_lib = static_library(
    'gcs_service_proto_cpp_lib',
    dependencies: gcs_service_proto_cpp_lib_dependencies,
    install: false
)

gcs_service_proto_cpp_lib_dep = declare_dependency(
    link_with: [gcs_service_proto_cpp_lib],
    dependencies: gcs_service_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [gcs_service_proto_cpp_lib_dep]
# @end: proto_cpp (@original: gcs_service_cc_proto)

# @begin: proto_cpp_grpc (@original: gcs_service_cc_grpc)
gcs_service_proto_cpp_grpc = custom_target(
    input: gcs_service_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

gcs_service_proto_cpp_grpc_dep = declare_dependency(
    sources: gcs_service_proto_cpp_grpc
)

gcs_service_proto_cpp_grpc_lib_dependencies = [
    # Package-managed
    gcs_service_proto_cpp_grpc_dep,
    gcs_service_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
    grpc
]

gcs_service_proto_cpp_grpc_lib = static_library(
    'gcs_service_proto_cpp_grpc_lib',
    dependencies: gcs_service_proto_cpp_grpc_lib_dependencies,
    install: false
)

gcs_service_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [gcs_service_proto_cpp_grpc_lib],
    dependencies: gcs_service_proto_cpp_grpc_lib_dependencies
)

all_proto_cpp_grpc_libs_dependencies += [gcs_service_proto_cpp_grpc_lib_dep]
# @end: proto_cpp (@original: gcs_service_cc_proto)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
gcs_service_proto_py = custom_target(
    input: gcs_service_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: gcs_service.proto ===


# === @begin: test_service.proto ===
test_service_proto = ['test_service.proto']

# @begin: proto_cpp (@original: test_service_cc_proto)
test_service_proto_cpp = custom_target(
    input: test_service_proto,
    kwargs: proto_cpp_default_kwargs
)

test_service_proto_cpp_dep = declare_dependency(
    sources: test_service_proto_cpp
)

test_service_proto_cpp_lib_dependencies = [
    # Package-managed
    test_service_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

test_service_proto_cpp_lib = static_library(
    'test_service_proto_cpp_lib',
    dependencies: test_service_proto_cpp_lib_dependencies,
    install: false
)

test_service_proto_cpp_lib_dep = declare_dependency(
    link_with: [test_service_proto_cpp_lib],
    dependencies: test_service_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [test_service_proto_cpp_lib_dep]
# @end: proto_cpp (@original: test_service_cc_proto)

# @begin: proto_cpp_grpc (@original: test_service_cc_grpc)
test_service_proto_cpp_grpc = custom_target(
    input: test_service_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

test_service_proto_cpp_grpc_dep = declare_dependency(
    sources: test_service_proto_cpp_grpc
)

test_service_proto_cpp_grpc_lib_dependencies = [
    # Package-managed
    test_service_proto_cpp_grpc_dep,
    test_service_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
    grpc
]

test_service_proto_cpp_grpc_lib = static_library(
    'test_service_proto_cpp_grpc_lib',
    dependencies: test_service_proto_cpp_grpc_lib_dependencies,
    install: false
)

test_service_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [test_service_proto_cpp_grpc_lib],
    dependencies: test_service_proto_cpp_grpc_lib_dependencies
)

all_proto_cpp_grpc_libs_dependencies += [test_service_proto_cpp_grpc_lib_dep]
# @end: proto_cpp_grpc (@original: test_service_cc_grpc)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
test_service_proto_py = custom_target(
    input: test_service_proto,
    kwargs: proto_py_default_kwargs + {
        'install': false # @TODO+:why this is build at all if not installed?
    }
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: test_service.proto ===


# === @begin: object_manager.proto ===
object_manager_proto = ['object_manager.proto']

# @begin: proto_cpp (@original: object_manager_cc_proto)
object_manager_proto_cpp = custom_target(
    input: object_manager_proto,
    kwargs: proto_cpp_default_kwargs
)

object_manager_proto_cpp_dep = declare_dependency(
    sources: object_manager_proto_cpp,
    dependencies: [common_proto_cpp_dep]
)

object_manager_proto_cpp_lib_dependencies = [
    # Package-managed
    object_manager_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

object_manager_proto_cpp_lib = static_library(
    'object_manager_proto_cpp_lib',
    dependencies: object_manager_proto_cpp_lib_dependencies,
    install: false
)

object_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [object_manager_proto_cpp_lib],
    dependencies: object_manager_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [object_manager_proto_cpp_lib_dep]
# @end: proto_cpp (@original: object_manager_cc_proto)

# @begin: proto_cpp_grpc (@original: object_manager_cc_grpc)
object_manager_proto_cpp_grpc = custom_target(
    input: object_manager_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

object_manager_proto_cpp_grpc_dep = declare_dependency(
    sources: object_manager_proto_cpp_grpc
)

object_manager_proto_cpp_grpc_lib_dependencies = [
    # Package-managed
    object_manager_proto_cpp_grpc_dep,
    object_manager_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
    grpc
]

object_manager_proto_cpp_grpc_lib = static_library(
    'object_manager_proto_cpp_grpc_lib',
    dependencies: object_manager_proto_cpp_grpc_lib_dependencies,
    install: false
)

object_manager_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [object_manager_proto_cpp_grpc_lib],
    dependencies: object_manager_proto_cpp_grpc_lib_dependencies
)

all_proto_cpp_grpc_libs_dependencies += [object_manager_proto_cpp_grpc_lib_dep]
# @end: proto_cpp_grpc (@original: object_manager_cc_grpc)
# === @end: object_manager.proto ===


# === @begin: core_worker.proto ===
core_worker_proto = ['core_worker.proto']

# @begin: proto_cpp (@original: worker_cc_proto (yep, original version doesn't have `core_` prefix, likely, for historical reasons))
core_worker_proto_cpp = custom_target(
    input: core_worker_proto,
    kwargs: proto_cpp_default_kwargs
)

core_worker_proto_cpp_dep = declare_dependency(
    sources: core_worker_proto_cpp,
    dependencies: [common_proto_cpp_dep, pubsub_proto_cpp_dep]
)

core_worker_proto_cpp_lib_dependencies = [
    # Package-managed
    core_worker_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

core_worker_proto_cpp_lib = static_library(
    'core_worker_proto_cpp_lib',
    dependencies: core_worker_proto_cpp_lib_dependencies,
    install: false
)

core_worker_proto_cpp_lib_dep = declare_dependency(
    link_with: [core_worker_proto_cpp_lib],
    dependencies: core_worker_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [core_worker_proto_cpp_lib_dep]
# @end: proto_cpp (@original: worker_cc_proto (yep, original version doesn't have `core_` prefix, likely, for historical reasons))

# @begin: proto_cpp_grpc (@original: worker_cc_grpc (yep, original version doesn't have `core_` prefix, likely, for historical reasons))
core_worker_proto_cpp_grpc = custom_target(
    input: core_worker_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

core_worker_proto_cpp_grpc_dep = declare_dependency(
    sources: core_worker_proto_cpp_grpc
)

core_worker_proto_cpp_grpc_lib_dependencies = [
    # Package-managed
    core_worker_proto_cpp_grpc_dep,
    core_worker_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
    grpc
]

core_worker_proto_cpp_grpc_lib = static_library(
    'core_worker_proto_cpp_grpc_lib',
    dependencies: core_worker_proto_cpp_grpc_lib_dependencies,
    install: false
)

core_worker_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [core_worker_proto_cpp_grpc_lib],
    dependencies: core_worker_proto_cpp_grpc_lib_dependencies
)

all_proto_cpp_grpc_libs_dependencies += [core_worker_proto_cpp_grpc_lib_dep]
# @end: proto_cpp_grpc (@original: worker_cc_grpc (yep, original version doesn't have `core_` prefix, likely, for historical reasons))

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
core_worker_proto_py = custom_target(
    input: core_worker_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: core_worker.proto ===


# === @begin: serialization.proto ===
serialization_proto = ['serialization.proto']

# @begin: proto_cpp (@original: serialization_cc_proto)
serialization_proto_cpp = custom_target(
    input: serialization_proto,
    kwargs: proto_cpp_default_kwargs
)

serialization_proto_cpp_dep = declare_dependency(
    sources: serialization_proto_cpp
)

serialization_proto_cpp_lib_dependencies = [
    # Package-managed
    serialization_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

serialization_proto_cpp_lib = static_library(
    'serialization_proto_cpp_lib',
    dependencies: serialization_proto_cpp_lib_dependencies,
    install: false
)

serialization_proto_cpp_lib_dep = declare_dependency(
    link_with: [serialization_proto_cpp_lib],
    dependencies: serialization_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [serialization_proto_cpp_lib_dep]
# @end: proto_cpp (@original: serialization_cc_proto)
# === @end: serialization.proto ===


# === @begin: event.proto ===
event_proto = ['event.proto']

# @begin: proto_cpp (@original: event_cc_proto)
event_proto_cpp = custom_target(
    input: event_proto,
    kwargs: proto_cpp_default_kwargs
)

event_proto_cpp_dep = declare_dependency(
    sources: event_proto_cpp
)

event_proto_cpp_lib_dependencies = [
    # Package-managed
    event_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

event_proto_cpp_lib = static_library(
    'event_proto_cpp_lib',
    dependencies: event_proto_cpp_lib_dependencies,
    install: false
)

event_proto_cpp_lib_dep = declare_dependency(
    link_with: [event_proto_cpp_lib],
    dependencies: event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [event_proto_cpp_lib_dep]
# @end: proto_cpp (@original: event_cc_proto)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
event_proto_py = custom_target(
    input: event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: event.proto ===


# NOTE: agent_manager.proto was deleted upstream


# === @begin: runtime_env_agent.proto ===
runtime_env_agent_proto = ['runtime_env_agent.proto']

# @begin: proto_cpp (@original: runtime_env_agent_cc_proto)
runtime_env_agent_proto_cpp = custom_target(
    input: runtime_env_agent_proto,
    kwargs: proto_cpp_default_kwargs
)

runtime_env_agent_proto_cpp_dep = declare_dependency(
    sources: runtime_env_agent_proto_cpp,
    dependencies: [runtime_env_common_proto_cpp_dep, runtime_environment_proto_cpp_dep, common_proto_cpp_dep]
)

runtime_env_agent_proto_cpp_lib_dependencies = [
    # Package-managed
    runtime_env_agent_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

runtime_env_agent_proto_cpp_lib = static_library(
    'runtime_env_agent_proto_cpp_lib',
    dependencies: runtime_env_agent_proto_cpp_lib_dependencies,
    install: false
)

runtime_env_agent_proto_cpp_lib_dep = declare_dependency(
    link_with: [runtime_env_agent_proto_cpp_lib],
    dependencies: runtime_env_agent_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [runtime_env_agent_proto_cpp_lib_dep]
# @end: proto_cpp (@original: runtime_env_agent_cc_proto)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
runtime_env_agent_proto_py = custom_target(
    input: runtime_env_agent_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: runtime_env_agent.proto ===


# === @begin: usage.proto ===
usage_proto = ['usage.proto']

# @begin: proto_cpp (@original: usage_cc_proto)
usage_proto_cpp = custom_target(
    input: usage_proto,
    kwargs: proto_cpp_default_kwargs
)

usage_proto_cpp_dep = declare_dependency(
    sources: usage_proto_cpp
)

usage_proto_cpp_lib_dependencies = [
    # Package-managed
    usage_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

usage_proto_cpp_lib = static_library(
    'usage_proto_cpp_lib',
    dependencies: usage_proto_cpp_lib_dependencies,
    install: false
)

usage_proto_cpp_lib_dep = declare_dependency(
    link_with: [usage_proto_cpp_lib],
    dependencies: usage_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [usage_proto_cpp_lib_dep]
# @end: proto_cpp (@original: usage_cc_proto)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
usage_proto_py = custom_target(
    input: usage_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: usage.proto ===


# === @begin: dependency.proto ===
dependency_proto = ['dependency.proto']

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
dependency_proto_py = custom_target(
    input: dependency_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: dependency.proto ===


# NOTE: job_agent.proto was deleted upstream


# === @begin: instance_manager.proto ===
# (moved from `experimental/` subdir to this top level file)
instance_manager_proto = ['instance_manager.proto']

# @begin: proto_cpp (original: instance_manager_cc_proto)
instance_manager_proto_cpp = custom_target(
    input: instance_manager_proto,
    kwargs: proto_cpp_default_kwargs
)

instance_manager_proto_cpp_dep = declare_dependency(
    sources: instance_manager_proto_cpp
)

instance_manager_proto_cpp_lib_dependencies = [
    # Package-managed
    instance_manager_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

instance_manager_proto_cpp_lib = static_library(
    'instance_manager_proto_cpp_lib',
    dependencies: instance_manager_proto_cpp_lib_dependencies,
    install: false
)

instance_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [instance_manager_proto_cpp_lib],
    dependencies: instance_manager_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [instance_manager_proto_cpp_lib_dep]
# @end: proto_cpp (original: instance_manager_cc_proto)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
instance_manager_proto_py = custom_target(
    input: instance_manager_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: instance_manager.proto ===


# === @begin: ray_client.proto ===
ray_client_proto = ['ray_client.proto']

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
ray_client_proto_py = custom_target(
    input: ray_client_proto,
    kwargs: proto_py_default_kwargs,
)
# @end: proto_py (@original: core_py_proto)
# === @end: ray_client.proto ===


# === @begin: serve.proto ===
serve_proto = ['serve.proto']

# @begin: proto_py (@original: serve_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
serve_proto_py = custom_target(
    input: serve_proto,
    kwargs: proto_py_default_kwargs + {
        'install_dir': py.get_install_dir() / 'ray/serve/generated/'
    }
)
# @end: proto_py (@original: serve_py_proto)
# === @end: serve.proto ===


# === @begin: export_runtime_env.proto ===
# (moved from export_api/ to top level)
export_runtime_env_proto = ['export_runtime_env.proto']

# @begin: proto_cpp (@original: export_runtime_env_cc_proto)
export_runtime_env_proto_cpp = custom_target(
    input: export_runtime_env_proto,
    kwargs: proto_cpp_default_kwargs
)

export_runtime_env_proto_cpp_dep = declare_dependency(
    sources: export_runtime_env_proto_cpp
)

export_runtime_env_proto_cpp_lib_dependencies = [
    # Package-managed
    export_runtime_env_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_runtime_env_proto_cpp_lib = static_library(
    'export_runtime_env_proto_cpp_lib',
    dependencies: export_runtime_env_proto_cpp_lib_dependencies,
    install: false
)

export_runtime_env_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_runtime_env_proto_cpp_lib],
    dependencies: export_runtime_env_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_runtime_env_proto_cpp_lib_dep]
# @end: proto_cpp (@original: export_runtime_env_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_runtime_env_proto_py = custom_target(
    input: export_runtime_env_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: export_runtime_env.proto ===


# === @begin: export_train_state.proto ===
export_train_state_proto = ['export_train_state.proto']

# @begin: proto_cpp (original: export_train_state_cc_proto)
export_train_state_proto_cpp = custom_target(
    input: export_train_state_proto,
    kwargs: proto_cpp_default_kwargs
)

export_train_state_proto_cpp_dep = declare_dependency(
    sources: export_train_state_proto_cpp
)

export_train_state_proto_cpp_lib_dependencies = [
    # Package-managed
    export_train_state_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_train_state_proto_cpp_lib = static_library(
    'export_train_state_proto_cpp_lib',
    dependencies: export_train_state_proto_cpp_lib_dependencies,
    install: false
)

export_train_state_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_train_state_proto_cpp_lib],
    dependencies: export_train_state_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_train_state_proto_cpp_lib_dep]
# @end: proto_cpp (original: export_train_state_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_train_state_proto_py = custom_target(
    input: export_train_state_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: export_train_state.proto ===


# === @begin: export_dataset_operator_event.proto ===
export_dataset_operator_event_proto = ['export_dataset_operator_event.proto']

# @begin: proto_cpp (original: export_dataset_operator_event_cc_proto)
export_dataset_operator_event_proto_cpp = custom_target(
    input: export_dataset_operator_event_proto,
    kwargs: proto_cpp_default_kwargs
)

export_dataset_operator_event_proto_cpp_dep = declare_dependency(
    sources: export_dataset_operator_event_proto_cpp
)

export_dataset_operator_event_proto_cpp_lib_dependencies = [
    # Package-managed
    export_dataset_operator_event_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_dataset_operator_event_proto_cpp_lib = static_library(
    'export_dataset_operator_event_proto_cpp_lib',
    dependencies: export_dataset_operator_event_proto_cpp_lib_dependencies,
    install: false
)

export_dataset_operator_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_dataset_operator_event_proto_cpp_lib],
    dependencies: export_dataset_operator_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_dataset_operator_event_proto_cpp_lib_dep]
# @end: proto_cpp (original: export_dataset_operator_event_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_dataset_operator_event_proto_py = custom_target(
    input: export_dataset_operator_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: export_dataset_operator_event.proto ===


# === @begin: export_dataset_operator_schema.proto ===
export_dataset_operator_schema_proto = ['export_dataset_operator_schema.proto']

# @begin: proto_cpp (original: export_dataset_operator_schema_cc_proto)
export_dataset_operator_schema_proto_cpp = custom_target(
    input: export_dataset_operator_schema_proto,
    kwargs: proto_cpp_default_kwargs
)

export_dataset_operator_schema_proto_cpp_dep = declare_dependency(
    sources: export_dataset_operator_schema_proto_cpp
)

export_dataset_operator_schema_proto_cpp_lib_dependencies = [
    # Package-managed
    export_dataset_operator_schema_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_dataset_operator_schema_proto_cpp_lib = static_library(
    'export_dataset_operator_schema_proto_cpp_lib',
    dependencies: export_dataset_operator_schema_proto_cpp_lib_dependencies,
    install: false
)

export_dataset_operator_schema_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_dataset_operator_schema_proto_cpp_lib],
    dependencies: export_dataset_operator_schema_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_dataset_operator_schema_proto_cpp_lib_dep]
# @end: proto_cpp (original: export_dataset_operator_schema_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_dataset_operator_schema_proto_py = custom_target(
    input: export_dataset_operator_schema_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: export_dataset_operator_schema.proto ===


# === @begin: export_dataset_metadata.proto ===
export_dataset_metadata_proto = ['export_dataset_metadata.proto']

# @begin: proto_cpp (original: export_dataset_metadata_cc_proto)
export_dataset_metadata_proto_cpp = custom_target(
    input: export_dataset_metadata_proto,
    kwargs: proto_cpp_default_kwargs
)

export_dataset_metadata_proto_cpp_dep = declare_dependency(
    sources: export_dataset_metadata_proto_cpp
)

export_dataset_metadata_proto_cpp_lib_dependencies = [
    # Package-managed
    export_dataset_metadata_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_dataset_metadata_proto_cpp_lib = static_library(
    'export_dataset_metadata_proto_cpp_lib',
    dependencies: export_dataset_metadata_proto_cpp_lib_dependencies,
    install: false
)

export_dataset_metadata_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_dataset_metadata_proto_cpp_lib],
    dependencies: export_dataset_metadata_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_dataset_metadata_proto_cpp_lib_dep]
# @end: proto_cpp (original: export_dataset_metadata_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_dataset_metadata_proto_py = custom_target(
    input: export_dataset_metadata_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: export_dataset_metadata.proto ===


# === @begin: export_task_event.proto ===
# (moved from export_api/ to top level)
export_task_event_proto = ['export_task_event.proto']

# @begin: proto_cpp (@original: export_task_event_cc_proto)
export_task_event_proto_cpp = custom_target(
    input: export_task_event_proto,
    kwargs: proto_cpp_default_kwargs
)

export_task_event_proto_cpp_dep = declare_dependency(
    sources: export_task_event_proto_cpp,
    dependencies: [common_proto_cpp_dep, export_runtime_env_proto_cpp_dep, profile_events_proto_cpp_dep]
)

export_task_event_proto_cpp_lib_dependencies = [
    # Package-managed
    export_task_event_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_task_event_proto_cpp_lib = static_library(
    'export_task_event_proto_cpp_lib',
    dependencies: export_task_event_proto_cpp_lib_dependencies,
    install: false
)

export_task_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_task_event_proto_cpp_lib],
    dependencies: export_task_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_task_event_proto_cpp_lib_dep]
# @end: proto_cpp (@original: export_task_event_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_task_event_proto_py = custom_target(
    input: export_task_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: export_task_event.proto ===


# === @begin: export_node_data.proto ===
# (moved from export_api/ to top level)
export_node_data_proto = ['export_node_data.proto']

# @begin: proto_cpp (@original: export_node_data_cc_proto)
export_node_data_proto_cpp = custom_target(
    input: export_node_data_proto,
    kwargs: proto_cpp_default_kwargs
)

export_node_data_proto_cpp_dep = declare_dependency(
    sources: export_node_data_proto_cpp
)

export_node_data_proto_cpp_lib_dependencies = [
    # Package-managed
    export_node_data_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_node_data_proto_cpp_lib = static_library(
    'export_node_data_proto_cpp_lib',
    dependencies: export_node_data_proto_cpp_lib_dependencies,
    install: false
)

export_node_data_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_node_data_proto_cpp_lib],
    dependencies: export_node_data_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_node_data_proto_cpp_lib_dep]
# @end: proto_cpp (@original: export_node_data_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_node_data_proto_py = custom_target(
    input: export_node_data_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: export_node_data.proto ===


# === @begin: export_actor_data.proto ===
# (moved from export_api/ to top level)
export_actor_data_proto = ['export_actor_data.proto']

# @begin: proto_cpp (@original: export_actor_data_cc_proto)
export_actor_data_proto_cpp = custom_target(
    input: export_actor_data_proto,
    kwargs: proto_cpp_default_kwargs
)

export_actor_data_proto_cpp_dep = declare_dependency(
    sources: export_actor_data_proto_cpp,
    dependencies: [common_proto_cpp_dep]
)

export_actor_data_proto_cpp_lib_dependencies = [
    # Package-managed
    export_actor_data_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_actor_data_proto_cpp_lib = static_library(
    'export_actor_data_proto_cpp_lib',
    dependencies: export_actor_data_proto_cpp_lib_dependencies,
    install: false
)

export_actor_data_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_actor_data_proto_cpp_lib],
    dependencies: export_actor_data_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_actor_data_proto_cpp_lib_dep]
# @end: proto_cpp (@original: export_actor_data_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_actor_data_proto_py = custom_target(
    input: export_actor_data_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: export_actor_data.proto ===


# === @begin: export_driver_job_event.proto ===
# (moved from export_api/ to top level)
export_driver_job_event_proto = ['export_driver_job_event.proto']

# @begin: proto_cpp (@original: export_driver_job_event_cc_proto)
export_driver_job_event_proto_cpp = custom_target(
    input: export_driver_job_event_proto,
    kwargs: proto_cpp_default_kwargs
)

export_driver_job_event_proto_cpp_dep = declare_dependency(
    sources: export_driver_job_event_proto_cpp,
    dependencies: [common_proto_cpp_dep, export_runtime_env_proto_cpp_dep]
)

export_driver_job_event_proto_cpp_lib_dependencies = [
    # Package-managed
    export_driver_job_event_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_driver_job_event_proto_cpp_lib = static_library(
    'export_driver_job_event_proto_cpp_lib',
    dependencies: export_driver_job_event_proto_cpp_lib_dependencies,
    install: false
)

export_driver_job_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_driver_job_event_proto_cpp_lib],
    dependencies: export_driver_job_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_driver_job_event_proto_cpp_lib_dep]
# @end: proto_cpp (@original: export_driver_job_event_cc_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_driver_job_event_proto_py = custom_target(
    input: export_driver_job_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: export_driver_job_event.proto ===


# === @begin: export_submission_job_event.proto ===
# (moved from export_api/ to top level — no longer imports anything)
export_submission_job_event_proto = ['export_submission_job_event.proto']

# @begin: proto_cpp (@original: export_submission_job_event_cpp_proto)
export_submission_job_event_proto_cpp = custom_target(
    input: export_submission_job_event_proto,
    kwargs: proto_cpp_default_kwargs
)

export_submission_job_event_proto_cpp_dep = declare_dependency(
    sources: export_submission_job_event_proto_cpp
)

export_submission_job_event_proto_cpp_lib_dependencies = [
    # Package-managed
    export_submission_job_event_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_submission_job_event_proto_cpp_lib = static_library(
    'export_submission_job_event_proto_cpp_lib',
    dependencies: export_submission_job_event_proto_cpp_lib_dependencies,
    install: false
)

export_submission_job_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_submission_job_event_proto_cpp_lib],
    dependencies: export_submission_job_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_submission_job_event_proto_cpp_lib_dep]
# @end: proto_cpp (@original: export_submission_job_event_cpp_proto)

# @begin: proto_py (@original: N/A — no Bazel py target)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_submission_job_event_proto_py = custom_target(
    input: export_submission_job_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: N/A — no Bazel py target)
# === @end: export_submission_job_event.proto ===


# === @begin: export_event.proto ===
# (moved from export_api/ to top level — now includes additional export deps)
export_event_proto = ['export_event.proto']

# @begin: proto_cpp (@original: export_event_cpp_proto)
export_event_proto_cpp = custom_target(
    input: export_event_proto,
    kwargs: proto_cpp_default_kwargs
)

export_event_proto_cpp_dep = declare_dependency(
    sources: export_event_proto_cpp,
    dependencies: [
        export_task_event_proto_cpp_dep,
        export_node_data_proto_cpp_dep,
        export_actor_data_proto_cpp_dep,
        export_driver_job_event_proto_cpp_dep,
        export_submission_job_event_proto_cpp_dep,
        export_train_state_proto_cpp_dep,
        export_dataset_operator_event_proto_cpp_dep,
        export_dataset_operator_schema_proto_cpp_dep,
        export_dataset_metadata_proto_cpp_dep
    ]
)

export_event_proto_cpp_lib_dependencies = [
    # Package-managed
    export_event_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

export_event_proto_cpp_lib = static_library(
    'export_event_proto_cpp_lib',
    dependencies: export_event_proto_cpp_lib_dependencies,
    install: false
)

export_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_event_proto_cpp_lib],
    dependencies: export_event_proto_cpp_lib_dependencies
)

all_proto_cpp_libs_dependencies += [export_event_proto_cpp_lib_dep]
# @end: proto_cpp (@original: export_event_cpp_proto)

# @begin: proto_py (@original: core_py_proto)
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs `*_pb2.py/*_pb2_grpc.py` into the wheel via `proto_py_default_kwargs` (install: true).
export_event_proto_py = custom_target(
    input: export_event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_py_proto)
# === @end: export_event.proto ===


# === @begin: Virtual `all` Protobufs Targets ===
all_proto_cpp_libs_dep = declare_dependency(
    dependencies: all_proto_cpp_libs_dependencies
    # @Important: Implies: + [opencensus_metrics_proto_cpp_dep, opencensus_resource_proto_cpp_dep]
)

all_proto_cpp_grpc_libs_dep = declare_dependency(
    dependencies: all_proto_cpp_grpc_libs_dependencies
)
# === @end: Virtual `all` Protobufs Targets ===








