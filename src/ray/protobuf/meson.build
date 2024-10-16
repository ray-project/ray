# === @begin: experimental/ ===
subdir('experimental/')
# === @end: experimental/ ===


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
# @end: proto_cpp (original: runtime_env_common_cc_proto)

# @begin: proto_py (original: runtime_env_common_py_proto)
runtime_env_common_proto_py = custom_target(
    input: runtime_env_common_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (original: runtime_env_common_py_proto)
# === @end: runtime_env_common.proto ===


# === @begin: common.proto ===
common_proto = ['common.proto']

# @begin: proto_cpp (original: common_cc_proto)
common_proto_cpp = custom_target(
    input: common_proto,
    kwargs: proto_cpp_default_kwargs
)

common_proto_cpp_dep = declare_dependency(
    sources: common_proto_cpp,
    dependencies: [runtime_env_common_proto_cpp_dep]
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
# @end: proto_cpp (original: common_cc_proto)

# @begin: proto_py (original: common_py_proto)
common_proto_py = custom_target(
    input: common_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (original: common_py_proto)
# === @end: common.proto ===


# === @begin: autoscaler.proto ===
autoscaler_proto = ['autoscaler.proto']

# @begin: proto_cpp (original: autoscaler_cc_proto)
autoscaler_proto_cpp = custom_target(
    input: autoscaler_proto,
    kwargs: proto_cpp_default_kwargs
)

autoscaler_proto_cpp_dep = declare_dependency(
    sources: autoscaler_proto_cpp
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
# @end: proto_cpp_grpc (original: autoscaler_cc_grpc)

# @begin: proto_py (original: autoscaler_py_proto)
autoscaler_proto_py = custom_target(
    input: autoscaler_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (original: autoscaler_py_proto)
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
# @end: proto_cpp_grpc (original: ray_syncer_cc_grpc)
# === @end: ray_syncer.proto ===


# === @begin: gcs.proto ===
gcs_proto = ['gcs.proto']

# @begin: proto_cpp (@original: gcs_cc_proto)
gcs_proto_cpp = custom_target(
    input: gcs_proto,
    kwargs: proto_cpp_default_kwargs
)

gcs_proto_cpp_dep = declare_dependency(
    sources: gcs_proto_cpp,
    dependencies: [common_proto_cpp_dep]
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
# @end: proto_cpp (@original: gcs_cc_proto)

# @begin: proto_py (@originial: gcs_py_proto)
gcs_proto_py = custom_target(
    input: gcs_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@originial: gcs_py_proto)
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
# @end: proto_cpp (@original: logging_cc_proto)

# @begin: proto_py (@originial: logging_py_proto)
logging_proto_py = custom_target(
    input: logging_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@originial: logging_py_proto)
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
    dependencies: [common_proto_cpp_dep, gcs_proto_cpp_dep, autoscaler_proto_cpp_dep]
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
# @end: proto_cpp_grpc (@original: node_manager_cc_grpc)

# @begin: proto_py (@originial: node_manager_py_proto)
node_manager_proto_py = custom_target(
    input: node_manager_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@originial: node_manager_py_proto)
# === @end: node_manager.proto ===


# === @begin: reporter.proto ===
reporter_proto = 'reporter.proto'

# @begin: proto_cpp (@original: reporter_cc_proto)
reporter_proto_cpp = custom_target(
    input: reporter_proto,
    kwargs: proto_cpp_default_kwargs + {
        'command': [protoc_cpp_with_default_args, '-I@SOURCE_ROOT@/eugo/include']
    }
)

reporter_proto_cpp_dep = declare_dependency(
    sources: reporter_proto_cpp,
    dependencies: [opencensus_metrics_proto_cpp_dep]
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
# @end: proto_cpp (@original: reporter_cc_proto)

# @begin: proto_cpp_grpc (@original: reporter_cc_grpc)
reporter_proto_cpp_grpc = custom_target(
    input: reporter_proto,
    kwargs: proto_cpp_grpc_default_kwargs + {
        'command': [protoc_cpp_grpc_with_default_args, '-I@SOURCE_ROOT@/eugo/include']
    }
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
# @end: proto_cpp_grpc (@original: reporter_cc_grpc)

# @begin: proto_py (@original: reporter_py_proto)
reporter_proto_py = custom_target(
    input: reporter_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: reporter_py_proto)
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
# @end: proto_cpp_grpc (@original: pubsub_cc_grpc)

# @Important:
# Original Bazel script doesn't have a rule to build these Python protos, but likely behind the scenes it generates and
# installs them together with some other protos which depend on them. In the same way, `metrics.proto` and `resource.proto`
# from `opencensus-proto` aren't specified anywhere for installation but included there, so we decided to emulate that.
# For details, find them in `@SOURCE_ROOT@/eugo/include/opencensus/`
# @begin: proto_py (@original: ???)
pubsub_proto_py = custom_target(
    input: pubsub_proto,
    kwargs: proto_py_default_kwargs
)

# @end: proto_py (@original: ???)
# === @end: pubsub.proto ===


# === @begin: gcs_service.proto ===
gcs_service_proto = ['gcs_service.proto']

# @begin: proto_cpp (@original: gcs_service_cc_proto)
gcs_service_proto_cpp = custom_target(
    input: gcs_service_proto,
    kwargs: proto_cpp_default_kwargs
)

gcs_service_proto_cpp_dep = declare_dependency(
    sources: gcs_service_proto_cpp,
    dependencies: [common_proto_cpp_dep, gcs_proto_cpp_dep, pubsub_proto_cpp_dep]
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
# @end: proto_cpp (@original: gcs_service_cc_proto)

# @begin: proto_py (@original: gcs_service_py_proto)
gcs_service_proto_py = custom_target(
    input: gcs_service_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: gcs_service_py_proto)
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
# @end: proto_cpp_grpc (@original: test_service_cc_grpc)

# @begin: proto_py (@original: test_service_py_proto)
test_service_proto_py = custom_target(
    input: test_service_proto,
    kwargs: proto_py_default_kwargs + {
        'install': false # @TODO+:why this is build et al if not installed?
    }
)
# @end: proto_py (@original: test_service_py_proto)
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
# @end: proto_cpp_grpc (@original: worker_cc_grpc (yep, original version doesn't have `core_` prefix, likely, for historical reasons))

# @begin: proto_py (@original: core_worker_py_proto (at the same time, py_proto and proto targets have the `core_` prefix, lol))
core_worker_proto_py = custom_target(
    input: core_worker_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: core_worker_py_proto (at the same time, py_proto and proto targets have the `core_` prefix, lol))
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
# @end: proto_cpp (@original: event_cc_proto)

# @begin: proto_py (@original: event_py_proto)
event_proto_py = custom_target(
    input: event_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: event_py_proto)
# === @end: event.proto ===


# === @begin: agent_manager.proto ===
agent_manager_proto = ['agent_manager.proto']

# @begin: proto_cpp (@original: agent_manager_cc_proto)
agent_manager_proto_cpp = custom_target(
    input: agent_manager_proto,
    kwargs: proto_cpp_default_kwargs
)

agent_manager_proto_cpp_dep = declare_dependency(
    sources: agent_manager_proto_cpp
)

agent_manager_proto_cpp_lib_dependencies = [
    # Package-managed
    agent_manager_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

agent_manager_proto_cpp_lib = static_library(
    'agent_manager_proto_cpp_lib',
    dependencies: agent_manager_proto_cpp_lib_dependencies,
    install: false
)

agent_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [agent_manager_proto_cpp_lib],
    dependencies: agent_manager_proto_cpp_lib_dependencies
)
# @end: proto_cpp (@original: agent_manager_cc_proto)

# @begin: proto_py (@original: agent_manager_py_proto)
agent_manager_proto_py = custom_target(
    input: agent_manager_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: agent_manager_py_proto)
# === @end: agent_manager.proto ===


# === @begin: runtime_env_agent.proto ===
runtime_env_agent_proto = ['runtime_env_agent.proto']

# @begin: proto_cpp (@original: runtime_env_agent_cc_proto)
runtime_env_agent_proto_cpp = custom_target(
    input: runtime_env_agent_proto,
    kwargs: proto_cpp_default_kwargs
)

runtime_env_agent_proto_cpp_dep = declare_dependency(
    sources: runtime_env_agent_proto_cpp,
    dependencies: [runtime_env_common_proto_cpp_dep, agent_manager_proto_cpp_dep]
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
# @end: proto_cpp (@original: runtime_env_agent_cc_proto)

# @begin: proto_py (@original: runtime_env_agent_py_proto)
runtime_env_agent_proto_py = custom_target(
    input: runtime_env_agent_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: runtime_env_agent_py_proto)
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
    'usage_proto_cpp_lib_proto_cpp_lib',
    dependencies: usage_proto_cpp_lib_dependencies,
    install: false
)

usage_proto_cpp_lib_dep = declare_dependency(
    link_with: [usage_proto_cpp_lib],
    dependencies: usage_proto_cpp_lib_dependencies
)
# @end: proto_cpp (@original: usage_cc_proto)

# @begin: proto_py (@original: usage_py_proto)
usage_proto_py = custom_target(
    input: usage_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: usage_py_proto)
# === @end: usage.proto ===


# === @begin: dependency.proto ===
dependency_proto = ['dependency.proto']

# @begin: proto_py (@original: dependency_py_proto)
dependency_proto_py = custom_target(
    input: dependency_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: dependency_py_proto)
# === @end: dependency.proto ===


# === @begin: job_agent.proto ===
job_agent_proto = ['job_agent.proto']

# @begin: proto_py (@original: job_agent_py_proto)
job_agent_proto_py = custom_target(
    input: job_agent_proto,
    kwargs: proto_py_default_kwargs,
)
# @end: proto_py (@original: job_agent_py_proto)
# === @end: job_agent.proto ===


# === @begin: ray_client.proto ===
ray_client_proto = ['ray_client.proto']

# @begin: proto_py (@original: ray_client_py_proto)
ray_client_proto_py = custom_target(
    input: ray_client_proto,
    kwargs: proto_py_default_kwargs,
)
# @end: proto_py (@original: ray_client_py_proto)
# === @end: ray_client.proto ===


# === @begin: serve.proto ===
serve_proto = ['serve.proto']

# @begin: proto_py (@original: serve_py_proto)
serve_proto_py = custom_target(
    input: serve_proto,
    kwargs: proto_py_default_kwargs + {
        'install_dir': py.get_install_dir() / 'ray/serve/generated/'
    }
)
# @end: proto_py (@original: serve_py_proto)
# === @end: serve.proto ===


# === @begin: export_api/ ===
subdir('export_api/')
# === @end: export_api/ ===
