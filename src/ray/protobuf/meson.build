# === @begin: experimental/ ===
subdir('experimental/')
# === @end: experimental/ ===


# === @begin: runtime_env_common.proto ===
runtime_env_common_proto = custom_target(
    'runtime_env_common_proto',
    output: ['runtime_env_common.pb.cc', 'runtime_env_common.pb.h'],
    input: 'runtime_env_common.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

runtime_env_common_proto_dep = declare_dependency(
    sources: runtime_env_common_proto
)

runtime_env_common_cc_proto = static_library(
    'runtime_env_common_cc_proto',
    dependencies: [runtime_env_common_proto_dep]
)
# === @end: runtime_env_common.proto ===


# === @begin: common.proto ===
common_proto = custom_target(
    'common_proto',
    output: ['common.pb.cc', 'common.pb.h'],
    input: 'common.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@'],
)

common_proto_dep = declare_dependency(
    sources: common_proto,
    dependencies: [runtime_env_common_proto_dep]
)

common_cc_proto = static_library(
    'common_cc_proto',
    dependencies: [common_proto_dep]
)
# === @end: common.proto ===


# === @begin: autoscaler.proto ===
autoscaler_proto = custom_target(
    'autoscaler_proto',
    output: ['autoscaler.pb.cc', 'autoscaler.pb.h'],
    input: 'autoscaler.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

autoscaler_proto_dep = declare_dependency(
    sources: autoscaler_proto
)

autoscaler_cc_proto = static_library(
    'autoscaler_cc_proto',
    dependencies: [autoscaler_proto_dep]
)
# === @end: autoscaler.proto ===


# === @begin: ray_syncer.proto ===
ray_syncer_proto = custom_target(
    'ray_syncer_proto',
    output: ['ray_syncer.pb.cc', 'ray_syncer.pb.h'],
    input: 'ray_syncer.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

ray_syncer_proto_dep = declare_dependency(
    sources: ray_syncer_proto
)

ray_syncer_cc_proto = static_library(
    'ray_syncer_cc_proto',
    dependencies: [ray_syncer_proto_dep]
)
# === @end: ray_syncer.proto ===


# === @begin: gcs.proto ===
gcs_proto = custom_target(
    'gcs_proto',
    output: ['gcs.pb.cc', 'gcs.pb.h'],
    input: 'gcs.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

gcs_proto_dep = declare_dependency(
    sources: gcs_proto,
    dependencies: [common_proto_dep]
)

gcs_cc_proto = static_library(
    'gcs_cc_proto',
    dependencies: [gcs_proto_dep]
)
# === @end: gcs.proto ===


# === @begin: logging.proto ===
logging_proto = custom_target(
    'logging_proto',
    output: ['logging.pb.cc', 'logging.pb.h'],
    input: 'logging.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@'],
)

logging_proto_dep = declare_dependency(
    sources: logging_proto
)

logging_cc_proto = static_library(
    'logging_cc_proto',
    dependencies: [logging_proto_dep]
)
# === @end: logging.proto ===


# === @begin: node_manager.proto ===
node_manager_proto = custom_target(
    'node_manager_proto',
    output: ['node_manager.pb.cc', 'node_manager.pb.h'],
    input: 'node_manager.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

node_manager_proto_dep = declare_dependency(
    sources: node_manager_proto,
    dependencies: [common_proto_dep, gcs_proto_dep, autoscaler_proto_dep]
)

node_manager_cc_proto = static_library(
    'node_manager_cc_proto',
    dependencies: [node_manager_proto_dep]
)
# === @end: node_manager.proto ===


# === @begin: reporter.proto ===
reporter_proto = custom_target(
    'reporter_proto',
    output: ['reporter.pb.cc', 'reporter.pb.h'],
    input: 'reporter.proto',
    command: [
        protoc,
        '--proto_path=..',
        '--cpp_out=' + meson.project_build_root(),
        '-I' + meson.project_source_root() / '__eugo' / 'include',
        '@INPUT@'
    ],
)

reporter_proto_dep = declare_dependency(
    sources: reporter_proto,
    dependencies: [opencensus_proto_metrics_v1_metrics_proto_dep]
)

reporter_cc_proto = static_library(
    'reporter_cc_proto',
    dependencies: [reporter_proto_dep]
)
# === @end: reporter.proto ===


# === @begin: pubsub.proto ===
pubsub_proto = custom_target(
    'pubsub_proto',
    output: ['pubsub.pb.cc', 'pubsub.pb.h'],
    input: 'pubsub.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

pubsub_proto_dep = declare_dependency(
    sources: pubsub_proto,
    dependencies: [common_proto_dep, gcs_proto_dep, logging_proto_dep]
)

pubsub_cc_proto = static_library(
    'pubsub_cc_proto',
    dependencies: [pubsub_proto_dep]
)
# === @end: pubsub.proto ===


# === @begin: gcs_service.proto ===
gcs_service_proto = custom_target(
    'gcs_service_proto',
    output: ['gcs_service.pb.cc', 'gcs_service.pb.h'],
    input: 'gcs_service.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

gcs_service_proto_dep = declare_dependency(
    sources: gcs_service_proto,
    dependencies: [common_proto_dep, gcs_proto_dep, pubsub_proto_dep]
)

gcs_service_cc_proto = static_library(
    'gcs_service_cc_proto',
    dependencies: [gcs_service_proto_dep]
)
# === @end: gcs_service.proto ===


# === @begin: test_service.proto ===
test_service_proto = custom_target(
    'test_service_proto',
    output: ['test_service.pb.cc', 'test_service.pb.h'],
    input: 'test_service.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

test_service_proto_dep = declare_dependency(
    sources: test_service_proto
)

test_service_cc_proto = static_library(
    'test_service_cc_proto',
    dependencies: [test_service_proto_dep]
)
# === @end: test_service.proto ===


# === @begin: object_manager.proto ===
object_manager_proto = custom_target(
    'object_manager_proto',
    output: ['object_manager.pb.cc', 'object_manager.pb.h'],
    input: 'object_manager.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

object_manager_proto_dep = declare_dependency(
    sources: object_manager_proto,
    dependencies: [common_proto_dep]
)

object_manager_cc_proto = static_library(
    'object_manager_cc_proto',
    dependencies: [object_manager_proto_dep]
)
# === @end: object_manager.proto ===


# === @begin: core_worker.proto ===
core_worker_proto = custom_target(
    'core_worker_proto',
    output: ['core_worker.pb.cc', 'core_worker.pb.h'],
    input: 'core_worker.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

core_worker_proto_dep = declare_dependency(
    sources: core_worker_proto,
    dependencies: [common_proto_dep, pubsub_proto_dep]
)

core_worker_cc_proto = static_library(
    'core_worker_cc_proto',
    dependencies: [core_worker_proto_dep]
)
# === @end: core_worker.proto ===


# === @begin: serialization.proto ===
serialization_proto = custom_target(
    'serialization_proto',
    output: ['serialization.pb.cc', 'serialization.pb.h'],
    input: 'serialization.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

serialization_proto_dep = declare_dependency(
    sources: serialization_proto
)

serialization_cc_proto = static_library(
    'serialization_cc_proto',
    dependencies: [serialization_proto_dep]
)
# === @end: serialization.proto ===


# === @begin: event.proto ===
event_proto = custom_target(
    'event_proto',
    output: ['event.pb.cc', 'event.pb.h'],
    input: 'event.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

event_proto_dep = declare_dependency(
    sources: event_proto
)

event_cc_proto = static_library(
    'event_cc_proto',
    dependencies: [event_proto_dep]
)
# === @end: event.proto ===


# === @begin: agent_manager.proto ===
agent_manager_proto = custom_target(
    'agent_manager_proto',
    output: ['agent_manager.pb.cc', 'agent_manager.pb.h'],
    input: 'agent_manager.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

agent_manager_proto_dep = declare_dependency(
    sources: agent_manager_proto
)

agent_manager_cc_proto = static_library(
    'agent_manager_cc_proto',
    dependencies: [agent_manager_proto_dep]
)
# === @end: agent_manager.proto ===


# === @begin: runtime_env_agent.proto ===
runtime_env_agent_proto = custom_target(
    'runtime_env_agent_proto',
    output: ['runtime_env_agent.pb.cc', 'runtime_env_agent.pb.h'],
    input: 'runtime_env_agent.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

runtime_env_agent_proto_dep = declare_dependency(
    sources: runtime_env_agent_proto,
    dependencies: [runtime_env_common_proto_dep, agent_manager_proto_dep]
)

runtime_env_agent_cc_proto = static_library(
    'runtime_env_agent_cc_proto',
    dependencies: [runtime_env_agent_proto_dep]
)
# === @end: runtime_env_agent.proto ===


# === @begin: usage.proto ===
usage_proto = custom_target(
    'usage_proto',
    output: ['usage.pb.cc', 'usage.pb.h'],
    input: 'usage.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

usage_proto_dep = declare_dependency(
    sources: usage_proto
)

usage_cc_proto = static_library(
    'usage_cc_proto',
    dependencies: [usage_proto_dep]
)
# === @end: usage.proto ===


# === @begin: export_api/ ===
subdir('export_api/')
# === @end: export_api/ ===



# === Begin of `setup` scope ===
# === 1. .proto -> .pb.cc, .pb.h ===
native_source_proto_files = [
    'dependency.proto',
    'job_agent.proto',
    'ray_client.proto',
    'serve.proto'
]

# === 2. .proto -> .pb2.py ===

# === End of `compile` scope ===



#python_grpc_compile(
#    name = "common_py_proto",
#    deps = [":common_proto"],
#)

#python_grpc_compile(
#    name = "instance_manager_py_proto",
#    deps = [":instance_manager_proto"],
#)

#python_grpc_compile(
#    name = "runtime_env_common_py_proto",
#    deps = [":runtime_env_common_proto"],
#)

#python_grpc_compile(
#    name = "gcs_py_proto",
#    deps = [":gcs_proto"],
#)

#python_grpc_compile(
#    name = "dependency_py_proto",
#    deps = [":dependency_proto"],
#)

#python_grpc_compile(
#    name = "logging_py_proto",
#    deps = [":logging_proto"],
#)

#python_grpc_compile(
#    name = "node_manager_py_proto",
#    deps = [":node_manager_proto"],
#)

#python_grpc_compile(
#    name = "reporter_py_proto",
#    deps = [":reporter_proto"],
#)

#python_grpc_compile(
#    name = "gcs_service_py_proto",
#    deps = [":gcs_service_proto"],
#)
#

#python_grpc_compile(
#    name = "test_service_py_proto",
#    deps = [":test_service_proto"],
#)

#python_grpc_compile(
#    name = "core_worker_py_proto",
#    deps = [":core_worker_proto"],
#)

#python_grpc_compile(
#    name = "event_py_proto",
#    deps = [":event_proto"],
#)

#python_grpc_compile(
#    name = "export_event_py_proto",
#    deps = [":export_event_proto"],
#)

#python_grpc_compile(
#    name = "export_task_event_py_proto",
#    deps = [":export_task_event_proto"],
#)

#python_grpc_compile(
#    name = "export_runtime_env_py_proto",
#    deps = [":export_runtime_env_proto"],
#)

#python_grpc_compile(
#    name = "export_node_event_py_proto",
#    deps = [":export_node_event_proto"],
#)

#python_grpc_compile(
#    name = "export_actor_event_py_proto",
#    deps = [":export_actor_event_proto"],
#)

#python_grpc_compile(
#    name = "export_driver_job_event_py_proto",
#    deps = [":export_driver_job_event_proto"],
#)

#python_grpc_compile(
#    name = "export_submission_job_event_py_proto",
#    deps = [":export_submission_job_event_proto"],
#)

#python_grpc_compile(
#    name = "job_agent_py_proto",
#    deps = [":job_agent_proto"],
#)

#python_grpc_compile(
#    name = "agent_manager_py_proto",
#    deps = [":agent_manager_proto"],
#)

#python_grpc_compile(
#    name = "ray_client_py_proto",
#    deps = [":ray_client_proto"],
#)

#python_grpc_compile(
#    name = "runtime_env_agent_py_proto",
#    deps = [":runtime_env_agent_proto"],
#)

#python_grpc_compile(
#    name = "serve_py_proto",
#    deps = [":serve_proto"],
#)

#python_grpc_compile(
#    name = "usage_py_proto",
#    deps = [":usage_proto"],
#)

#python_grpc_compile(
#    name = "autoscaler_py_proto",
#    deps = [":autoscaler_proto"],
#)
