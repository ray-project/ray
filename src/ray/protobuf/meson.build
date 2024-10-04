# === @begin: experimental/ ===
subdir('experimental/')
# === @end: experimental/ ===


# === @begin: runtime_env_common.proto ===
runtime_env_common_proto = ['runtime_env_common.proto']

runtime_env_common_proto_cpp = custom_target(
    input: runtime_env_common_proto,
    kwargs: proto_cpp_default_kwargs
)

runtime_env_common_proto_cpp_dep = declare_dependency(
    sources: runtime_env_common_proto_cpp
)

runtime_env_common_proto_cpp_lib = static_library(
    'runtime_env_common_proto_cpp_lib',
    dependencies: [protobuf, runtime_env_common_proto_cpp_dep],
    install: false
)

runtime_env_common_proto_cpp_lib_dep = declare_dependency(
    link_with: [runtime_env_common_proto_cpp_lib]
)

runtime_env_common_proto_py = custom_target(
    input: runtime_env_common_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: runtime_env_common.proto ===


# === @begin: common.proto ===
common_proto = ['common.proto']

common_proto_cpp = custom_target(
    input: common_proto,
    kwargs: proto_cpp_default_kwargs
)

common_proto_cpp_dep = declare_dependency(
    sources: common_proto_cpp,
    dependencies: [runtime_env_common_proto_cpp_dep]
)

common_proto_cpp_lib = static_library(
    'common_proto_cpp_lib',
    dependencies: [protobuf, common_proto_cpp_dep],
    install: false
)

common_proto_cpp_lib_dep = declare_dependency(
    link_with: [common_proto_cpp_lib]
)

common_proto_py = custom_target(
    input: common_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: common.proto ===


# === @begin: autoscaler.proto ===
autoscaler_proto = ['autoscaler.proto']

autoscaler_proto_cpp = custom_target(
    input: autoscaler_proto,
    kwargs: proto_cpp_default_kwargs
)

autoscaler_proto_cpp_dep = declare_dependency(
    sources: autoscaler_proto_cpp
)

autoscaler_proto_cpp_lib = static_library(
    'autoscaler_proto_cpp_lib',
    dependencies: [protobuf, autoscaler_proto_cpp_dep],
    install: false
)

autoscaler_proto_cpp_lib_dep = declare_dependency(
    link_with: [autoscaler_proto_cpp_lib]
)

autoscaler_proto_py = custom_target(
    input: autoscaler_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: autoscaler.proto ===


# === @begin: ray_syncer.proto ===
ray_syncer_proto = ['ray_syncer.proto']

ray_syncer_proto_cpp = custom_target(
    input: ray_syncer_proto,
    kwargs: proto_cpp_default_kwargs
)

ray_syncer_proto_cpp_dep = declare_dependency(
    sources: ray_syncer_proto_cpp
)

ray_syncer_proto_cpp_lib = static_library(
    'ray_syncer_proto_cpp_lib',
    dependencies: [protobuf, ray_syncer_proto_cpp_dep],
    install: false
)

ray_syncer_proto_cpp_lib_dep = declare_dependency(
    link_with: [ray_syncer_proto_cpp_lib]
)
# === @end: ray_syncer.proto ===


# === @begin: gcs.proto ===
gcs_proto = ['gcs.proto']

gcs_proto_cpp = custom_target(
    input: gcs_proto,
    kwargs: proto_cpp_default_kwargs
)

gcs_proto_cpp_dep = declare_dependency(
    sources: gcs_proto_cpp,
    dependencies: [common_proto_cpp_dep]
)

gcs_proto_cpp_lib = static_library(
    'gcs_proto_cpp_lib',
    dependencies: [protobuf, gcs_proto_cpp_dep],
    install: false
)

gcs_proto_cpp_lib_dep = declare_dependency(
    link_with: [gcs_proto_cpp_lib]
)

gcs_proto_py = custom_target(
    input: gcs_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: gcs.proto ===


# === @begin: logging.proto ===
logging_proto = ['logging.proto']

logging_proto_cpp = custom_target(
    input: logging_proto,
    kwargs: proto_cpp_default_kwargs
)

logging_proto_cpp_dep = declare_dependency(
    sources: logging_proto_cpp
)

logging_proto_cpp_lib = static_library(
    'logging_proto_cpp_lib',
    dependencies: [protobuf, logging_proto_cpp_dep],
    install: false
)

logging_proto_cpp_lib_dep = declare_dependency(
    link_with: [logging_proto_cpp_lib]
)

logging_proto_py = custom_target(
    input: logging_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: logging.proto ===


# === @begin: node_manager.proto ===
node_manager_proto = ['node_manager.proto']

node_manager_proto_cpp = custom_target(
    input: node_manager_proto,
    kwargs: proto_cpp_default_kwargs
)

node_manager_proto_cpp_dep = declare_dependency(
    sources: node_manager_proto_cpp,
    dependencies: [common_proto_cpp_dep, gcs_proto_cpp_dep, autoscaler_proto_cpp_dep]
)

node_manager_proto_cpp_lib = static_library(
    'node_manager_proto_cpp_lib',
    dependencies: [protobuf, node_manager_proto_cpp_dep],
    install: false
)

node_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [node_manager_proto_cpp_lib]
)

node_manager_proto_cpp_grpc = custom_target(
    input: node_manager_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

node_manager_proto_cpp_grpc_dep = declare_dependency(
    sources: node_manager_proto_cpp_grpc
)

node_manager_proto_cpp_grpc_lib = static_library(
    'node_manager_proto_cpp_grpc_lib',
    dependencies: [protobuf, grpc, node_manager_proto_cpp_grpc_dep, node_manager_proto_cpp_lib_dep],
    install: false
)

node_manager_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [node_manager_proto_cpp_grpc_lib]
)

node_manager_proto_py = custom_target(
    input: node_manager_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: node_manager.proto ===


# === @begin: reporter.proto ===
reporter_proto = 'reporter.proto'

reporter_proto_cpp = custom_target(
    input: reporter_proto,
    kwargs: proto_cpp_default_kwargs + {
        'command': [protoc_cpp_with_default_args, '-I@SOURCE_ROOT@/__eugo/include']
    }
)

reporter_proto_cpp_dep = declare_dependency(
    sources: reporter_proto_cpp,
    dependencies: [opencensus_metrics_proto_cpp_dep]
)

reporter_proto_cpp_lib = static_library(
    'reporter_proto_cpp_lib',
    dependencies: [protobuf, reporter_proto_cpp_dep],
    install: false
)

reporter_proto_cpp_lib_dep = declare_dependency(
    link_with: [reporter_proto_cpp_lib]
)

reporter_proto_py = custom_target(
    input: reporter_proto,
    kwargs: proto_py_default_kwargs + {
        'command': [protoc_py_with_default_args, '-I@SOURCE_ROOT@/__eugo/include']
    }
)
# === @end: reporter.proto ===


# === @begin: pubsub.proto ===
pubsub_proto = ['pubsub.proto']

pubsub_proto_cpp = custom_target(
    input: pubsub_proto,
    kwargs: proto_cpp_default_kwargs
)

pubsub_proto_cpp_dep = declare_dependency(
    sources: pubsub_proto_cpp,
    dependencies: [common_proto_cpp_dep, gcs_proto_cpp_dep, logging_proto_cpp_dep]
)

pubsub_proto_cpp_lib = static_library(
    'pubsub_proto_cpp_lib',
    dependencies: [protobuf, pubsub_proto_cpp_dep],
    install: false
)

pubsub_proto_cpp_lib_dep = declare_dependency(
    link_with: [pubsub_proto_cpp_lib]
)
# === @end: pubsub.proto ===


# === @begin: gcs_service.proto ===
gcs_service_proto = ['gcs_service.proto']

gcs_service_proto_cpp = custom_target(
    input: gcs_service_proto,
    kwargs: proto_cpp_default_kwargs
)

gcs_service_proto_cpp_dep = declare_dependency(
    sources: gcs_service_proto_cpp,
    dependencies: [common_proto_cpp_dep, gcs_proto_cpp_dep, pubsub_proto_cpp_dep]
)

gcs_service_proto_cpp_lib = static_library(
    'gcs_service_proto_cpp_lib',
    dependencies: [protobuf, gcs_service_proto_cpp_dep],
    install: false
)

gcs_service_proto_cpp_lib_dep = declare_dependency(
    link_with: [gcs_service_proto_cpp_lib]
)

gcs_service_proto_py = custom_target(
    input: gcs_service_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: gcs_service.proto ===


# === @begin: test_service.proto ===
test_service_proto = ['test_service.proto']

test_service_proto_cpp = custom_target(
    input: test_service_proto,
    kwargs: proto_cpp_default_kwargs
)

test_service_proto_cpp_dep = declare_dependency(
    sources: test_service_proto_cpp
)

test_service_proto_cpp_lib = static_library(
    'test_service_proto_cpp_lib',
    dependencies: [protobuf, test_service_proto_cpp_dep],
    install: false
)

test_service_proto_cpp_lib_dep = declare_dependency(
    link_with: [test_service_proto_cpp_lib]
)

test_service_proto_py = custom_target(
    input: test_service_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: test_service.proto ===


# === @begin: object_manager.proto ===
proto_basename = 'object_manager'

object_manager_proto = f'@proto_basename@.proto'

object_manager_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: object_manager_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

object_manager_proto_cpp_dep = declare_dependency(
    sources: object_manager_proto_cpp,
    dependencies: [common_proto_cpp_dep]
)

object_manager_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, object_manager_proto_cpp_dep],
    install: false
)

object_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [object_manager_proto_cpp_lib]
)
# === @end: object_manager.proto ===


# === @begin: core_worker.proto ===
proto_basename = 'core_worker'

core_worker_proto = f'@proto_basename@.proto'

core_worker_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: core_worker_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

core_worker_proto_cpp_dep = declare_dependency(
    sources: core_worker_proto_cpp,
    dependencies: [common_proto_cpp_dep, pubsub_proto_cpp_dep]
)

core_worker_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, core_worker_proto_cpp_dep],
    install: false
)

core_worker_proto_cpp_lib_dep = declare_dependency(
    link_with: [core_worker_proto_cpp_lib]
)

core_worker_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: core_worker_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: core_worker.proto ===


# === @begin: serialization.proto ===
proto_basename = 'serialization'

serialization_proto = f'@proto_basename@.proto'

serialization_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: serialization_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

serialization_proto_cpp_dep = declare_dependency(
    sources: serialization_proto_cpp
)

serialization_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, serialization_proto_cpp_dep],
    install: false
)

serialization_proto_cpp_lib_dep = declare_dependency(
    link_with: [serialization_proto_cpp_lib]
)
# === @end: serialization.proto ===


# === @begin: event.proto ===
proto_basename = 'event'

event_proto = f'@proto_basename@.proto'

event_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: event_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

event_proto_cpp_dep = declare_dependency(
    sources: event_proto_cpp
)

event_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, event_proto_cpp_dep],
    install: false
)

event_proto_cpp_lib_dep = declare_dependency(
    link_with: [event_proto_cpp_lib]
)

event_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: event_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: event.proto ===


# === @begin: agent_manager.proto ===
proto_basename = 'agent_manager'

agent_manager_proto = f'@proto_basename@.proto'

agent_manager_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: agent_manager_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

agent_manager_proto_cpp_dep = declare_dependency(
    sources: agent_manager_proto_cpp
)

agent_manager_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, agent_manager_proto_cpp_dep],
    install: false
)

agent_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [agent_manager_proto_cpp_lib]
)

agent_manager_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: agent_manager_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: agent_manager.proto ===


# === @begin: runtime_env_agent.proto ===
proto_basename = 'runtime_env_agent'

runtime_env_agent_proto = f'@proto_basename@.proto'

runtime_env_agent_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: runtime_env_agent_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

runtime_env_agent_proto_cpp_dep = declare_dependency(
    sources: runtime_env_agent_proto_cpp,
    dependencies: [runtime_env_common_proto_cpp_dep, agent_manager_proto_cpp_dep]
)

runtime_env_agent_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, runtime_env_agent_proto_cpp_dep],
    install: false
)

runtime_env_agent_proto_cpp_lib_dep = declare_dependency(
    link_with: [runtime_env_agent_proto_cpp_lib]
)

runtime_env_agent_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: runtime_env_agent_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: runtime_env_agent.proto ===


# === @begin: usage.proto ===
proto_basename = 'usage'

usage_proto = f'@proto_basename@.proto'

usage_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: usage_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

usage_proto_cpp_dep = declare_dependency(
    sources: usage_proto_cpp
)

usage_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, usage_proto_cpp_dep],
    install: false
)

usage_proto_cpp_lib_dep = declare_dependency(
    link_with: [usage_proto_cpp_lib]
)

usage_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: usage_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: usage.proto ===


# === @begin: dependency.proto ===
proto_basename = 'dependency'

dependency_proto = f'@proto_basename@.proto'

dependency_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: dependency_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: dependency.proto ===


# === @begin: job_agent.proto ===
proto_basename = 'job_agent'

job_agent_proto = f'@proto_basename@.proto'

job_agent_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: job_agent_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: job_agent.proto ===


# === @begin: ray_client.proto ===
proto_basename = 'ray_client'

ray_client_proto = f'@proto_basename@.proto'

ray_client_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: ray_client_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: ray_client.proto ===


# === @begin: serve.proto ===
proto_basename = 'serve'

serve_proto = f'@proto_basename@.proto'

serve_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: serve_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: serve.proto ===


# === @begin: export_api/ ===
subdir('export_api/')
# === @end: export_api/ ===