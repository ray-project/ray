# === @begin: experimental/ ===
subdir('experimental/')
# === @end: experimental/ ===


# === @begin: runtime_env_common.proto ===
proto_basename = 'runtime_env_common'

runtime_env_common_proto = f'@proto_basename@.proto'

runtime_env_common_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: runtime_env_common_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

runtime_env_common_proto_cpp_dep = declare_dependency(
    sources: runtime_env_common_proto_cpp
)

runtime_env_common_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, runtime_env_common_proto_cpp_dep]
)

runtime_env_common_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: runtime_env_common_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: runtime_env_common.proto ===


# === @begin: common.proto ===
proto_basename = 'common'

common_proto = f'@proto_basename@.proto'

common_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: common_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

common_proto_cpp_dep = declare_dependency(
    sources: common_proto_cpp,
    dependencies: [runtime_env_common_proto_cpp_dep]
)

common_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, common_proto_cpp_dep]
)

common_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: common_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: common.proto ===


# === @begin: autoscaler.proto ===
proto_basename = 'autoscaler'

autoscaler_proto = f'@proto_basename@.proto'

autoscaler_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: autoscaler_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

autoscaler_proto_cpp_dep = declare_dependency(
    sources: autoscaler_proto_cpp
)

autoscaler_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, autoscaler_proto_cpp_dep]
)

autoscaler_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: autoscaler_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: autoscaler.proto ===


# === @begin: ray_syncer.proto ===
proto_basename = 'ray_syncer'

ray_syncer_proto = f'@proto_basename@.proto'

ray_syncer_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: ray_syncer_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

ray_syncer_proto_cpp_dep = declare_dependency(
    sources: ray_syncer_proto_cpp
)

ray_syncer_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, ray_syncer_proto_cpp_dep]
)
# === @end: ray_syncer.proto ===


# === @begin: gcs.proto ===
proto_basename = 'gcs'

gcs_proto = f'@proto_basename@.proto'

gcs_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: gcs_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

gcs_proto_cpp_dep = declare_dependency(
    sources: gcs_proto_cpp,
    dependencies: [common_proto_cpp_dep]
)

gcs_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, gcs_proto_cpp_dep]
)

gcs_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: gcs_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: gcs.proto ===


# === @begin: logging.proto ===
proto_basename = 'logging'

logging_proto = f'@proto_basename@.proto'

logging_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: logging_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

logging_proto_cpp_dep = declare_dependency(
    sources: logging_proto_cpp
)

logging_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, logging_proto_cpp_dep]
)

logging_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: logging_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: logging.proto ===


# === @begin: node_manager.proto ===
proto_basename = 'node_manager'

node_manager_proto = f'@proto_basename@.proto'

node_manager_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: node_manager_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

node_manager_proto_cpp_dep = declare_dependency(
    sources: node_manager_proto_cpp,
    dependencies: [common_proto_cpp_dep, gcs_proto_cpp_dep, autoscaler_proto_cpp_dep]
)

node_manager_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, node_manager_proto_cpp_dep]
)

node_manager_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: node_manager_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: node_manager.proto ===


# === @begin: reporter.proto ===
proto_basename = 'reporter'

reporter_proto = f'@proto_basename@.proto'

reporter_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: reporter_proto,
    command: [protoc_cpp_with_default_args, '-I@SOURCE_ROOT@/__eugo/include'],
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

reporter_proto_cpp_dep = declare_dependency(
    sources: reporter_proto_cpp,
    dependencies: [opencensus_metrics_proto_cpp_dep]
)

reporter_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, reporter_proto_cpp_dep]
)

reporter_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: reporter_proto,
    command: [protoc_py_with_default_args, '-I@SOURCE_ROOT@/__eugo/include'],
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: reporter.proto ===


## === @begin: pubsub.proto ===
#pubsub_proto = custom_target(
#    'pubsub_proto',
#    output: ['pubsub.pb.cc', 'pubsub.pb.h'],
#    input: 'pubsub.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#pubsub_proto_dep = declare_dependency(
#    sources: pubsub_proto,
#    dependencies: [common_proto_dep, gcs_proto_dep, logging_proto_dep]
#)
#
#pubsub_cc_proto = static_library(
#    'pubsub_cc_proto',
#    dependencies: [pubsub_proto_dep]
#)
## === @end: pubsub.proto ===
#
#
## === @begin: gcs_service.proto ===
#gcs_service_proto = custom_target(
#    'gcs_service_proto',
#    output: ['gcs_service.pb.cc', 'gcs_service.pb.h'],
#    input: 'gcs_service.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#gcs_service_proto_dep = declare_dependency(
#    sources: gcs_service_proto,
#    dependencies: [common_proto_dep, gcs_proto_dep, pubsub_proto_dep]
#)
#
#gcs_service_cc_proto = static_library(
#    'gcs_service_cc_proto',
#    dependencies: [gcs_service_proto_dep]
#)
#
#gcs_service_py_proto = custom_target(
#    'gcs_service_py_proto',
#    output: ['gcs_service_pb2.py', 'gcs_service_pb2_grpc.py'],
#    input: 'gcs_service.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: gcs_service.proto ===
#
#
## === @begin: test_service.proto ===
#test_service_proto = custom_target(
#    'test_service_proto',
#    output: ['test_service.pb.cc', 'test_service.pb.h'],
#    input: 'test_service.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#test_service_proto_dep = declare_dependency(
#    sources: test_service_proto
#)
#
#test_service_cc_proto = static_library(
#    'test_service_cc_proto',
#    dependencies: [test_service_proto_dep]
#)
#
#test_service_py_proto = custom_target(
#    'test_service_py_proto',
#    output: ['test_service_pb2.py', 'test_service_pb2_grpc.py'],
#    input: 'test_service.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: test_service.proto ===
#
#
## === @begin: object_manager.proto ===
#object_manager_proto = custom_target(
#    'object_manager_proto',
#    output: ['object_manager.pb.cc', 'object_manager.pb.h'],
#    input: 'object_manager.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#object_manager_proto_dep = declare_dependency(
#    sources: object_manager_proto,
#    dependencies: [common_proto_dep]
#)
#
#object_manager_cc_proto = static_library(
#    'object_manager_cc_proto',
#    dependencies: [object_manager_proto_dep]
#)
## === @end: object_manager.proto ===
#
#
## === @begin: core_worker.proto ===
#core_worker_proto = custom_target(
#    'core_worker_proto',
#    output: ['core_worker.pb.cc', 'core_worker.pb.h'],
#    input: 'core_worker.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#core_worker_proto_dep = declare_dependency(
#    sources: core_worker_proto,
#    dependencies: [common_proto_dep, pubsub_proto_dep]
#)
#
#core_worker_cc_proto = static_library(
#    'core_worker_cc_proto',
#    dependencies: [core_worker_proto_dep]
#)
#
#core_worker_py_proto = custom_target(
#    'core_worker_py_proto',
#    output: ['core_worker_pb2.py', 'core_worker_pb2_grpc.py'],
#    input: 'core_worker.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: core_worker.proto ===
#
#
## === @begin: serialization.proto ===
#serialization_proto = custom_target(
#    'serialization_proto',
#    output: ['serialization.pb.cc', 'serialization.pb.h'],
#    input: 'serialization.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#serialization_proto_dep = declare_dependency(
#    sources: serialization_proto
#)
#
#serialization_cc_proto = static_library(
#    'serialization_cc_proto',
#    dependencies: [serialization_proto_dep]
#)
## === @end: serialization.proto ===
#
#
## === @begin: event.proto ===
#event_proto = custom_target(
#    'event_proto',
#    output: ['event.pb.cc', 'event.pb.h'],
#    input: 'event.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#event_proto_dep = declare_dependency(
#    sources: event_proto
#)
#
#event_cc_proto = static_library(
#    'event_cc_proto',
#    dependencies: [event_proto_dep]
#)
#
#event_py_proto = custom_target(
#    'event_py_proto',
#    output: ['event_pb2.py', 'event_pb2_grpc.py'],
#    input: 'event.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: event.proto ===
#
#
## === @begin: agent_manager.proto ===
#agent_manager_proto = custom_target(
#    'agent_manager_proto',
#    output: ['agent_manager.pb.cc', 'agent_manager.pb.h'],
#    input: 'agent_manager.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#agent_manager_proto_dep = declare_dependency(
#    sources: agent_manager_proto
#)
#
#agent_manager_cc_proto = static_library(
#    'agent_manager_cc_proto',
#    dependencies: [agent_manager_proto_dep]
#)
#
#agent_manager_py_proto = custom_target(
#    'agent_manager_py_proto',
#    output: ['agent_manager_pb2.py', 'agent_manager_pb2_grpc.py'],
#    input: 'agent_manager.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: agent_manager.proto ===
#
#
## === @begin: runtime_env_agent.proto ===
#runtime_env_agent_proto = custom_target(
#    'runtime_env_agent_proto',
#    output: ['runtime_env_agent.pb.cc', 'runtime_env_agent.pb.h'],
#    input: 'runtime_env_agent.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#runtime_env_agent_proto_dep = declare_dependency(
#    sources: runtime_env_agent_proto,
#    dependencies: [runtime_env_common_proto_dep, agent_manager_proto_dep]
#)
#
#runtime_env_agent_cc_proto = static_library(
#    'runtime_env_agent_cc_proto',
#    dependencies: [runtime_env_agent_proto_dep]
#)
#
#runtime_env_agent_py_proto = custom_target(
#    'runtime_env_agent_py_proto',
#    output: ['runtime_env_agent_pb2.py', 'runtime_env_agent_pb2_grpc.py'],
#    input: 'runtime_env_agent.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: runtime_env_agent.proto ===
#
#
## === @begin: usage.proto ===
#usage_proto = custom_target(
#    'usage_proto',
#    output: ['usage.pb.cc', 'usage.pb.h'],
#    input: 'usage.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#usage_proto_dep = declare_dependency(
#    sources: usage_proto
#)
#
#usage_cc_proto = static_library(
#    'usage_cc_proto',
#    dependencies: [usage_proto_dep]
#)
## === @end: usage.proto ===
#
#
## === @begin: export_api/ ===
#subdir('export_api/')
## === @end: export_api/ ===
#
#
## === @begin: dependency.proto ===
#dependency_py_proto = custom_target(
#    'dependency_py_proto',
#    output: ['dependency_pb2.py', 'dependency_pb2_grpc.py'],
#    input: 'dependency.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: dependency.proto ===
#
#
## === @begin: job_agent.proto ===
#job_agent_py_proto = custom_target(
#    'job_agent_py_proto',
#    output: ['job_agent_pb2.py', 'job_agent_pb2_grpc.py'],
#    input: 'job_agent.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: job_agent.proto ===
#
#
## === @begin: ray_client.proto ===
#ray_client_py_proto = custom_target(
#    'ray_client_py_proto',
#    output: ['ray_client_pb2.py', 'ray_client_pb2_grpc.py'],
#    input: 'ray_client.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: ray_client.proto ===
#
#
## === @begin: serve.proto ===
#serve_py_proto = custom_target(
#    'serve_py_proto',
#    output: ['serve_pb2.py', 'serve_pb2_grpc.py'],
#    input: 'serve.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: serve.proto ===
#
#
##python_grpc_compile(
##    name = "usage_py_proto",
##    deps = [":usage_proto"],
##)
#

