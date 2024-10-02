proto_files = {
    'runtime_env_common.proto': [PROTOC_TARGET_PROTO, PROTOC_TARGET_CPP, PROTOC_TARGET_CPP_STATIC_LIBRARY, PROTOC_TARGET_PY, PROTOC_TARGET_GRPC_PY]
}

protoc_targets_proto = {}
protoc_targets_cpp = {}
protoc_targets_cpp_static_library = {}
protoc_targets_py = {}
protoc_targets_grpc_py = {}
protoc_targets_grpc_cpp = {}
protoc_targets_grpc_cpp_static_library = {}

foreach proto_file, proto_file_targets : proto_files
    proto_file_without_extension = fs.stem(proto_file)

    if proto_file_targets.contains(PROTOC_TARGET_PROTO)
        protoc_targets_proto += {proto_file: custom_target(
            '@proto_file_without_extension@_proto',
            output: ['@proto_file_without_extension@.pb.cc', '@proto_file_without_extension@.pb.h'],
            input: proto_file,
            command: [protoc, '--proto_path=..', '--cpp_out=@BUILD_ROOT@', '@INPUT@']
        )}
    endif

    #runtime_env_common_proto_dep = declare_dependency(
    #    sources: runtime_env_common_proto
    #)
    #
    #runtime_env_common_cc_proto = static_library(
    #    'runtime_env_common_cc_proto',
    #    dependencies: [runtime_env_common_proto_dep]
    #)
    #
    #runtime_env_common_py_proto = custom_target(
    #    'runtime_env_common_py_proto',
    #    output: ['runtime_env_common_pb2.py', 'runtime_env_common_pb2_grpc.py'],
    #    input: 'runtime_env_common.proto',
    #    command: [
    #        'python3', '-m', 'grpc_tools.protoc',
    #        '--proto_path=..',
    #        '--python_out=' + meson.project_build_root(),
    #        '--grpc_python_out=' + meson.project_build_root(),
    #        '@INPUT@'
    #    ]
    #)
endforeach


## === @begin: experimental/ ===
#subdir('experimental/')
## === @end: experimental/ ===
#
#
## === @begin: runtime_env_common.proto ===
#runtime_env_common_proto = custom_target(
#    'runtime_env_common_proto',
#    output: ['runtime_env_common.pb.cc', 'runtime_env_common.pb.h'],
#    input: 'runtime_env_common.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#runtime_env_common_proto_dep = declare_dependency(
#    sources: runtime_env_common_proto
#)
#
#runtime_env_common_cc_proto = static_library(
#    'runtime_env_common_cc_proto',
#    dependencies: [runtime_env_common_proto_dep]
#)
#
#runtime_env_common_py_proto = custom_target(
#    'runtime_env_common_py_proto',
#    output: ['runtime_env_common_pb2.py', 'runtime_env_common_pb2_grpc.py'],
#    input: 'runtime_env_common.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: runtime_env_common.proto ===
#
#
## === @begin: common.proto ===
#common_proto = custom_target(
#    'common_proto',
#    output: ['common.pb.cc', 'common.pb.h'],
#    input: 'common.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@'],
#)
#
#common_proto_dep = declare_dependency(
#    sources: common_proto,
#    dependencies: [runtime_env_common_proto_dep]
#)
#
#common_cc_proto = static_library(
#    'common_cc_proto',
#    dependencies: [common_proto_dep]
#)
#
#common_py_proto = custom_target(
#    'common_py_proto',
#    output: ['common_pb2.py', 'common_pb2_grpc.py'],
#    input: 'common.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: common.proto ===
#
#
## === @begin: autoscaler.proto ===
#autoscaler_proto = custom_target(
#    'autoscaler_proto',
#    output: ['autoscaler.pb.cc', 'autoscaler.pb.h'],
#    input: 'autoscaler.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#autoscaler_proto_dep = declare_dependency(
#    sources: autoscaler_proto
#)
#
#autoscaler_cc_proto = static_library(
#    'autoscaler_cc_proto',
#    dependencies: [autoscaler_proto_dep]
#)
## === @end: autoscaler.proto ===
#
#
## === @begin: ray_syncer.proto ===
#ray_syncer_proto = custom_target(
#    'ray_syncer_proto',
#    output: ['ray_syncer.pb.cc', 'ray_syncer.pb.h'],
#    input: 'ray_syncer.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#ray_syncer_proto_dep = declare_dependency(
#    sources: ray_syncer_proto
#)
#
#ray_syncer_cc_proto = static_library(
#    'ray_syncer_cc_proto',
#    dependencies: [ray_syncer_proto_dep]
#)
## === @end: ray_syncer.proto ===
#
#
## === @begin: gcs.proto ===
#gcs_proto = custom_target(
#    'gcs_proto',
#    output: ['gcs.pb.cc', 'gcs.pb.h'],
#    input: 'gcs.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#gcs_proto_dep = declare_dependency(
#    sources: gcs_proto,
#    dependencies: [common_proto_dep]
#)
#
#gcs_cc_proto = static_library(
#    'gcs_cc_proto',
#    dependencies: [gcs_proto_dep]
#)
#
#gcs_py_proto = custom_target(
#    'gcs_py_proto',
#    output: ['gcs_pb2.py', 'gcs_pb2_grpc.py'],
#    input: 'gcs.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: gcs.proto ===
#
#
## === @begin: logging.proto ===
#logging_proto = custom_target(
#    'logging_proto',
#    output: ['logging.pb.cc', 'logging.pb.h'],
#    input: 'logging.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@'],
#)
#
#logging_proto_dep = declare_dependency(
#    sources: logging_proto
#)
#
#logging_cc_proto = static_library(
#    'logging_cc_proto',
#    dependencies: [logging_proto_dep]
#)
#
#logging_py_proto = custom_target(
#    'logging_py_proto',
#    output: ['logging_pb2.py', 'logging_pb2_grpc.py'],
#    input: 'logging.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: logging.proto ===
#
#
## === @begin: node_manager.proto ===
#node_manager_proto = custom_target(
#    'node_manager_proto',
#    output: ['node_manager.pb.cc', 'node_manager.pb.h'],
#    input: 'node_manager.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#node_manager_proto_dep = declare_dependency(
#    sources: node_manager_proto,
#    dependencies: [common_proto_dep, gcs_proto_dep, autoscaler_proto_dep]
#)
#
#node_manager_cc_proto = static_library(
#    'node_manager_cc_proto',
#    dependencies: [node_manager_proto_dep]
#)
#
#node_manager_py_proto = custom_target(
#    'node_manager_py_proto',
#    output: ['node_manager_pb2.py', 'node_manager_pb2_grpc.py'],
#    input: 'node_manager.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: node_manager.proto ===
#
#
## === @begin: reporter.proto ===
#reporter_proto = custom_target(
#    'reporter_proto',
#    output: ['reporter.pb.cc', 'reporter.pb.h'],
#    input: 'reporter.proto',
#    command: [
#        protoc,
#        '--proto_path=..',
#        '--cpp_out=' + meson.project_build_root(),
#        '-I' + meson.project_source_root() / '__eugo' / 'include',
#        '@INPUT@'
#    ],
#)
#
#reporter_proto_dep = declare_dependency(
#    sources: reporter_proto,
#    dependencies: [opencensus_proto_metrics_v1_metrics_proto_dep]
#)
#
#reporter_cc_proto = static_library(
#    'reporter_cc_proto',
#    dependencies: [reporter_proto_dep]
#)
#
#reporter_py_proto = custom_target(
#    'reporter_py_proto',
#    output: ['reporter_pb2.py', 'reporter_pb2_grpc.py'],
#    input: 'reporter.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: reporter.proto ===
#
#
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
##python_grpc_compile(
##    name = "autoscaler_py_proto",
##    deps = [":autoscaler_proto"],
##)
