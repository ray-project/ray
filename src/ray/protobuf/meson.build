current_dir = fs.relative_to(meson.current_source_dir(), meson.project_source_root())
current_dir_offset = fs.relative_to(meson.project_source_root(), meson.current_source_dir())

autoscaler_proto = custom_target(
    'autoscaler_proto',
    output: ['autoscaler.pb.cc', 'autoscaler.pb.h'],
    input: 'autoscaler.proto',
    command: [
        protoc,
        '--proto_path=..',
        '--cpp_out=' + meson.project_build_root(),
        '@INPUT@',
    ]
)

autoscaler_proto_dep = declare_dependency(
    sources: autoscaler_proto
)

autoscaler_cc_proto = static_library(
    'autoscaler_cc_proto',
    [],
    dependencies: [autoscaler_proto_dep],
    include_directories: [current_dir_offset]
)

#custom_target(
#    protoc,
#    '--proto_path=' + current_dir_offset,
#    '--cpp_out=' + current_dir_offset,
#    native_source_proto_file_paths,
#    check: true
#)

# === Begin of `setup` scope ===
# === 1. .proto -> .pb.cc, .pb.h ===
#native_source_proto_files = [
#    'autoscaler.proto',
#    'usage.proto',
#    'serve.proto',
#    'runtime_env_agent.proto',
#    'pubsub.proto',
#    'ray_client.proto',
#    'agent_manager.proto',
#    'job_agent.proto',
#    'export_api/export_submission_job_event.proto',
#    'export_api/export_driver_job_event.proto',
#    'export_api/export_actor_data.proto',
#    'export_api/export_node_data.proto',
#    'export_api/export_runtime_env.proto',
#    'export_api/export_task_event.proto',
#    'export_api/export_event.proto',
#    'event.proto',
#    'serialization.proto',
#    'core_worker.proto',
#    'object_manager.proto',
#    'test_service.proto',
#    'common.proto',
#    'dependency.proto',
#    'experimental/instance_manager.proto',
#    'gcs.proto',
#    'gcs_service.proto',
#    'logging.proto',
#    'node_manager.proto',
#    'ray_syncer.proto',
#    #'reporter.proto', # @TODO: Deps: @io_opencensus_proto//opencensus/proto/metrics/v1:metrics_proto
#    'runtime_env_common.proto',
#]
#
#native_source_proto_file_paths = []
#
#foreach proto_file : native_source_proto_files
#    native_source_proto_file_paths += [
#        current_dir / proto_file
#    ]
#endforeach
#
##  @TODO: try extra -IPATH, --proto_path=PATH? may resolve current overcomplication
#run_command(
#    protoc,
#    '--proto_path=' + current_dir_offset,
#    '--cpp_out=' + current_dir_offset,
#    native_source_proto_file_paths,
#    check: true
#)
## === End of `setup` scope ===
#
#
## === Being of `compile` scope ===
#common_cc_proto = static_library(
#    'common_cc_proto',
#    ['common.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#instance_manager_cc_proto = static_library(
#    'instance_manager_cc_proto',
#    ['experimental/instance_manager.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#ray_syncer_cc_proto = static_library(
#    'ray_syncer_cc_proto',
#    ['ray_syncer.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#runtime_env_common_cc_proto = static_library(
#    'runtime_env_common_cc_proto',
#    ['runtime_env_common.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#gcs_cc_proto = static_library(
#    'gcs_cc_proto',
#    ['gcs.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#logging_cc_proto = static_library(
#    'logging_cc_proto',
#    ['logging.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#node_manager_cc_proto = static_library(
#    'node_manager_cc_proto',
#    ['node_manager.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#reporter_cc_proto = static_library(
#    'reporter_cc_proto',
#    ['reporter.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#gcs_service_cc_proto = static_library(
#    'gcs_service_cc_proto',
#    ['gcs_service.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#test_service_cc_proto = static_library(
#    'test_service_cc_proto',
#    ['test_service.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#object_manager_cc_proto = static_library(
#    'object_manager_cc_proto',
#    ['object_manager.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#core_worker_cc_proto = static_library(
#    'core_worker_cc_proto', # Original: `worker_cc_proto`
#    ['core_worker.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#serialization_cc_proto = static_library(
#    'serialization_cc_proto',
#    ['serialization.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#event_cc_proto = static_library(
#    'event_cc_proto',
#    ['event.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#export_event_cc_proto = static_library(
#    'export_event_cc_proto',
#    ['export_api/export_event.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#export_task_event_cc_proto = static_library(
#    'export_task_event_cc_proto',
#    ['export_api/export_task_event.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#export_runtime_env_cc_proto = static_library(
#    'export_runtime_env_cc_proto',
#    ['export_api/export_runtime_env.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#export_node_event_cc_proto = static_library(
#    'export_node_event_cc_proto',
#    ['export_api/export_node_event.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#export_actor_event_cc_proto = static_library(
#    'export_actor_event_cc_proto',
#    ['export_api/export_actor_event.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#export_driver_job_event_cc_proto = static_library(
#    'export_driver_job_event_cc_proto',
#    ['export_api/export_driver_job_event.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#export_submission_job_event_cc_proto = static_library(
#    'export_submission_job_event_cc_proto',
#    ['export_api/export_submission_job_event.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#agent_manager_cc_proto = static_library(
#    'agent_manager_cc_proto',
#    ['agent_manager.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#pubsub_cc_proto = static_library(
#    'pubsub_cc_proto',
#    ['pubsub.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#runtime_env_agent_cc_proto = static_library(
#    'runtime_env_agent_cc_proto',
#    ['runtime_env_agent.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#usage_cc_proto = static_library(
#    'usage_cc_proto',
#    ['usage.pb.cc'],
#    include_directories: [current_dir_offset]
#)
#
#autoscaler_cc_proto = static_library(
#    'autoscaler_cc_proto',
#    ['autoscaler.pb.cc'],
#    include_directories: [current_dir_offset]
#)
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
