# === @begin: export_runtime_env.proto ===
proto_basename = 'export_runtime_env'

export_runtime_env_proto = f'@proto_basename@.proto'

export_runtime_env_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: export_runtime_env_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

export_runtime_env_proto_cpp_dep = declare_dependency(
    sources: export_runtime_env_proto_cpp
)

export_runtime_env_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, export_runtime_env_proto_cpp_dep],
    install: false
)

export_runtime_env_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_runtime_env_proto_cpp_lib]
)

export_runtime_env_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: export_runtime_env_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: export_runtime_env.proto ===


# === @begin: export_task_event.proto ===
proto_basename = 'export_task_event'

export_task_event_proto = f'@proto_basename@.proto'

export_task_event_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: export_task_event_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

export_task_event_proto_cpp_dep = declare_dependency(
    sources: export_task_event_proto_cpp,
    dependencies: [common_proto_cpp_dep, export_runtime_env_proto_cpp_dep]
)

export_task_event_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, export_task_event_proto_cpp_dep],
    install: false
)

export_task_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_task_event_proto_cpp_lib]
)

export_task_event_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: export_task_event_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: export_task_event.proto ===


# === @begin: export_node_data.proto ===
proto_basename = 'export_node_data'

export_node_data_proto = f'@proto_basename@.proto'

export_node_data_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: export_node_data_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

export_node_data_proto_cpp_dep = declare_dependency(
    sources: export_node_data_proto_cpp
)

export_node_data_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, export_node_data_proto_cpp_dep],
    install: false
)

export_node_data_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_node_data_proto_cpp_lib]
)

export_node_data_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: export_node_data_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py'],
    build_by_default: true
)
# === @end: export_node_data.proto ===


## === @begin: export_actor_data.proto ===
#export_actor_data_proto = custom_target(
#    'export_actor_data_proto',
#    output: ['export_actor_data.pb.cc', 'export_actor_data.pb.h'],
#    input: 'export_actor_data.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#export_actor_data_proto_dep = declare_dependency(
#    sources: export_actor_data_proto,
#    dependencies: [common_proto_dep]
#)
#
#export_actor_data_cc_proto = static_library(
#    'export_actor_data_cc_proto',
#    dependencies: [export_actor_data_proto_dep]
#)
#
#export_actor_data_py_proto = custom_target(
#    'export_actor_data_py_proto',
#    output: ['export_actor_data_pb2.py', 'export_actor_data_pb2_grpc.py'],
#    input: 'export_actor_data.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: export_actor_data.proto ===
#
#
## === @begin: export_driver_job_event.proto ===
#export_driver_job_event_proto = custom_target(
#    'export_driver_job_event_proto',
#    output: ['export_driver_job_event.pb.cc', 'export_driver_job_event.pb.h'],
#    input: 'export_driver_job_event.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#export_driver_job_event_proto_dep = declare_dependency(
#    sources: export_driver_job_event_proto,
#    dependencies: [common_proto_dep, export_runtime_env_proto_dep]
#)
#
#export_driver_job_event_cc_proto = static_library(
#    'export_driver_job_event_cc_proto',
#    dependencies: [export_driver_job_event_proto_dep]
#)
#
#export_driver_job_event_py_proto = custom_target(
#    'export_driver_job_event_py_proto',
#    output: ['export_driver_job_event_pb2.py', 'export_driver_job_event_pb2_grpc.py'],
#    input: 'export_driver_job_event.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: export_driver_job_event.proto ===
#
#
## === @begin: export_submission_job_event.proto ===
#export_submission_job_event_proto = custom_target(
#    'export_submission_job_event_proto',
#    output: ['export_submission_job_event.pb.cc', 'export_submission_job_event.pb.h'],
#    input: 'export_submission_job_event.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#export_submission_job_event_proto_dep = declare_dependency(
#    sources: export_submission_job_event_proto,
#    dependencies: [common_proto_dep, export_runtime_env_proto_dep]
#)
#
#export_submission_job_event_cc_proto = static_library(
#    'export_submission_job_event_cc_proto',
#    dependencies: [export_submission_job_event_proto_dep]
#)
#
#export_submission_job_event_py_proto = custom_target(
#    'export_submission_job_event_py_proto',
#    output: ['export_submission_job_event_pb2.py', 'export_submission_job_event_pb2_grpc.py'],
#    input: 'export_submission_job_event.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: export_submission_job_event.proto ===
#
#
## === @begin: export_event.proto ===
#export_event_proto = custom_target(
#    'export_event_proto',
#    output: ['export_event.pb.cc', 'export_event.pb.h'],
#    input: 'export_event.proto',
#    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
#)
#
#export_event_proto_dep = declare_dependency(
#    sources: export_event_proto,
#    dependencies: [
#        export_task_event_proto_dep,
#        export_node_data_proto_dep,
#        export_actor_data_proto_dep,
#        export_driver_job_event_proto_dep,
#        export_submission_job_event_proto_dep
#    ]
#)
#
#export_event_cc_proto = static_library(
#    'export_event_cc_proto',
#    dependencies: [export_event_proto_dep]
#)
#
#export_event_py_proto = custom_target(
#    'export_event_py_proto',
#    output: ['export_event_pb2.py', 'export_event_pb2_grpc.py'],
#    input: 'export_event.proto',
#    command: [
#        'python3', '-m', 'grpc_tools.protoc',
#        '--proto_path=..',
#        '--python_out=' + meson.project_build_root(),
#        '--grpc_python_out=' + meson.project_build_root(),
#        '@INPUT@'
#    ]
#)
## === @end: export_event.proto ===
