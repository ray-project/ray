# === @begin: export_runtime_env.proto ===
export_runtime_env_proto = custom_target(
    'export_runtime_env_proto',
    output: ['export_runtime_env.pb.cc', 'export_runtime_env.pb.h'],
    input: 'export_runtime_env.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

export_runtime_env_proto_dep = declare_dependency(
    sources: export_runtime_env_proto
)

export_runtime_env_cc_proto = static_library(
    'export_runtime_env_cc_proto',
    dependencies: [export_runtime_env_proto_dep]
)

export_runtime_env_py_proto = custom_target(
    'export_runtime_env_py_proto',
    output: ['export_runtime_env_pb2.py', 'export_runtime_env_pb2_grpc.py'],
    input: 'export_runtime_env.proto',
    command: [
        'python3', '-m', 'grpc_tools.protoc',
        '--proto_path=..',
        '--python_out=' + meson.project_build_root(),
        '--grpc_python_out=' + meson.project_build_root(),
        '@INPUT@'
    ]
)
# === @end: export_runtime_env.proto ===


# === @begin: export_task_event.proto ===
export_task_event_proto = custom_target(
    'export_task_event_proto',
    output: ['export_task_event.pb.cc', 'export_task_event.pb.h'],
    input: 'export_task_event.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

export_task_event_proto_dep = declare_dependency(
    sources: export_task_event_proto,
    dependencies: [common_proto_dep, export_runtime_env_proto_dep]
)

export_task_event_cc_proto = static_library(
    'export_task_event_cc_proto',
    dependencies: [export_task_event_proto_dep]
)

export_task_event_py_proto = custom_target(
    'export_task_event_py_proto',
    output: ['export_task_event_pb2.py', 'export_task_event_pb2_grpc.py'],
    input: 'export_task_event.proto',
    command: [
        'python3', '-m', 'grpc_tools.protoc',
        '--proto_path=..',
        '--python_out=' + meson.project_build_root(),
        '--grpc_python_out=' + meson.project_build_root(),
        '@INPUT@'
    ]
)
# === @end: export_task_event.proto ===


# === @begin: export_node_data.proto ===
export_node_data_proto = custom_target(
    'export_node_data_proto',
    output: ['export_node_data.pb.cc', 'export_node_data.pb.h'],
    input: 'export_node_data.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

export_node_data_proto_dep = declare_dependency(
    sources: export_node_data_proto,
)

export_node_data_cc_proto = static_library(
    'export_node_data_cc_proto',
    dependencies: [export_node_data_proto_dep]
)

export_node_data_py_proto = custom_target(
    'export_node_data_py_proto',
    output: ['export_node_data_pb2.py', 'export_node_data_pb2_grpc.py'],
    input: 'export_node_data.proto',
    command: [
        'python3', '-m', 'grpc_tools.protoc',
        '--proto_path=..',
        '--python_out=' + meson.project_build_root(),
        '--grpc_python_out=' + meson.project_build_root(),
        '@INPUT@'
    ]
)
# === @end: export_node_data.proto ===


# === @begin: export_actor_data.proto ===
export_actor_data_proto = custom_target(
    'export_actor_data_proto',
    output: ['export_actor_data.pb.cc', 'export_actor_data.pb.h'],
    input: 'export_actor_data.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

export_actor_data_proto_dep = declare_dependency(
    sources: export_actor_data_proto,
    dependencies: [common_proto_dep]
)

export_actor_data_cc_proto = static_library(
    'export_actor_data_cc_proto',
    dependencies: [export_actor_data_proto_dep]
)

export_actor_data_py_proto = custom_target(
    'export_actor_data_py_proto',
    output: ['export_actor_data_pb2.py', 'export_actor_data_pb2_grpc.py'],
    input: 'export_actor_data.proto',
    command: [
        'python3', '-m', 'grpc_tools.protoc',
        '--proto_path=..',
        '--python_out=' + meson.project_build_root(),
        '--grpc_python_out=' + meson.project_build_root(),
        '@INPUT@'
    ]
)
# === @end: export_actor_data.proto ===


# === @begin: export_driver_job_event.proto ===
export_driver_job_event_proto = custom_target(
    'export_driver_job_event_proto',
    output: ['export_driver_job_event.pb.cc', 'export_driver_job_event.pb.h'],
    input: 'export_driver_job_event.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

export_driver_job_event_proto_dep = declare_dependency(
    sources: export_driver_job_event_proto,
    dependencies: [common_proto_dep, export_runtime_env_proto_dep]
)

export_driver_job_event_cc_proto = static_library(
    'export_driver_job_event_cc_proto',
    dependencies: [export_driver_job_event_proto_dep]
)

export_driver_job_event_py_proto = custom_target(
    'export_driver_job_event_py_proto',
    output: ['export_driver_job_event_pb2.py', 'export_driver_job_event_pb2_grpc.py'],
    input: 'export_driver_job_event.proto',
    command: [
        'python3', '-m', 'grpc_tools.protoc',
        '--proto_path=..',
        '--python_out=' + meson.project_build_root(),
        '--grpc_python_out=' + meson.project_build_root(),
        '@INPUT@'
    ]
)
# === @end: export_driver_job_event.proto ===


# === @begin: export_submission_job_event.proto ===
export_submission_job_event_proto = custom_target(
    'export_submission_job_event_proto',
    output: ['export_submission_job_event.pb.cc', 'export_submission_job_event.pb.h'],
    input: 'export_submission_job_event.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

export_submission_job_event_proto_dep = declare_dependency(
    sources: export_submission_job_event_proto,
    dependencies: [common_proto_dep, export_runtime_env_proto_dep]
)

export_submission_job_event_cc_proto = static_library(
    'export_submission_job_event_cc_proto',
    dependencies: [export_submission_job_event_proto_dep]
)

export_submission_job_event_py_proto = custom_target(
    'export_submission_job_event_py_proto',
    output: ['export_submission_job_event_pb2.py', 'export_submission_job_event_pb2_grpc.py'],
    input: 'export_submission_job_event.proto',
    command: [
        'python3', '-m', 'grpc_tools.protoc',
        '--proto_path=..',
        '--python_out=' + meson.project_build_root(),
        '--grpc_python_out=' + meson.project_build_root(),
        '@INPUT@'
    ]
)
# === @end: export_submission_job_event.proto ===


# === @begin: export_event.proto ===
export_event_proto = custom_target(
    'export_event_proto',
    output: ['export_event.pb.cc', 'export_event.pb.h'],
    input: 'export_event.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

export_event_proto_dep = declare_dependency(
    sources: export_event_proto,
    dependencies: [
        export_task_event_proto_dep,
        export_node_data_proto_dep,
        export_actor_data_proto_dep,
        export_driver_job_event_proto_dep,
        export_submission_job_event_proto_dep
    ]
)

export_event_cc_proto = static_library(
    'export_event_cc_proto',
    dependencies: [export_event_proto_dep]
)

export_event_py_proto = custom_target(
    'export_event_py_proto',
    output: ['export_event_pb2.py', 'export_event_pb2_grpc.py'],
    input: 'export_event.proto',
    command: [
        'python3', '-m', 'grpc_tools.protoc',
        '--proto_path=..',
        '--python_out=' + meson.project_build_root(),
        '--grpc_python_out=' + meson.project_build_root(),
        '@INPUT@'
    ]
)
# === @end: export_event.proto ===
