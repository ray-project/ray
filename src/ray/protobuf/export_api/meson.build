# === @begin: export_runtime_env.proto ===
export_runtime_env_proto = ['export_runtime_env.proto']

export_runtime_env_proto_cpp = custom_target(
    input: export_runtime_env_proto,
    kwargs: proto_cpp_default_kwargs
)

export_runtime_env_proto_cpp_dep = declare_dependency(
    sources: export_runtime_env_proto_cpp
)

export_runtime_env_proto_cpp_lib = static_library(
    'export_runtime_env_proto_cpp_lib',
    dependencies: [protobuf, export_runtime_env_proto_cpp_dep],
    install: false
)

export_runtime_env_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_runtime_env_proto_cpp_lib]
)

export_runtime_env_proto_py = custom_target(
    input: export_runtime_env_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: export_runtime_env.proto ===


# === @begin: export_task_event.proto ===
export_task_event_proto = ['export_task_event.proto']

export_task_event_proto_cpp = custom_target(
    input: export_task_event_proto,
    kwargs: proto_cpp_default_kwargs
)

export_task_event_proto_cpp_dep = declare_dependency(
    sources: export_task_event_proto_cpp,
    dependencies: [common_proto_cpp_dep, export_runtime_env_proto_cpp_dep]
)

export_task_event_proto_cpp_lib = static_library(
    'export_task_event_proto_cpp_lib',
    dependencies: [protobuf, export_task_event_proto_cpp_dep],
    install: false
)

export_task_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_task_event_proto_cpp_lib]
)

export_task_event_proto_py = custom_target(
    input: export_task_event_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: export_task_event.proto ===


# === @begin: export_node_data.proto ===
export_node_data_proto = ['export_node_data.proto']

export_node_data_proto_cpp = custom_target(
    input: export_node_data_proto,
    kwargs: proto_cpp_default_kwargs
)

export_node_data_proto_cpp_dep = declare_dependency(
    sources: export_node_data_proto_cpp
)

export_node_data_proto_cpp_lib = static_library(
    'export_node_data_proto_cpp_lib',
    dependencies: [protobuf, export_node_data_proto_cpp_dep],
    install: false
)

export_node_data_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_node_data_proto_cpp_lib]
)

export_node_data_proto_py = custom_target(
    input: export_node_data_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: export_node_data.proto ===


# === @begin: export_actor_data.proto ===
export_actor_data_proto = ['export_actor_data.proto']

export_actor_data_proto_cpp = custom_target(
    input: export_actor_data_proto,
    kwargs: proto_cpp_default_kwargs
)

export_actor_data_proto_cpp_dep = declare_dependency(
    sources: export_actor_data_proto_cpp,
    dependencies: [common_proto_cpp_dep]
)

export_actor_data_proto_cpp_lib = static_library(
    'export_actor_data_proto_cpp_lib',
    dependencies: [protobuf, export_actor_data_proto_cpp_dep],
    install: false
)

export_actor_data_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_actor_data_proto_cpp_lib]
)

export_actor_data_proto_py = custom_target(
    input: export_actor_data_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: export_actor_data.proto ===


# === @begin: export_driver_job_event.proto ===
export_driver_job_event_proto = ['export_driver_job_event.proto']

export_driver_job_event_proto_cpp = custom_target(
    input: export_driver_job_event_proto,
    kwargs: proto_cpp_default_kwargs
)

export_driver_job_event_proto_cpp_dep = declare_dependency(
    sources: export_driver_job_event_proto_cpp,
    dependencies: [common_proto_cpp_dep, export_runtime_env_proto_cpp_dep]
)

export_driver_job_event_proto_cpp_lib = static_library(
    'export_driver_job_event_proto_cpp_lib',
    dependencies: [protobuf, export_driver_job_event_proto_cpp_dep],
    install: false
)

export_driver_job_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_driver_job_event_proto_cpp_lib]
)

export_driver_job_event_proto_py = custom_target(
    input: export_driver_job_event_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: export_driver_job_event.proto ===


# === @begin: export_submission_job_event.proto ===
export_submission_job_event_proto = ['export_submission_job_event.proto']

export_submission_job_event_proto_cpp = custom_target(
    input: export_submission_job_event_proto,
    kwargs: proto_cpp_default_kwargs
)

export_submission_job_event_proto_cpp_dep = declare_dependency(
    sources: export_submission_job_event_proto_cpp,
    dependencies: [common_proto_cpp_dep, export_runtime_env_proto_cpp_dep]
)

export_submission_job_event_proto_cpp_lib = static_library(
    'export_submission_job_event_proto_cpp_lib',
    dependencies: [protobuf, export_submission_job_event_proto_cpp_dep],
    install: false
)

export_submission_job_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_submission_job_event_proto_cpp_lib]
)

export_submission_job_event_proto_py = custom_target(
    input: export_submission_job_event_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: export_submission_job_event.proto ===


# === @begin: export_event.proto ===
export_event_proto = ['export_event.proto']

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
        export_submission_job_event_proto_cpp_dep
    ]
)

export_event_proto_cpp_lib = static_library(
    'export_event_proto_cpp_lib',
    dependencies: [protobuf, export_event_proto_cpp_dep],
    install: false
)

export_event_proto_cpp_lib_dep = declare_dependency(
    link_with: [export_event_proto_cpp_lib]
)

export_event_proto_py = custom_target(
    input: export_event_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: export_event.proto ===
