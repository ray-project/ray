# === @begin: task_exec_fiber_cpp_lib (@original: fiber) ===
task_exec_fiber_cpp_lib_dep = declare_dependency(
    sources: ['fiber.h'],
    dependencies: [
        logging_cpp_lib_dep,
        boost_fiber,
    ]
)
# === @end: task_exec_fiber_cpp_lib ===


# === @begin: task_exec_thread_pool_cpp_lib (@original: thread_pool) ===
task_exec_thread_pool_cpp_lib_dependencies = [
    logging_cpp_lib_dep,
    boost_asio,
    boost_thread,
]

task_exec_thread_pool_cpp_lib = static_library(
    'task_exec_thread_pool_cpp_lib',
    ['thread_pool.cc'],
    install: false,
    dependencies: task_exec_thread_pool_cpp_lib_dependencies
)

task_exec_thread_pool_cpp_lib_dep = declare_dependency(
    link_with: [task_exec_thread_pool_cpp_lib],
    dependencies: task_exec_thread_pool_cpp_lib_dependencies
)
# === @end: task_exec_thread_pool_cpp_lib ===


# === @begin: task_exec_concurrency_group_manager_cpp_lib (@original: concurrency_group_manager) ===
task_exec_concurrency_group_manager_cpp_lib_dependencies = [
    task_exec_fiber_cpp_lib_dep,
    task_exec_thread_pool_cpp_lib_dep,
    task_common_cpp_lib_dep,
]

task_exec_concurrency_group_manager_cpp_lib = static_library(
    'task_exec_concurrency_group_manager_cpp_lib',
    ['concurrency_group_manager.cc'],
    install: false,
    dependencies: task_exec_concurrency_group_manager_cpp_lib_dependencies
)

task_exec_concurrency_group_manager_cpp_lib_dep = declare_dependency(
    link_with: [task_exec_concurrency_group_manager_cpp_lib],
    dependencies: task_exec_concurrency_group_manager_cpp_lib_dependencies
)
# === @end: task_exec_concurrency_group_manager_cpp_lib ===


# === @begin: task_exec_common_cpp_lib (@original: task_execution:common) ===
task_exec_common_cpp_lib_dependencies = [
    id_cpp_lib_dep,
    ray_object_cpp_lib_dep,
    task_common_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
]

task_exec_common_cpp_lib = static_library(
    'task_exec_common_cpp_lib',
    ['common.cc'],
    install: false,
    dependencies: task_exec_common_cpp_lib_dependencies
)

task_exec_common_cpp_lib_dep = declare_dependency(
    link_with: [task_exec_common_cpp_lib],
    dependencies: task_exec_common_cpp_lib_dependencies
)
# === @end: task_exec_common_cpp_lib ===


# === @begin: task_exec_actor_queue_interface_cpp_lib (@original: actor_task_execution_queue_interface) ===
task_exec_actor_queue_interface_cpp_lib_dep = declare_dependency(
    sources: ['actor_task_execution_queue_interface.h'],
    dependencies: [
        task_exec_common_cpp_lib_dep,
        task_common_cpp_lib_dep,
        rpc_callback_types_cpp_lib_dep,
    ]
)
# === @end: task_exec_actor_queue_interface_cpp_lib ===


# === @begin: task_exec_normal_queue_cpp_lib (@original: normal_task_execution_queue) ===
task_exec_normal_queue_cpp_lib_dependencies = [
    task_exec_common_cpp_lib_dep,
    id_cpp_lib_dep,
    task_common_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    absl_synchronization,
]

task_exec_normal_queue_cpp_lib = static_library(
    'task_exec_normal_queue_cpp_lib',
    ['normal_task_execution_queue.cc'],
    install: false,
    dependencies: task_exec_normal_queue_cpp_lib_dependencies
)

task_exec_normal_queue_cpp_lib_dep = declare_dependency(
    link_with: [task_exec_normal_queue_cpp_lib],
    dependencies: task_exec_normal_queue_cpp_lib_dependencies
)
# === @end: task_exec_normal_queue_cpp_lib ===


# === @begin: task_exec_ordered_actor_queue_cpp_lib (@original: ordered_actor_task_execution_queue) ===
task_exec_ordered_actor_queue_cpp_lib_dependencies = [
    task_exec_actor_queue_interface_cpp_lib_dep,
    task_exec_common_cpp_lib_dep,
    task_exec_concurrency_group_manager_cpp_lib_dep,
    task_exec_fiber_cpp_lib_dep,
    task_exec_thread_pool_cpp_lib_dep,
    id_cpp_lib_dep,
    task_common_cpp_lib_dep,
    cw_task_event_buffer_cpp_lib_dep,       # from parent core_worker/meson.build
    common_proto_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    absl_base_core_headers,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_synchronization,
]

task_exec_ordered_actor_queue_cpp_lib = static_library(
    'task_exec_ordered_actor_queue_cpp_lib',
    ['ordered_actor_task_execution_queue.cc'],
    install: false,
    dependencies: task_exec_ordered_actor_queue_cpp_lib_dependencies
)

task_exec_ordered_actor_queue_cpp_lib_dep = declare_dependency(
    link_with: [task_exec_ordered_actor_queue_cpp_lib],
    dependencies: task_exec_ordered_actor_queue_cpp_lib_dependencies
)
# === @end: task_exec_ordered_actor_queue_cpp_lib ===


# === @begin: task_exec_unordered_actor_queue_cpp_lib (@original: unordered_actor_task_execution_queue) ===
task_exec_unordered_actor_queue_cpp_lib_dependencies = [
    task_exec_actor_queue_interface_cpp_lib_dep,
    task_exec_common_cpp_lib_dep,
    task_exec_concurrency_group_manager_cpp_lib_dep,
    task_exec_fiber_cpp_lib_dep,
    task_exec_thread_pool_cpp_lib_dep,
    id_cpp_lib_dep,
    task_common_cpp_lib_dep,
    cw_task_event_buffer_cpp_lib_dep,       # from parent core_worker/meson.build
    common_proto_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    absl_base_core_headers,
    absl_container_flat_hash_map,
    absl_synchronization,
]

task_exec_unordered_actor_queue_cpp_lib = static_library(
    'task_exec_unordered_actor_queue_cpp_lib',
    ['unordered_actor_task_execution_queue.cc'],
    install: false,
    dependencies: task_exec_unordered_actor_queue_cpp_lib_dependencies
)

task_exec_unordered_actor_queue_cpp_lib_dep = declare_dependency(
    link_with: [task_exec_unordered_actor_queue_cpp_lib],
    dependencies: task_exec_unordered_actor_queue_cpp_lib_dependencies
)
# === @end: task_exec_unordered_actor_queue_cpp_lib ===


# === @begin: task_exec_task_receiver_cpp_lib (@original: task_receiver) ===
task_exec_task_receiver_cpp_lib_dependencies = [
    task_exec_common_cpp_lib_dep,
    task_exec_concurrency_group_manager_cpp_lib_dep,
    task_exec_fiber_cpp_lib_dep,
    task_exec_normal_queue_cpp_lib_dep,
    task_exec_ordered_actor_queue_cpp_lib_dep,
    task_exec_thread_pool_cpp_lib_dep,
    task_exec_unordered_actor_queue_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_object_cpp_lib_dep,
    task_common_cpp_lib_dep,
    cw_common_cpp_lib_dep,                  # from parent core_worker:common
    cw_task_event_buffer_cpp_lib_dep,       # from parent core_worker/meson.build
    core_worker_proto_cpp_lib_dep,
    raylet_client_interface_cpp_lib_dep,
    absl_base_core_headers,
    absl_container_flat_hash_map,
]

task_exec_task_receiver_cpp_lib = static_library(
    'task_exec_task_receiver_cpp_lib',
    ['task_receiver.cc'],
    install: false,
    dependencies: task_exec_task_receiver_cpp_lib_dependencies
)

task_exec_task_receiver_cpp_lib_dep = declare_dependency(
    link_with: [task_exec_task_receiver_cpp_lib],
    dependencies: task_exec_task_receiver_cpp_lib_dependencies
)
# === @end: task_exec_task_receiver_cpp_lib ===
