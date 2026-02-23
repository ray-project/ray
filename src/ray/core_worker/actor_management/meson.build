# @IMPORTANT: `actor_manager` is defined in the parent core_worker/meson.build
# because it depends on `task_submission:actor_task_submitter`, which in turn
# depends on `actor_management:actor_creator`. This breaks the circular dep.

# === @begin: am_actor_handle_cpp_lib (@original: actor_handle) ===
am_actor_handle_cpp_lib_dependencies = [
    # Package-managed
    id_cpp_lib_dep,
    task_common_cpp_lib_dep,
    core_worker_context_cpp_lib_dep,             # from parent core_worker/meson.build
    core_worker_proto_cpp_lib_dep,

    # Eugo-managed
    gcs_proto_cpp_lib_dep,
    absl_types_optional,
    # "@com_google_googletest//:gtest_prod", # only needed for testing
]

am_actor_handle_cpp_lib = static_library(
    'am_actor_handle_cpp_lib',
    ['actor_handle.cc'],
    install: false,
    dependencies: am_actor_handle_cpp_lib_dependencies
)

am_actor_handle_cpp_lib_dep = declare_dependency(
    link_with: [am_actor_handle_cpp_lib],
    dependencies: am_actor_handle_cpp_lib_dependencies
)
# === @end: am_actor_handle_cpp_lib ===


# === @begin: am_actor_creator_cpp_lib (@original: actor_creator) ===
am_actor_creator_cpp_lib_dependencies = [
    # Package-managed
    gcs_client_cpp_lib_dep,
    thread_utils_cpp_lib_dep,
]

am_actor_creator_cpp_lib = static_library(
    'am_actor_creator_cpp_lib',
    ['actor_creator.cc'],
    install: false,
    dependencies: am_actor_creator_cpp_lib_dependencies
)

am_actor_creator_cpp_lib_dep = declare_dependency(
    link_with: [am_actor_creator_cpp_lib],
    dependencies: am_actor_creator_cpp_lib_dependencies
)
# === @end: am_actor_creator_cpp_lib ===


# === @begin: am_fake_actor_creator_cpp_lib (@original: fake_actor_creator) ===
# Header-only
# Header-only, just re-exports am_actor_creator_cpp_lib
am_fake_actor_creator_cpp_lib_dependencies = [
    # Package-managed
    am_actor_creator_cpp_lib_dep,
]

am_fake_actor_creator_cpp_lib_dep = declare_dependency(
    sources: ['fake_actor_creator.h'],
    dependencies: am_fake_actor_creator_cpp_lib_dependencies
)
# === @end: am_fake_actor_creator_cpp_lib ===
