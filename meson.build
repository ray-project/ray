project(
    'ray-meson',
    'c', 'cpp',
    default_options: [
        'c_std=gnu17', 'cpp_std=gnu++20'
    ],
    meson_version: '>=1.3.0'
)

# === Begin of imports ===
fs = import('fs')
py = import('python').find_installation(pure: false, modules: ['grpc_tools'])
# === End of imports ===


# === Begin of dependencies ===
# I don't want to bring the entire GTest, portions of which Ray requires even for non-test code.
# Instead, I've vendored a few GTest headers it really uses and add them into search paths, used by all compiler invocations.
add_project_arguments(
    '-I' + meson.project_build_root() / '__eugo' / 'include',
    '-I' + meson.project_source_root() / '__eugo' / 'include',
    language: ['c', 'cpp']
)

# Required to make generated protobuf headers visible to the files relying on them.
# These generated headers are included using not their names but their paths starting from the repo root, so we have to
# use `project_build_root` as they are generated dynamically.
add_project_arguments(
    '-I' + meson.project_build_root(),
    language: ['c', 'cpp']
)

absl = dependency('absl', method: 'cmake', modules: [
    #'absl::raw_logging_internal', # https://github.com/ray-project/ray/issues/8588, https://github.com/ray-project/ray/issues/17212

    'absl::config', # @TODO+:not sure if it's really used

    # `#include "absl/base/*"`
    'absl::core_headers', # https://github.com/ray-project/ray/blob/7591f91832872fc4606dd54641bb2b781de170a0/BUILD.bazel#L565
#    'absl::base_internal',
    'absl::base', # https://github.com/ray-project/ray/blob/7591f91832872fc4606dd54641bb2b781de170a0/BUILD.bazel#L565

    # `#include "absl/types/optional.h"`
    'absl::optional',
#    'absl::bad_optional_access',

    # `#include "absl/random/*"`
    'absl::random_random',
#    'absl::random_bit_gen_ref',
#    'absl::random_seed_gen_exception',
#    'absl::random_seed_sequences',
#    'absl::random_distributions',
#    'absl::random_internal_mock_helpers',
#    'absl::random_internal_traits',
#    'absl::random_internal_distribution_caller',
#    'absl::random_internal_fast_uniform_bits',
#    'absl::random_internal_seed_material',
#    'absl::random_internal_pool_urbg',
#    'absl::random_internal_salted_seed_seq',
#    'absl::random_internal_iostream_state_saver',
#    'absl::random_internal_generate_real',
#    'absl::random_internal_wide_multiply',
#    'absl::random_internal_fastmath',
#    'absl::random_internal_nonsecure_base',
#    'absl::random_internal_pcg_engine',
#    'absl::random_internal_randen_engine',
#    'absl::random_internal_platform',
#    'absl::random_internal_randen',
#    'absl::random_internal_randen_slow',
#    'absl::random_internal_randen_hwaes',
#    'absl::random_internal_randen_hwaes_impl',
#    'absl::random_internal_distribution_test_util',
#    'absl::random_internal_uniform_helper',

    # `#include "absl/container/*"`
    'absl::btree',
    'absl::flat_hash_map',
    'absl::flat_hash_set',
    'absl::node_hash_map',
    'absl::node_hash_set',

    # `#include "absl/cleanup/*"`
    'absl::cleanup',
#    'absl::cleanup_internal',

    # `#include "absl/memory/*"`
    'absl::memory',

    # `#include "absl/flags/*"`
    'absl::flags',
#    'absl::flags_path_util',
#    'absl::flags_program_name',
#    'absl::flags_config',
#    'absl::flags_marshalling',
#    'absl::flags_commandlineflag_internal',
#    'absl::flags_commandlineflag',
#    'absl::flags_private_handle_accessor',
#    'absl::flags_reflection',
#    'absl::flags_internal',
#    'absl::flags_usage_internal',
#    'absl::flags_usage',
#    'absl::flags_parse',

    # `#include "absl/strings/*"`
    'absl::strings',
    'absl::strings_internal',

    # `#include "absl/time/*"`
    'absl::time',
#    'absl::civil_time',
#    'absl::time_zone'

    # `#include "absl/hash/*"`
    'absl::hash',

    # `#include "absl/synchronization/*"`
    'absl::synchronization',

    # `#include "absl/functional/*"`

#    'absl::endian',
#    'absl::scoped_set_env',
#    'absl::prefetch',
#    'absl::algorithm',
#    'absl::algorithm_container',
#    'absl::compressed_tuple',
#    'absl::fixed_array',
#    'absl::inlined_vector_internal',
#    'absl::inlined_vector',
#    'absl::counting_allocator',
#    'absl::container_memory',
#    'absl::hash_function_defaults',
#    'absl::hash_policy_traits',
#    'absl::hashtablez_sampler',
#    'absl::hashtable_debug',
#    'absl::hashtable_debug_hooks',
#    'absl::node_slot_policy',
#    'absl::raw_hash_map',
#    'absl::container_common',
#    'absl::raw_hash_set',
#    'absl::layout',
#    'absl::stacktrace',
#    'absl::symbolize',
#    'absl::examine_stack',
#    'absl::failure_signal_handler',
#    'absl::debugging_internal',
#    'absl::demangle_internal',
#    'absl::leak_check',
#    'absl::debugging',
#    'absl::bind_front',
#    'absl::function_ref',
#    'absl::city',
#    'absl::low_level_hash',
#    'absl::type_traits',
#    'absl::meta',
#    'absl::bits',
#    'absl::int128',
#    'absl::numeric',
#    'absl::numeric_representation',
#    'absl::sample_recorder',
#    'absl::exponential_biased',
#    'absl::periodic_sampler',
#    'absl::status',
#    'absl::statusor',
#    'absl::str_format',
#    'absl::str_format_internal',
#    'absl::graphcycles_internal',
#    'absl::kernel_timeout_internal',
#    'absl::span',
#    'absl::compare',
#    'absl::utility'
])
# Not yet required:
# - 'absl::atomic_hook', 'absl::errno_saver', 'absl::log_severity', 'absl::spinlock_wait', 'absl::dynamic_annotations',
#   'absl::malloc_internal', 'absl::throw_delegate', 'absl::pretty_function', 'absl::strerror', 'absl::fast_type_id',
# - 'absl::cord', 'absl::cord_internal', 'absl::cordz_update_tracker', 'absl::cordz_functions', 'absl::cordz_info',
#   'absl::cordz_statistics', 'absl::cordz_handle', 'absl::cordz_sample_token', 'absl::cordz_update_scope',
# - 'absl::any', 'absl::bad_any_cast', 'absl::bad_any_cast_impl', 'absl::any_invocable',
# - 'absl::variant', 'absl::bad_variant_access',

# @TODO-:Add module-based approach like for `absl`?
boost = dependency('boost', method: 'cmake')

msgpack = dependency('msgpack-cxx', method: 'cmake')

# @TODO+: reimplment asap
#opencensus = dependency('OpenCensus')

protoc_cpp = find_program('protoc')
protoc_cpp_with_default_args = [protoc_cpp, '--proto_path=..', '--cpp_out=@BUILD_ROOT@', '@INPUT@']
protoc_py = ['python3', '-m', 'grpc_tools.protoc']
protoc_py_with_default_args = [protoc_py, '--proto_path=..', '--python_out=@BUILD_ROOT@', '--grpc_python_out=@BUILD_ROOT@', '@INPUT@']

# If not disabling `pkg-config` search, this causes the dependency cycle and infinite execution
protobuf = dependency('protobuf', modules: ['protobuf::libprotobuf'], method: 'cmake')

# Depend on `protobuf`, so the same caveat applies
grpc = dependency('gRPC', method: 'cmake')
# === End of dependencies ===


# === @begin: __eugo/ ===
subdir('__eugo/')
# === @end: __eugo/ ===


# === Begin of rpc definitions ===
# GRPC common lib.
grpc_common_lib_sources = files(
    'src/ray/rpc/common.cc',
    'src/ray/rpc/grpc_server.cc',
    'src/ray/rpc/server_call.cc',
)

grpc_common_lib = static_library(
    'grpc_common_lib',
    grpc_common_lib_sources,
    include_directories: ['src/'],
    dependencies: [
        absl,
        boost,
        grpc,
        msgpack,
        #opencensus,
        protobuf
    ],
    install: false
)

subdir('src/')

# Node manager gRPC lib.
#node_manager_cc_grpc = cc_grpc_library(
#    name = "node_manager_cc_grpc",
#    srcs = ["//src/ray/protobuf:node_manager_proto"],
#    grpc_only = True,
#    deps = ["//src/ray/protobuf:node_manager_cc_proto"],
#)

#
## Node manager server and client.
#ray_cc_library(
#    name = "node_manager_rpc",
#    srcs = glob([
#    "src/ray/rpc/node_manager/*.cc",
#    ]),
#hdrs = glob([
#"src/ray/rpc/node_manager/*.h",
#]),
#deps = [
#":grpc_common_lib",
#":node_manager_cc_grpc",
#":ray_common",
#"@boost//:asio",
#"@com_github_grpc_grpc//:grpc++",
#],
#)
#
## gcs_service gRPC lib.
#cc_grpc_library(
#    name = "gcs_service_cc_grpc",
#    srcs = ["//src/ray/protobuf:gcs_service_proto"],
#    grpc_only = True,
#    deps = ["//src/ray/protobuf:gcs_service_cc_proto"],
#)
#
## gcs_service gRPC lib.
#cc_grpc_library(
#    name = "test_service_cc_grpc",
#    srcs = ["//src/ray/protobuf:test_service_proto"],
#    grpc_only = True,
#    deps = ["//src/ray/protobuf:test_service_cc_proto"],
#)
#
## gcs rpc server and client.
#ray_cc_library(
#    name = "gcs_service_rpc",
#    hdrs = [
#    "src/ray/rpc/gcs_server/gcs_rpc_client.h",
#    "src/ray/rpc/gcs_server/gcs_rpc_server.h",
#    ],
#    deps = [
#    ":autoscaler_cc_grpc",
#    ":gcs_service_cc_grpc",
#    ":grpc_common_lib",
#    ":pubsub_lib",
#    ":ray_common",
#    "@boost//:asio",
#    "@com_github_grpc_grpc//:grpc++",
#    "@com_google_absl//absl/container:btree",
#    ],
#)
#
## Object manager gRPC lib.
#cc_grpc_library(
#    name = "object_manager_cc_grpc",
#    srcs = ["//src/ray/protobuf:object_manager_proto"],
#    grpc_only = True,
#    deps = ["//src/ray/protobuf:object_manager_cc_proto"],
#)
#
## Object manager rpc server and client.
#ray_cc_library(
#    name = "object_manager_rpc",
#    hdrs = glob([
#    "src/ray/rpc/object_manager/*.h",
#    ]),
#deps = [
#":grpc_common_lib",
#":object_manager_cc_grpc",
#":ray_common",
#"@boost//:asio",
#"@com_github_grpc_grpc//:grpc++",
#],
#)
#
## Worker gRPC lib.
#cc_grpc_library(
#    name = "worker_cc_grpc",
#    srcs = ["//src/ray/protobuf:core_worker_proto"],
#    grpc_only = True,
#    deps = ["//src/ray/protobuf:worker_cc_proto"],
#)
#
## worker server and client.
#ray_cc_library(
#    name = "worker_rpc",
#    srcs = glob([
#    "src/ray/rpc/worker/*.cc",
#    ]),
#hdrs = glob([
#"src/ray/rpc/worker/*.h",
#]),
#deps = [
#":grpc_common_lib",
#":pubsub_lib",
#":ray_common",
#":worker_cc_grpc",
#"@boost//:asio",
#"@boost//:thread",
#"@com_github_grpc_grpc//:grpc++",
#],
#)
#
## Metrics Agent gRPC lib.
#cc_grpc_library(
#    name = "reporter_cc_grpc",
#    srcs = ["//src/ray/protobuf:reporter_proto"],
#    grpc_only = True,
#    deps = ["//src/ray/protobuf:reporter_cc_proto"],
#)
#
## Metrics Agent client.
#ray_cc_library(
#    name = "reporter_rpc",
#    hdrs = glob([
#    "src/ray/rpc/metrics_agent_client.h",
#    ]),
#deps = [
#":grpc_common_lib",
#":reporter_cc_grpc",
#"//src/ray/common:status",
#"@boost//:asio",
#"@boost//:thread",
#"@com_github_grpc_grpc//:grpc++",
#],
#)
#
## pubsub.
#cc_grpc_library(
#    name = "pubsub_cc_grpc",
#    srcs = ["//src/ray/protobuf:pubsub_proto"],
#    grpc_only = True,
#    deps = [
#    "//src/ray/protobuf:common_cc_proto",
#    "//src/ray/protobuf:gcs_cc_proto",
#    "//src/ray/protobuf:pubsub_cc_proto",
#    ],
#)
#
#ray_cc_library(
#    name = "pubsub_rpc",
#    deps = [
#    "pubsub_cc_grpc",
#    ":grpc_common_lib",
#    ":ray_common",
#    ],
#)
#
#cc_grpc_library(
#    name = "autoscaler_cc_grpc",
#    srcs = ["//src/ray/protobuf:autoscaler_proto"],
#    grpc_only = True,
#    deps = [
#    "//src/ray/protobuf:autoscaler_cc_proto",
#    ],
#)
#
#ray_cc_library(
#    name = "autoscaler_rpc",
#    deps = [
#    ":autoscaler_cc_grpc",
#    ],
#)

# === End of rpc definitions ===