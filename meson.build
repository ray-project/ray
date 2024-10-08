# === @begin: General ===
project(
    'ray-meson',
    'c', 'cpp',
    default_options: [
        'c_std=gnu17',
        'cpp_std=gnu++23'
    ],
    meson_version: '>=1.3.0'
)
# === @end: General ===


# === @begin: Meson modules imports ===
fs = import('fs')
py = import('python').find_installation(pure: false, modules: ['grpc_tools'])
# === @end: Meson modules imports ===


# === @begin: Project-wide settings ===
# I don't want to bring the entire GTest, portions of which Ray requires even for non-test code.
# Instead, I've vendored a few GTest headers it really uses and add them into search paths, used by all compiler invocations.
add_project_arguments(
    '-I' + meson.project_build_root() / 'eugo' / 'include',
    '-I' + meson.project_source_root() / 'eugo' / 'include',
    language: ['c', 'cpp']
)

# Required to make generated protobuf headers visible to the files relying on them.
# These generated headers are included using not their names but their paths starting from the repo root, so we have to
# use `project_build_root` as they are generated dynamically.
add_project_arguments(
    '-I' + meson.project_build_root(),
    '-I' + meson.project_build_root() / 'src',
    '-I' + meson.project_source_root(),
    '-I' + meson.project_source_root() / 'src',
    language: ['c', 'cpp']
)
# === @end: Project-wide settings ===


# === @begin: eugo/ ===
subdir('eugo/')
# === @end: eugo/ ===


# === @begin: src/ ===
subdir('src/')
# === @end: src/ ===


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

##
##ray_cc_library(
##    name = "autoscaler_rpc",
##    deps = [
##    ":autoscaler_cc_grpc",
##    ],
##)
##
### === End of rpc definitions ===
##
### === Begin of plasma definitions ===
##
### TODO(mehrdadn): (How to) support dynamic linking?
##PROPAGATED_WINDOWS_DEFINES = ["ARROW_STATIC"]
##
##PLASMA_COPTS = COPTS + select({
##                                  "@platforms//os:windows": [
##                                  ] + ["-D" + define for define in PROPAGATED_WINDOWS_DEFINES],
##                                  "//conditions:default": [
##                                  "-DARROW_USE_GLOG",
##                                  ],
##                              })
##
##PLASMA_LINKOPTS = [] + select({
##                                  "@platforms//os:windows": [
##                                  "-DefaultLib:" + "ws2_32.lib",
##                                  ],
##                                  "//conditions:default": [
##                                  ],
##                              })
##
##ray_cc_library(
##    name = "plasma_client",
##    srcs = [
##    "src/ray/object_manager/common.cc",
##    "src/ray/object_manager/plasma/client.cc",
##    "src/ray/object_manager/plasma/connection.cc",
##    "src/ray/object_manager/plasma/malloc.cc",
##    "src/ray/object_manager/plasma/plasma.cc",
##    "src/ray/object_manager/plasma/protocol.cc",
##    "src/ray/object_manager/plasma/shared_memory.cc",
##    ] + select({
##    "@platforms//os:windows": [
##    ],
##    "//conditions:default": [
##    "src/ray/object_manager/plasma/fling.cc",
##    ],
##    }),
##hdrs = [
##"src/ray/object_manager/common.h",
##"src/ray/object_manager/plasma/client.h",
##"src/ray/object_manager/plasma/common.h",
#"src/ray/object_manager/plasma/compat.h",
#"src/ray/object_manager/plasma/connection.h",
#"src/ray/object_manager/plasma/malloc.h",
#"src/ray/object_manager/plasma/plasma.h",
#"src/ray/object_manager/plasma/plasma_generated.h",
#"src/ray/object_manager/plasma/protocol.h",
#"src/ray/object_manager/plasma/shared_memory.h",
#] + select({
#"@platforms//os:windows": [
#],
#"//conditions:default": [
#"src/ray/object_manager/plasma/fling.h",
#],
#}),
#copts = PLASMA_COPTS,
#defines = select({
#"@platforms//os:windows": PROPAGATED_WINDOWS_DEFINES,
#"//conditions:default": [],
#}),
#linkopts = PLASMA_LINKOPTS,
#deps = [
#":plasma_fbs",
#":ray_common",
#"//src/ray/protobuf:common_cc_proto",
#"//src/ray/util",
#"@msgpack",
#],
#)
#
#ray_cc_library(
#    name = "plasma_store_server_lib",
#    srcs = [
#    "src/ray/object_manager/plasma/create_request_queue.cc",
#    "src/ray/object_manager/plasma/dlmalloc.cc",
#    "src/ray/object_manager/plasma/eviction_policy.cc",
#    "src/ray/object_manager/plasma/get_request_queue.cc",
#    "src/ray/object_manager/plasma/object_lifecycle_manager.cc",
#    "src/ray/object_manager/plasma/object_store.cc",
#    "src/ray/object_manager/plasma/plasma_allocator.cc",
#    "src/ray/object_manager/plasma/stats_collector.cc",
#    "src/ray/object_manager/plasma/store.cc",
#    "src/ray/object_manager/plasma/store_runner.cc",
#    ],
#    hdrs = [
#    "src/ray/object_manager/common.h",
#    "src/ray/object_manager/plasma/allocator.h",
#    "src/ray/object_manager/plasma/create_request_queue.h",
#    "src/ray/object_manager/plasma/eviction_policy.h",
#    "src/ray/object_manager/plasma/get_request_queue.h",
#    "src/ray/object_manager/plasma/object_lifecycle_manager.h",
#    "src/ray/object_manager/plasma/object_store.h",
#    "src/ray/object_manager/plasma/plasma_allocator.h",
#    "src/ray/object_manager/plasma/stats_collector.h",
#    "src/ray/object_manager/plasma/store.h",
#    "src/ray/object_manager/plasma/store_runner.h",
#    "src/ray/thirdparty/dlmalloc.c",
#    ],
#    copts = PLASMA_COPTS,
#    linkopts = PLASMA_LINKOPTS,
#    deps = [
#    ":plasma_client",
#    ":stats_lib",
#    "//src/ray/common:network",
#    ],
#)

#flatbuffer_cc_library(
#    name = "plasma_fbs",
#    srcs = ["src/ray/object_manager/plasma/plasma.fbs"],
#    flatc_args = FLATC_ARGS,
#    out_prefix = "src/ray/object_manager/plasma/",
#)
#
## === End of plasma definitions ===
#
#ray_cc_library(
#    name = "ray_mock",
#    hdrs = glob(
#    ["src/mock/**/*.h"],
#),
#)
#


#
#ray_cc_binary(
#    name = "raylet",
#    srcs = ["src/ray/raylet/main.cc"],
#    visibility = ["//java:__subpackages__"],
#    deps = [
#    ":raylet_lib",
#    "//src/ray/util",
#    "@com_github_gflags_gflags//:gflags",
#    ],
#)
#
#ray_cc_library(
#    name = "gcs_pub_sub_lib",
#    srcs = [
#    "src/ray/gcs/pubsub/gcs_pub_sub.cc",
#    ],
#    hdrs = [
#    "src/ray/gcs/pubsub/gcs_pub_sub.h",
#    ],
#    deps = [
#    ":gcs",
#    ":gcs_service_rpc",
#    ":pubsub_lib",
#    ":ray_common",
#    ":redis_client",
#    ],
#)
#
#ray_cc_library(
#    name = "gcs_server_lib",
#    srcs = glob(
#    [
#    "src/ray/gcs/gcs_server/*.cc",
#    ],
#    exclude = [
#    "src/ray/gcs/gcs_server/gcs_server_main.cc",
#    "src/ray/gcs/gcs_server/test/*.cc",
#    ],
#),
#hdrs = glob(
#[
#"src/ray/gcs/gcs_server/*.h",
#],
#),
#deps = [
#":autoscaler_rpc",
#":gcs",
#":gcs_pub_sub_lib",
#":gcs_service_cc_grpc",
#":gcs_service_rpc",
#":gcs_table_storage_lib",
#":node_manager_rpc",
#":observable_store_client",
#":pubsub_lib",
#":raylet_client_lib",
#":scheduler",
#":worker_rpc",
#"//src/ray/protobuf:agent_manager_cc_proto",
#"@boost//:bimap",
#"@com_github_grpc_grpc//src/proto/grpc/health/v1:health_proto",
#"@com_google_absl//absl/container:btree",
#],
#)
#
#ray_cc_binary(
#    name = "gcs_server",
#    srcs = [
#    "src/ray/gcs/gcs_server/gcs_server_main.cc",
#    ],
#    visibility = ["//java:__subpackages__"],
#    deps = [
#    ":gcs_server_lib",
#    ":stats_lib",
#    "@com_github_gflags_gflags//:gflags",
#    ],
#)
#
## Ray native pubsub module.
#ray_cc_library(
#    name = "pubsub_lib",
#    srcs = glob([
#    "src/ray/pubsub/*.cc",
#    ]),
#hdrs = glob([
#"src/ray/pubsub/*.h",
#]),
#deps = [
#":pubsub_rpc",
#"@boost//:any",
#"@com_google_absl//absl/container:flat_hash_map",
#"@com_google_absl//absl/container:flat_hash_set",
#"@com_google_absl//absl/synchronization",
#],
#)
#

#
#ray_cc_library(
#    name = "stats_lib",
#    srcs = [
#    "src/ray/stats/metric_exporter.cc",
#    ],
#    hdrs = [
#    "src/ray/stats/metric.h",
#    "src/ray/stats/metric_exporter.h",
#    "src/ray/stats/stats.h",
#    "src/ray/stats/tag_defs.h",
#    ],
#    linkopts = select({
#    "@platforms//os:windows": [
#    ],
#    "//conditions:default": [
#    "-lpthread",
#    ],
#    }),
#deps = [
#":reporter_rpc",
#":stats_metric",
#"@com_github_grpc_grpc//:grpc_opencensus_plugin",
#],
#)
#
#ray_cc_library(
#    name = "scheduler",
#    srcs = glob(
#    [
#    "src/ray/raylet/scheduling/**/*.cc",
#    ],
#    exclude = [
#    "src/ray/raylet/scheduling/**/*_test.cc",
#    ],
#),
#hdrs = glob(
#[
#"src/ray/raylet/scheduling/**/*.h",
#"src/ray/core_worker/common.h",
#],
#),
#linkopts = select({
#"@platforms//os:windows": [
#],
#"//conditions:default": [
#"-lpthread",
#],
#}),
#deps = [
#":gcs_client_lib",
#":ray_common",
#":stats_lib",
#"//src/ray/protobuf:common_cc_proto",
#"//src/ray/util",
#"@boost//:asio",
#"@boost//:system",
#"@com_github_jupp0r_prometheus_cpp//pull",
#"@com_google_absl//absl/base:core_headers",
#"@com_google_absl//absl/container:flat_hash_set",
#"@com_google_absl//absl/memory",
#"@com_google_absl//absl/random",
#"@com_google_absl//absl/random:bit_gen_ref",
#"@com_google_absl//absl/strings",
#"@com_google_googletest//:gtest",
#"@io_opencensus_cpp//opencensus/exporters/stats/prometheus:prometheus_exporter",
#"@io_opencensus_cpp//opencensus/stats",
#"@io_opencensus_cpp//opencensus/tags",
#],
#)
#
#ray_cc_library(
#    name = "raylet_lib",
#    srcs = glob(
#    [
#    "src/ray/raylet/**/*.cc",
#    ],
#    exclude = [
#    "src/ray/raylet/**/*_test.cc",
#    "src/ray/raylet/scheduling/**/*.cc",
#    "src/ray/raylet/main.cc",
#    ],
#),
#hdrs = glob(
#[
#"src/ray/raylet/**/*.h",
#],
#exclude = [
#"src/ray/raylet/scheduling/**/*.h",
#"src/ray/raylet/main.cc",
#],
#),
#linkopts = select({
#"@platforms//os:windows": [
#],
#"//conditions:default": [
#"-lpthread",
#],
#}),
#deps = [
#":gcs",
#":gcs_client_lib",
#":node_manager_fbs",
#":node_manager_rpc",
#":object_manager",
#":plasma_client",
#":pubsub_lib",
#":ray_common",
#":scheduler",
#":stats_lib",
#":worker_rpc",
#"//src/ray/protobuf:agent_manager_cc_proto",
#"//src/ray/protobuf:common_cc_proto",
#"//src/ray/protobuf:runtime_env_agent_cc_proto",
#"//src/ray/util",
#"@boost//:asio",
#"@boost//:beast",
#"@boost//:system",
#"@com_github_jupp0r_prometheus_cpp//pull",
#"@com_google_absl//absl/base:core_headers",
#"@com_google_absl//absl/container:flat_hash_set",
#"@com_google_absl//absl/memory",
#"@com_google_absl//absl/strings",
#"@com_google_googletest//:gtest",
#"@io_opencensus_cpp//opencensus/exporters/stats/prometheus:prometheus_exporter",
#"@io_opencensus_cpp//opencensus/stats",
#"@io_opencensus_cpp//opencensus/tags",
#],
#)
#
#ray_cc_library(
#    name = "raylet_client_lib",
#    srcs = glob([
#    "src/ray/raylet_client/*.cc",
#    ]),
#hdrs = glob([
#"src/ray/raylet_client/*.h",
#]),
#linkopts = select({
#"@platforms//os:windows": [
#],
#"//conditions:default": [
#"-lpthread",
#],
#}),
#deps = [
#":node_manager_fbs",
#":node_manager_rpc",
#":ray_common",
#"//src/ray/protobuf:gcs_cc_proto",
#"//src/ray/util",
#"@boost//:asio",
#],
#)
#
#ray_cc_library(
#    name = "core_worker_lib",
#    srcs = glob(
#    [
#    "src/ray/core_worker/*.cc",
#    "src/ray/core_worker/store_provider/*.cc",
#    "src/ray/core_worker/store_provider/memory_store/*.cc",
#    "src/ray/core_worker/transport/*.cc",
#    ],
#    exclude = [
#    "src/ray/core_worker/**/*_test.cc",
#    ],
#),
#hdrs = glob([
#"src/ray/core_worker/*.h",
#"src/ray/core_worker/store_provider/*.h",
#"src/ray/core_worker/store_provider/memory_store/*.h",
#"src/ray/core_worker/transport/*.h",
#]),
#deps = [
#":gcs",
#":gcs_client_lib",
#":plasma_client",
#":ray_common",
#":raylet_client_lib",
#":stats_lib",
#":worker_rpc",
#"//src/ray/protobuf:worker_cc_proto",
#"//src/ray/util",
#"@boost//:circular_buffer",
#"@boost//:fiber",
#"@com_google_absl//absl/cleanup",
#"@com_google_absl//absl/container:btree",
#"@com_google_absl//absl/container:flat_hash_map",
#"@com_google_absl//absl/container:flat_hash_set",
#"@com_google_absl//absl/container:node_hash_map",
#"@nlohmann_json",
#],
#)
#
## This header is used to wrap some internal codes so we can reduce suspicious
## symbols export.
#ray_cc_library(
#    name = "exported_internal",
#    srcs =
#    [
#    "src/ray/internal/internal.cc",
#    ],
#    hdrs =
#    [
#    "src/ray/internal/internal.h",
#    ],
#    copts = COPTS,
#    strip_include_prefix = "src",
#    deps = [
#    ":core_worker_lib",
#    ],
#    alwayslink = 1,
#)

#ray_cc_library(
#    name = "gcs_table_storage_lib",
#    srcs = glob(
#    [
#    "src/ray/gcs/gcs_server/gcs_table_storage.cc",
#    ],
#),
#hdrs = glob(
#[
#"src/ray/gcs/gcs_server/gcs_table_storage.h",
#],
#),
#deps = [
#":gcs",
#":gcs_in_memory_store_client",
#":observable_store_client",
#":pubsub_lib",
#":ray_common",
#":redis_store_client",
#],
#)
#

#ray_cc_library(
#    name = "gcs_client_lib",
#    srcs = [
#    "src/ray/gcs/gcs_client/accessor.cc",
#    "src/ray/gcs/gcs_client/gcs_client.cc",
#    ],
#    hdrs = [
#    "src/ray/gcs/gcs_client/accessor.h",
#    "src/ray/gcs/gcs_client/gcs_client.h",
#    ],
#    deps = [
#    ":gcs",
#    ":gcs_pub_sub_lib",
#    ":gcs_service_rpc",
#    ":pubsub_lib",
#    ":ray_common",
#    ":redis_store_client",
#    "//src/ray/protobuf:usage_cc_proto",
#    ],
#)
#
#ray_cc_library(
#    name = "global_state_accessor_lib",
#    srcs = glob(
#    [
#    "src/ray/gcs/gcs_client/global_state_accessor.cc",
#    ],
#),
#hdrs = glob(
#[
#"src/ray/gcs/gcs_client/global_state_accessor.h",
#],
#),
#deps = [
#":gcs_client_lib",
#],
#)
#
#ray_cc_library(
#    name = "object_manager",
#    srcs = glob([
#    "src/ray/object_manager/*.cc",
#    "src/ray/object_manager/notification/*.cc",
#    ]),
#hdrs = glob([
#"src/ray/object_manager/*.h",
#"src/ray/object_manager/notification/*.h",
#]),
#deps = [
#":core_worker_lib",
#":gcs",
#":object_manager_rpc",
#":plasma_store_server_lib",
#":ray_common",
#"//src/ray/util",
#"@boost//:asio",
#],
#)
#
#ray_cc_library(
#    name = "platform_shims",
#    srcs = [] + select({
#    "@platforms//os:windows": glob([
#    "src/shims/windows/**/*.c",
#    "src/shims/windows/**/*.cc",
#    "src/shims/windows/**/*.h",
#    ]),
#"//conditions:default": [],
#}),
#hdrs = [] + select({
#"@platforms//os:windows": glob([
#"src/shims/windows/**/*.h",
#]),
#"//conditions:default": [],
#}),
#strip_include_prefix = select({
#"@platforms//os:windows": "src/shims/windows",
#"//conditions:default": "",
#}),
#)
#
#filegroup(
#    name = "extra_actions_base_proto",
#    srcs = [
#    "thirdparty/protobuf/extra_actions_base.proto",
#    ],
#)
#
#proto_library(
#    name = "extra_actions_base_proto_lib",
#    srcs = ["thirdparty/protobuf/extra_actions_base.proto"],
#)
#
#cc_proto_library(
#    name = "extra_actions_cc_proto_lib",
#    deps = [":extra_actions_base_proto_lib"],
#)
#

#alias(
#    name = "hiredis",
#    actual = "@com_github_redis_hiredis//:hiredis",
#)
#
#ray_cc_library(
#    name = "redis_client",
#    srcs = [
#    "src/ray/gcs/asio.cc",
#    "src/ray/gcs/redis_async_context.cc",
#    "src/ray/gcs/redis_client.cc",
#    "src/ray/gcs/redis_context.cc",
#    ],
#    hdrs = [
#    "src/ray/gcs/asio.h",
#    "src/ray/gcs/redis_async_context.h",
#    "src/ray/gcs/redis_client.h",
#    "src/ray/gcs/redis_context.h",
#    ],
#    deps = [
#    ":hiredis",
#    ":ray_common",
#    ":stats_lib",
#    "//src/ray/util",
#    "@boost//:asio",
#    ],
#)
#
#ray_cc_library(
#    name = "redis_store_client",
#    srcs = [
#    "src/ray/gcs/store_client/redis_store_client.cc",
#    ],
#    hdrs = [
#    "src/ray/gcs/callback.h",
#    "src/ray/gcs/store_client/redis_store_client.h",
#    "src/ray/gcs/store_client/store_client.h",
#    ],
#    deps = [
#    "redis_client",
#    "@com_google_absl//absl/strings:str_format",
#    "@com_google_absl//absl/synchronization",
#    ],
#)
#
#ray_cc_library(
#    name = "gcs_in_memory_store_client",
#    srcs = [
#    "src/ray/gcs/store_client/in_memory_store_client.cc",
#    ],
#    hdrs = [
#    "src/ray/gcs/callback.h",
#    "src/ray/gcs/store_client/in_memory_store_client.h",
#    "src/ray/gcs/store_client/store_client.h",
#    ],
#    deps = [
#    ":ray_common",
#    "//src/ray/util",
#    ],
#)
#
#ray_cc_library(
#    name = "observable_store_client",
#    srcs = [
#    "src/ray/gcs/store_client/observable_store_client.cc",
#    ],
#    hdrs = [
#    "src/ray/gcs/callback.h",
#    "src/ray/gcs/store_client/observable_store_client.h",
#    "src/ray/gcs/store_client/store_client.h",
#    ],
#    deps = [
#    ":ray_common",
#    "//src/ray/util",
#    ],
#)

#ray_cc_library(
#    name = "gcs",
#    srcs = glob(
#    [
#    "src/ray/gcs/*.cc",
#    ],
#    exclude = [
#    "src/ray/gcs/*_test.cc",
#    ],
#),
#hdrs = glob([
#"src/ray/gcs/*.h",
#]),
#deps = [
#":hiredis",
#":node_manager_fbs",
#":node_manager_rpc",
#":ray_common",
#":stats_lib",
#"//src/ray/protobuf:agent_manager_cc_proto",
#"//src/ray/protobuf:gcs_cc_proto",
#"//src/ray/protobuf:gcs_service_cc_proto",
#"//src/ray/util",
#"@boost//:asio",
#],
#)
#



#
#ray_cc_library(
#    name = "python_callbacks",
#    hdrs = [
#    "src/ray/gcs/gcs_client/python_callbacks.h",
#    ],
#)
#
#pyx_library(
#    name = "_raylet",
#    srcs = glob([
#    "python/ray/__init__.py",
#    "python/ray/_raylet.pxd",
#    "python/ray/_raylet.pyx",
#    "python/ray/includes/*.pxd",
#    "python/ray/includes/*.pxi",
#    ]),
## Export ray ABI symbols, which can then be used by _streaming.so.
## We need to dlopen this lib with RTLD_GLOBAL to use ABI in this
## shared lib, see python/ray/__init__.py.
#cc_kwargs = dict(
#srcs = PYX_SRCS,
#copts = COPTS + PYX_COPTS,
## see https://github.com/tensorflow/tensorflow/blob/r2.1/tensorflow/lite/BUILD#L444
#linkopts = select({
#"@platforms//os:osx": [
#"-Wl,-exported_symbols_list,$(location //:src/ray/ray_exported_symbols.lds)",
#],
#"@platforms//os:windows": [],
#"//conditions:default": [
#"-Wl,--version-script,$(location //:src/ray/ray_version_script.lds)",
#],
#}),
#linkstatic = 1,
#),
#deps = [
#":python_callbacks",
#"//:core_worker_lib",
#"//:exported_internal",
#"//:gcs_server_lib",
#"//:global_state_accessor_lib",
#"//:raylet_lib",
#"//:redis_client",
#"//:src/ray/ray_exported_symbols.lds",
#"//:src/ray/ray_version_script.lds",
#"//:stats_lib",
#"//src/ray/protobuf:serialization_cc_proto",
#"//src/ray/util",
#],
#)
#
#ray_cc_binary(
#    name = "libcore_worker_library_java.so",
#    srcs = glob([
#    "src/ray/core_worker/lib/java/*.h",
#    "src/ray/core_worker/lib/java/*.cc",
#    ]),
## Export ray ABI symbols, which can then be used by libstreaming_java.so. see `//:_raylet`
#linkopts = select({
#"@platforms//os:osx": [
#"-Wl,-exported_symbols_list,$(location //:src/ray/ray_exported_symbols.lds)",
#],
#"@platforms//os:windows": [],
#"//conditions:default": [
#"-Wl,--version-script,$(location //:src/ray/ray_version_script.lds)",
#],
#}),
#linkshared = 1,
#linkstatic = 1,
#visibility = ["//java:__subpackages__"],
#deps = [
#"//:core_worker_lib",
#"//:exported_internal",
#"//:global_state_accessor_lib",
#"//:src/ray/ray_exported_symbols.lds",
#"//:src/ray/ray_version_script.lds",
#"//:stats_lib",
#"@bazel_tools//tools/jdk:jni",
#],
#)
#
#filegroup(
#    name = "python_sources",
#    srcs = glob([
#    "python/ray/*.py",
#    "python/ray/autoscaler/*.py",
#    "python/ray/autoscaler/_private/*.py",
#    "python/ray/autoscaler/_private/_azure/*.json",
#    "python/ray/autoscaler/aws/defaults.yaml",
#    "python/ray/autoscaler/azure/defaults.yaml",
#    "python/ray/autoscaler/gcp/defaults.yaml",
#    "python/ray/autoscaler/local/defaults.yaml",
#    "python/ray/autoscaler/vsphere/defaults.yaml",
#    "python/ray/cloudpickle/*.py",
#    "python/ray/core/__init__.py",
#    "python/ray/core/generated/__init__.py",
#    "python/ray/core/generated/ray/__init__.py",
#    "python/ray/core/generated/ray/protocol/__init__.py",
#    "python/ray/dashboard/**/*.py",
#    "python/ray/experimental/*.py",
#    "python/ray/util/*.py",
#    "python/ray/internal/*.py",
#    "python/ray/workers/default_worker.py",
#    ]),
#)

#filegroup(
#    name = "all_py_proto",
#    srcs = [
#    "//src/ray/protobuf:agent_manager_py_proto",
#    "//src/ray/protobuf:autoscaler_py_proto",
#    "//src/ray/protobuf:common_py_proto",
#    "//src/ray/protobuf:core_worker_py_proto",
#    "//src/ray/protobuf:event_py_proto",
#    "//src/ray/protobuf:gcs_py_proto",
#    "//src/ray/protobuf:gcs_service_py_proto",
#    "//src/ray/protobuf:instance_manager_py_proto",
#    "//src/ray/protobuf:job_agent_py_proto",
#    "//src/ray/protobuf:node_manager_py_proto",
#    "//src/ray/protobuf:ray_client_py_proto",
#    "//src/ray/protobuf:reporter_py_proto",
#    "//src/ray/protobuf:runtime_env_agent_py_proto",
#    "//src/ray/protobuf:runtime_env_common_py_proto",
#    "//src/ray/protobuf:usage_py_proto",
#    "//src/ray/protobuf:export_event_py_proto",
#    ],
#)
#
#filegroup(
#    name = "serve_py_proto",
#    srcs = [
#    "//src/ray/protobuf:serve_py_proto",
#    ],
#)
#
## This is a dummy test dependency that causes the python tests to be
## re-run if any of these files changes.
#py_library(
#    name = "ray_lib",
#    srcs = glob(
#    ["python/ray/**/*.py"],
#    exclude = ["python/ray/tests/*.py"],
#),
#visibility = ["__subpackages__"],
#)
#
#copy_to_workspace(
#    name = "cp_raylet_so",
#    srcs = ["python/ray/_raylet.so"],
#    dstdir = "python/ray",
#)
#
#copy_to_workspace(
#    name = "cp_all_py_proto",
#    srcs = [":all_py_proto"],
#    dstdir = "python/ray/core/generated",
#)
#
#copy_to_workspace(
#    name = "cp_serve_py_proto",
#    srcs = [":serve_py_proto"],
#    dstdir = "python/ray/serve/generated",
#)
#
#copy_to_workspace(
#    name = "cp_raylet",
#    srcs = [":raylet"],
#    dstdir = "python/ray/core/src/ray/raylet",
#)
#
#copy_to_workspace(
#    name = "cp_gcs_server",
#    srcs = [":gcs_server"],
#    dstdir = "python/ray/core/src/ray/gcs",
#)
#
#copy_to_workspace(
#    name = "cp_jemalloc",
#    srcs = ["@jemalloc//:shared"],
#    dstdir = "python/ray/core/",
#)
#
#genrule(
#    name = "install_py_proto",
#    srcs = [
#    ":cp_all_py_proto",
#    ":cp_serve_py_proto",
#    ],
#    outs = ["install_py_proto.out"],
#    cmd = """
#    # NOTE(hchen): Protobuf doesn't allow specifying Python package name. So we use this `sed`
#    # command to change the import path in the generated file.
#    # shellcheck disable=SC2006
#    files=(
#    `ls python/ray/core/generated/*_pb2*.py` \
#            `ls python/ray/serve/generated/*_pb2*.py` \
#            )
#sed -i -E 's/from src.ray.protobuf/from ./' "$${files[@]}"
## Help the generated serve files to have the correct module
#serve_files=(`ls python/ray/serve/generated/*_pb2*.py`)
#sed -i -E 's/'"'"'src.ray.protobuf./'"'"'ray.serve.generated./' "$${serve_files[@]}"
## TODO(sang): Build our own proto instead of creating a new proto for opencensus separately.
## https://github.com/ray-project/ray/issues/31358
#sed -i -E 's/from opencensus.proto.metrics.v1 import/from . import/' "$${files[@]}"
#sed -i -E 's/from opencensus.proto.resource.v1 import/from . import/' "$${files[@]}"
## TODO(rickyx): Remove this experimental path rewrite once we move autoscaler proto out
## of experimental.
#autoscale_files=(`ls python/ray/core/generated/instance_manager_pb2*.py`)
#sed -i -E 's/from ..experimental/from ./' "$${autoscale_files[@]}"
## Help the generated export api files to have the correct module
#export_api_files=(`ls python/ray/core/generated/export*_pb2*.py`)
#sed -i -E 's/from ..export_api/from ./' "$${export_api_files[@]}"
#echo "$${PWD}" > $@
#""",
#local = 1,
#)
#
#genrule(
#    name = "ray_pkg",
#    srcs = [
#    ":cp_raylet_so",
#    ":python_sources",
#    ":install_py_proto",
#    ":cp_redis",
#    ":cp_raylet",
#    ":cp_gcs_server",
#    ] + select({
#    ":jemalloc": [
#    ":cp_jemalloc",
#    ],
#    "//conditions:default": [],
#    }),
#outs = ["ray_pkg.out"],
#cmd = """
#if [ "$${OSTYPE-}" = "msys" ]; then
#ln -P -f -- python/ray/_raylet.so python/ray/_raylet.pyd
#fi
#echo "$${PWD}" > $@
#""",
#local = 1,
#)
