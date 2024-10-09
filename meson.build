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



#
#ray_cc_library(
#    name = "ray_mock",
#    hdrs = glob(
#    ["src/mock/**/*.h"],
#),
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
