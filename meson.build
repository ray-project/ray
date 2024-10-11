# === @begin: General ===
project(
    'ray-meson',
    'c', 'cpp', 'cython',
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


# === @begin: python/ ===
subdir('python/')
# === @end: python/ ===





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
