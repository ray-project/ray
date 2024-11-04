# === @begin: General ===
project(
    'ray-meson',
    'c', 'cpp', 'cython',
    version: '3.0.0.dev0',
    default_options: [
        'c_std=gnu17',
        'cpp_std=gnu++23',
        'buildtype=release',
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


# === @begin: ray_commit_sha ===
ray_commit_sha = run_command(git, 'rev-parse', 'HEAD', check: true).stdout().strip()
# === @end: ray_commit_sha ===


# === @begin: src/ ===
subdir('src/')
# === @end: src/ ===


# === @begin: python/ ===
subdir('python/')
# === @end: python/ ===
